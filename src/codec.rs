//! MQTT 3.1.1 packets: zero-copy parsing (packets borrow the receive buffer) and encoding into a
//! caller-owned `Vec<u8>`, for both the broker and the load generator.

use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Malformed packet: the connection must be closed.
    Malformed(&'static str),
    /// Packet larger than the configured maximum.
    TooLarge,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Malformed(why) => write!(f, "malformed packet: {why}"),
            Error::TooLarge => f.write_str("packet too large"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, PartialEq, Eq)]
pub enum Packet<'a> {
    Connect(Connect<'a>),
    ConnAck { session_present: bool, code: u8 },
    Publish { topic: &'a str, payload: &'a [u8], qos: u8, retain: bool, dup: bool, packet_id: Option<u16> },
    PubAck { packet_id: u16 },
    PubRec { packet_id: u16 },
    PubRel { packet_id: u16 },
    PubComp { packet_id: u16 },
    Subscribe { packet_id: u16, filters: Vec<(&'a str, u8)> },
    SubAck { packet_id: u16, codes: Vec<u8> },
    Unsubscribe { packet_id: u16, filters: Vec<&'a str> },
    UnsubAck { packet_id: u16 },
    PingReq,
    PingResp,
    Disconnect,
}

/// A CONNECT packet. `level` is 4 for MQTT 3.1.1 and 3 for MQTT 3.1 (`MQIsdp`); the broker
/// refuses other levels with a CONNACK.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Connect<'a> {
    pub level: u8,
    pub client_id: &'a str,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<Will<'a>>,
    pub username: Option<&'a str>,
    pub password: Option<&'a [u8]>,
}

/// The message the broker publishes when a client goes away without DISCONNECT.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Will<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub qos: u8,
    pub retain: bool,
}

/// Default limit on the size of a packet (its remaining length), 1 MiB.
pub const MAX_PACKET: usize = 1 << 20;

/// Parses one packet at the start of `buf`, of at most `MAX_PACKET` bytes. `Ok(None)`:
/// incomplete, wait for more bytes. `Ok(Some((packet, len)))`: `len` bytes consumed.
pub fn parse(buf: &[u8]) -> Result<Option<(Packet<'_>, usize)>, Error> {
    parse_limited(buf, MAX_PACKET)
}

/// Total size of the packet starting `buf`, once its fixed header is complete.
pub fn packet_len(buf: &[u8]) -> Option<usize> {
    let (rem, var_len) = remaining_length(buf.get(1..)?).ok()??;
    Some(1 + var_len + rem)
}

/// `parse` with a limit of `max` bytes on the remaining length.
pub fn parse_limited(buf: &[u8], max: usize) -> Result<Option<(Packet<'_>, usize)>, Error> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let (rem, var_len) = match remaining_length(&buf[1..])? {
        Some(v) => v,
        None => return Ok(None),
    };
    if rem > max {
        return Err(Error::TooLarge);
    }
    let total = 1 + var_len + rem;
    if buf.len() < total {
        return Ok(None);
    }
    let flags = buf[0] & 0x0f;
    let body = &buf[1 + var_len..total];
    let packet = match buf[0] >> 4 {
        1 => parse_connect(body)?,
        2 => {
            check(body.len() == 2, "connack length")?;
            Packet::ConnAck { session_present: body[0] & 1 == 1, code: body[1] }
        }
        3 => parse_publish(flags, body)?,
        4 => Packet::PubAck { packet_id: u16_at(body, 0)? },
        5 => Packet::PubRec { packet_id: u16_at(body, 0)? },
        6 => {
            check(flags == 2, "pubrel flags")?;
            Packet::PubRel { packet_id: u16_at(body, 0)? }
        }
        7 => Packet::PubComp { packet_id: u16_at(body, 0)? },
        8 => parse_subscribe(flags, body)?,
        9 => {
            let packet_id = u16_at(body, 0)?;
            Packet::SubAck { packet_id, codes: body[2..].to_vec() }
        }
        10 => parse_unsubscribe(flags, body)?,
        11 => Packet::UnsubAck { packet_id: u16_at(body, 0)? },
        12 => Packet::PingReq,
        13 => Packet::PingResp,
        14 => Packet::Disconnect,
        _ => return Err(Error::Malformed("unsupported packet type")),
    };
    Ok(Some((packet, total)))
}

fn check(ok: bool, why: &'static str) -> Result<(), Error> {
    if ok {
        Ok(())
    } else {
        Err(Error::Malformed(why))
    }
}

fn remaining_length(buf: &[u8]) -> Result<Option<(usize, usize)>, Error> {
    let mut value = 0usize;
    for (i, &b) in buf.iter().take(4).enumerate() {
        value |= ((b & 0x7f) as usize) << (7 * i);
        if b & 0x80 == 0 {
            return Ok(Some((value, i + 1)));
        }
    }
    if buf.len() >= 4 {
        Err(Error::Malformed("remaining length"))
    } else {
        Ok(None)
    }
}

fn u16_at(buf: &[u8], at: usize) -> Result<u16, Error> {
    match buf.get(at..at + 2) {
        Some(b) => Ok(u16::from_be_bytes([b[0], b[1]])),
        None => Err(Error::Malformed("truncated")),
    }
}

/// A length-prefixed UTF-8 string at `at`; returns it and the offset after it.
fn str_at(buf: &[u8], at: usize) -> Result<(&str, usize), Error> {
    let len = u16_at(buf, at)? as usize;
    let bytes = buf.get(at + 2..at + 2 + len).ok_or(Error::Malformed("truncated string"))?;
    let s = std::str::from_utf8(bytes).map_err(|_| Error::Malformed("invalid UTF-8"))?;
    Ok((s, at + 2 + len))
}

/// Length-prefixed binary data at `at`; returns it and the offset after it.
fn bytes_at(buf: &[u8], at: usize) -> Result<(&[u8], usize), Error> {
    let len = u16_at(buf, at)? as usize;
    let bytes = buf.get(at + 2..at + 2 + len).ok_or(Error::Malformed("truncated data"))?;
    Ok((bytes, at + 2 + len))
}

fn parse_connect(body: &[u8]) -> Result<Packet<'_>, Error> {
    let (name, at) = str_at(body, 0)?;
    check(name == "MQTT" || name == "MQIsdp", "protocol name")?;
    let level = *body.get(at).ok_or(Error::Malformed("truncated connect"))?;
    let flags = *body.get(at + 1).ok_or(Error::Malformed("truncated connect"))?;
    check(flags & 0x01 == 0, "connect reserved flag")?;
    let keep_alive = u16_at(body, at + 2)?;
    let (client_id, mut at) = str_at(body, at + 4)?;
    let will = if flags & 0x04 != 0 {
        let (topic, next) = str_at(body, at)?;
        let (payload, next) = bytes_at(body, next)?;
        at = next;
        let qos = (flags >> 3) & 3;
        check(qos < 3, "will qos")?;
        check(!topic.is_empty() && !topic.contains(['+', '#']), "will topic")?;
        Some(Will { topic, payload, qos, retain: flags & 0x20 != 0 })
    } else {
        check(flags & 0x38 == 0, "will flags without will")?;
        None
    };
    let username = if flags & 0x80 != 0 {
        let (u, next) = str_at(body, at)?;
        at = next;
        Some(u)
    } else {
        None
    };
    let password = if flags & 0x40 != 0 {
        let (p, _) = bytes_at(body, at)?;
        Some(p)
    } else {
        None
    };
    Ok(Packet::Connect(Connect {
        level,
        client_id,
        keep_alive,
        clean_session: flags & 0x02 != 0,
        will,
        username,
        password,
    }))
}

fn parse_publish(flags: u8, body: &[u8]) -> Result<Packet<'_>, Error> {
    let qos = (flags >> 1) & 3;
    check(qos < 3, "qos")?;
    let (topic, mut at) = str_at(body, 0)?;
    check(!topic.is_empty() && !topic.contains(['+', '#']), "publish topic")?;
    let packet_id = if qos > 0 {
        let id = u16_at(body, at)?;
        at += 2;
        Some(id)
    } else {
        None
    };
    Ok(Packet::Publish { topic, payload: &body[at..], qos, retain: flags & 1 == 1, dup: flags & 8 != 0, packet_id })
}

fn parse_subscribe(flags: u8, body: &[u8]) -> Result<Packet<'_>, Error> {
    check(flags == 2, "subscribe flags")?;
    let packet_id = u16_at(body, 0)?;
    let mut at = 2;
    let mut filters = Vec::new();
    while at < body.len() {
        let (filter, next) = str_at(body, at)?;
        let qos = *body.get(next).ok_or(Error::Malformed("truncated subscribe"))?;
        check(valid_filter(filter), "topic filter")?;
        filters.push((filter, qos & 3));
        at = next + 1;
    }
    check(!filters.is_empty(), "empty subscribe")?;
    Ok(Packet::Subscribe { packet_id, filters })
}

fn parse_unsubscribe(flags: u8, body: &[u8]) -> Result<Packet<'_>, Error> {
    check(flags == 2, "unsubscribe flags")?;
    let packet_id = u16_at(body, 0)?;
    let mut at = 2;
    let mut filters = Vec::new();
    while at < body.len() {
        let (filter, next) = str_at(body, at)?;
        filters.push(filter);
        at = next;
    }
    Ok(Packet::Unsubscribe { packet_id, filters })
}

/// `+` must fill a whole level; `#` must be last and fill a whole level.
pub fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        if level.contains('#') && (*level != "#" || i != levels.len() - 1) {
            return false;
        }
        if level.contains('+') && *level != "+" {
            return false;
        }
    }
    true
}

/// MQTT matching of `topic` against `filter`, including the rule that wildcards at the first
/// level do not match topics starting with `$`.
pub fn matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

// ---- Encoding ------------------------------------------------------------------------------

fn put_header(out: &mut Vec<u8>, first: u8, remaining: usize) {
    out.push(first);
    let mut x = remaining;
    loop {
        let mut b = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            b |= 0x80;
        }
        out.push(b);
        if x == 0 {
            break;
        }
    }
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u16).to_be_bytes());
    out.extend_from_slice(b);
}

pub fn connect(out: &mut Vec<u8>, client_id: &str, keep_alive: u16, clean_session: bool) {
    connect_with(out, &Connect { level: 4, client_id, keep_alive, clean_session, ..Connect::default() });
}

pub fn connect_with(out: &mut Vec<u8>, c: &Connect<'_>) {
    let mut flags = (c.clean_session as u8) << 1;
    let mut len = 10 + 2 + c.client_id.len();
    if let Some(w) = &c.will {
        flags |= 0x04 | (w.qos << 3) | (w.retain as u8) << 5;
        len += 4 + w.topic.len() + w.payload.len();
    }
    if let Some(u) = c.username {
        flags |= 0x80;
        len += 2 + u.len();
    }
    if let Some(p) = c.password {
        flags |= 0x40;
        len += 2 + p.len();
    }
    put_header(out, 0x10, len);
    put_str(out, if c.level == 3 { "MQIsdp" } else { "MQTT" });
    out.push(c.level);
    out.push(flags);
    out.extend_from_slice(&c.keep_alive.to_be_bytes());
    put_str(out, c.client_id);
    if let Some(w) = &c.will {
        put_str(out, w.topic);
        put_bytes(out, w.payload);
    }
    if let Some(u) = c.username {
        put_str(out, u);
    }
    if let Some(p) = c.password {
        put_bytes(out, p);
    }
}

pub fn connack(out: &mut Vec<u8>, session_present: bool, code: u8) {
    out.extend_from_slice(&[0x20, 2, session_present as u8, code]);
}

pub fn publish(out: &mut Vec<u8>, topic: &str, payload: &[u8], qos: u8, retain: bool, packet_id: Option<u16>) {
    let id_len = if qos > 0 { 2 } else { 0 };
    put_header(out, 0x30 | (qos << 1) | retain as u8, 2 + topic.len() + id_len + payload.len());
    put_str(out, topic);
    if qos > 0 {
        out.extend_from_slice(&packet_id.unwrap_or(1).to_be_bytes());
    }
    out.extend_from_slice(payload);
}

/// Re-encodes a QoS 0 PUBLISH built by `publish` as QoS `qos` with `packet_id`, keeping its
/// retain flag: the broker encodes a message once and gives each QoS 1 subscriber its own id.
pub fn requalify(out: &mut Vec<u8>, packet: &[u8], qos: u8, packet_id: u16) {
    let (rem, var_len) = remaining_length(&packet[1..]).ok().flatten().expect("a packet built by `publish`");
    let body = &packet[1 + var_len..1 + var_len + rem];
    let topic_end = 2 + u16::from_be_bytes([body[0], body[1]]) as usize;
    put_header(out, 0x30 | (qos << 1) | (packet[0] & 0x01), rem + 2);
    out.extend_from_slice(&body[..topic_end]);
    out.extend_from_slice(&packet_id.to_be_bytes());
    out.extend_from_slice(&body[topic_end..]);
}

/// Sets the DUP flag of an encoded PUBLISH (redelivery).
pub fn set_dup(packet: &mut [u8]) {
    packet[0] |= 0x08;
}

pub fn puback(out: &mut Vec<u8>, packet_id: u16) {
    ack(out, 0x40, packet_id);
}

pub fn pubrec(out: &mut Vec<u8>, packet_id: u16) {
    ack(out, 0x50, packet_id);
}

pub fn pubrel(out: &mut Vec<u8>, packet_id: u16) {
    ack(out, 0x62, packet_id);
}

pub fn pubcomp(out: &mut Vec<u8>, packet_id: u16) {
    ack(out, 0x70, packet_id);
}

fn ack(out: &mut Vec<u8>, first: u8, packet_id: u16) {
    out.push(first);
    out.push(2);
    out.extend_from_slice(&packet_id.to_be_bytes());
}

pub fn subscribe(out: &mut Vec<u8>, packet_id: u16, filters: &[(&str, u8)]) {
    let len = 2 + filters.iter().map(|(f, _)| 3 + f.len()).sum::<usize>();
    put_header(out, 0x82, len);
    out.extend_from_slice(&packet_id.to_be_bytes());
    for (f, qos) in filters {
        put_str(out, f);
        out.push(*qos);
    }
}

pub fn suback(out: &mut Vec<u8>, packet_id: u16, codes: &[u8]) {
    put_header(out, 0x90, 2 + codes.len());
    out.extend_from_slice(&packet_id.to_be_bytes());
    out.extend_from_slice(codes);
}

pub fn unsubscribe(out: &mut Vec<u8>, packet_id: u16, filters: &[&str]) {
    let len = 2 + filters.iter().map(|f| 2 + f.len()).sum::<usize>();
    put_header(out, 0xa2, len);
    out.extend_from_slice(&packet_id.to_be_bytes());
    for f in filters {
        put_str(out, f);
    }
}

pub fn unsuback(out: &mut Vec<u8>, packet_id: u16) {
    out.push(0xb0);
    out.push(2);
    out.extend_from_slice(&packet_id.to_be_bytes());
}

pub fn pingreq(out: &mut Vec<u8>) {
    out.extend_from_slice(&[0xc0, 0]);
}

pub fn pingresp(out: &mut Vec<u8>) {
    out.extend_from_slice(&[0xd0, 0]);
}

pub fn disconnect(out: &mut Vec<u8>) {
    out.extend_from_slice(&[0xe0, 0]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(bytes: &[u8]) -> Packet<'_> {
        let (p, len) = parse(bytes).unwrap().unwrap();
        assert_eq!(len, bytes.len());
        p
    }

    #[test]
    fn requalify_keeps_topic_payload_and_retain() {
        let mut q0 = Vec::new();
        publish(&mut q0, "a/b", b"hello", 0, true, None);
        let mut q1 = Vec::new();
        requalify(&mut q1, &q0, 1, 42);
        let mut want = Vec::new();
        publish(&mut want, "a/b", b"hello", 1, true, Some(42));
        assert_eq!(q1, want);
        let mut q2 = Vec::new();
        requalify(&mut q2, &q0, 2, 43);
        assert!(matches!(roundtrip(&q2), Packet::Publish { qos: 2, packet_id: Some(43), retain: true, .. }));
        for (encode, want) in [
            (pubrec as fn(&mut Vec<u8>, u16), Packet::PubRec { packet_id: 9 }),
            (pubrel, Packet::PubRel { packet_id: 9 }),
            (pubcomp, Packet::PubComp { packet_id: 9 }),
        ] {
            let mut b = Vec::new();
            encode(&mut b, 9);
            assert_eq!(roundtrip(&b), want);
        }
        set_dup(&mut q1);
        assert!(matches!(roundtrip(&q1), Packet::Publish { dup: true, qos: 1, packet_id: Some(42), .. }));
    }

    #[test]
    fn packets_roundtrip() {
        let mut b = Vec::new();
        connect(&mut b, "client-1", 30, true);
        assert_eq!(
            roundtrip(&b),
            Packet::Connect(Connect {
                level: 4,
                client_id: "client-1",
                keep_alive: 30,
                clean_session: true,
                ..Connect::default()
            })
        );
        b.clear();
        let full = Connect {
            level: 4,
            client_id: "dev",
            keep_alive: 10,
            clean_session: false,
            will: Some(Will { topic: "dev/status", payload: b"offline", qos: 1, retain: true }),
            username: Some("user"),
            password: Some(b"secret"),
        };
        connect_with(&mut b, &full);
        assert_eq!(roundtrip(&b), Packet::Connect(full));

        let mut b = Vec::new();
        publish(&mut b, "a/b", b"hello", 1, true, Some(7));
        assert_eq!(
            roundtrip(&b),
            Packet::Publish { topic: "a/b", payload: b"hello", qos: 1, retain: true, dup: false, packet_id: Some(7) }
        );

        let mut b = Vec::new();
        subscribe(&mut b, 3, &[("a/+", 0), ("b/#", 1)]);
        assert_eq!(roundtrip(&b), Packet::Subscribe { packet_id: 3, filters: vec![("a/+", 0), ("b/#", 1)] });

        let mut b = Vec::new();
        suback(&mut b, 3, &[0, 0]);
        assert_eq!(roundtrip(&b), Packet::SubAck { packet_id: 3, codes: vec![0, 0] });

        let mut b = Vec::new();
        unsubscribe(&mut b, 4, &["a/+"]);
        assert_eq!(roundtrip(&b), Packet::Unsubscribe { packet_id: 4, filters: vec!["a/+"] });

        for (enc, want) in [
            (pingreq as fn(&mut Vec<u8>), Packet::PingReq),
            (pingresp, Packet::PingResp),
            (disconnect, Packet::Disconnect),
        ] {
            let mut b = Vec::new();
            enc(&mut b);
            assert_eq!(roundtrip(&b), want);
        }
    }

    #[test]
    fn large_payload_and_partial_input() {
        let payload = vec![7u8; 300_000];
        let mut b = Vec::new();
        publish(&mut b, "big", &payload, 0, false, None);
        for cut in [0, 1, 2, 4, b.len() - 1] {
            assert_eq!(parse(&b[..cut]).unwrap(), None);
        }
        match roundtrip(&b) {
            Packet::Publish { payload: p, .. } => assert_eq!(p.len(), 300_000),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn several_packets_in_one_buffer() {
        let mut b = Vec::new();
        pingreq(&mut b);
        publish(&mut b, "t", b"x", 0, false, None);
        let (p1, n1) = parse(&b).unwrap().unwrap();
        assert_eq!(p1, Packet::PingReq);
        let (p2, n2) = parse(&b[n1..]).unwrap().unwrap();
        assert!(matches!(p2, Packet::Publish { topic: "t", .. }));
        assert_eq!(n1 + n2, b.len());
    }

    #[test]
    fn topic_matching() {
        assert!(matches("a/b", "a/b"));
        assert!(!matches("a/b", "a/c"));
        assert!(matches("a/+", "a/b"));
        assert!(!matches("a/+", "a/b/c"));
        assert!(matches("a/#", "a/b/c"));
        assert!(matches("a/#", "a"));
        assert!(matches("#", "x/y"));
        assert!(matches("+/+", "x/y"));
        assert!(!matches("#", "$SYS/x"));
        assert!(!matches("+/x", "$SYS/x"));
        assert!(matches("$SYS/#", "$SYS/x"));
        assert!(valid_filter("a/+/b/#"));
        assert!(!valid_filter("a/b#"));
        assert!(!valid_filter("a/#/b"));
        assert!(!valid_filter("a+/b"));
    }

    #[test]
    fn malformed_input_is_rejected() {
        assert!(parse(&[0x30, 0xff, 0xff, 0xff, 0xff]).is_err());
        let mut b = Vec::new();
        publish(&mut b, "a/+", b"x", 0, false, None);
        assert!(parse(&b).is_err());
    }
}
