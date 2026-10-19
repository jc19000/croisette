//! MQTT 3.1.1 and 5 packets: zero-copy parsing (packets borrow the receive buffer) and encoding
//! into a caller-owned `Vec<u8>`, for both the broker and the load generator.
//!
//! MQTT 5 packets carry properties. The parser keeps the ones the broker acts on and, for
//! PUBLISH, the raw bytes of the others to forward them (`Publish::props`).

use std::borrow::Cow;
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
    ConnAck {
        session_present: bool,
        code: u8,
        props: &'a [u8],
    },
    Publish {
        topic: &'a str,
        payload: &'a [u8],
        qos: u8,
        retain: bool,
        dup: bool,
        packet_id: Option<u16>,
        /// MQTT 5: the properties to forward (all but the topic alias and the message expiry).
        props: Cow<'a, [u8]>,
        topic_alias: Option<u16>,
        expiry: Option<u32>,
    },
    PubAck {
        packet_id: u16,
    },
    PubRec {
        packet_id: u16,
    },
    PubRel {
        packet_id: u16,
    },
    PubComp {
        packet_id: u16,
    },
    /// Filters with their options byte: QoS in bits 0-1; MQTT 5: No Local (bit 2), Retain As
    /// Published (bit 3), Retain Handling (bits 4-5).
    Subscribe {
        packet_id: u16,
        filters: Vec<(&'a str, u8)>,
    },
    SubAck {
        packet_id: u16,
        codes: Vec<u8>,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<&'a str>,
    },
    UnsubAck {
        packet_id: u16,
    },
    PingReq,
    PingResp,
    /// MQTT 5: reason code and session expiry interval.
    Disconnect {
        reason: u8,
        session_expiry: Option<u32>,
    },
    /// MQTT 5 AUTH (enhanced authentication, not supported by the broker).
    Auth,
}

/// A CONNECT packet. `level` is 5 for MQTT 5, 4 for MQTT 3.1.1 and 3 for MQTT 3.1 (`MQIsdp`);
/// the broker refuses other levels with a CONNACK. `clean_session` is Clean Start in MQTT 5.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Connect<'a> {
    pub level: u8,
    pub client_id: &'a str,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<Will<'a>>,
    pub username: Option<&'a str>,
    pub password: Option<&'a [u8]>,
    /// MQTT 5 properties.
    pub session_expiry: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
    pub auth_method: Option<&'a str>,
}

/// The message the broker publishes when a client goes away without DISCONNECT.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Will<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub qos: u8,
    pub retain: bool,
    /// MQTT 5: properties to forward with the message, delay and message expiry.
    pub props: Vec<u8>,
    pub delay: Option<u32>,
    pub expiry: Option<u32>,
}

/// MQTT 5 reason codes the broker sends.
pub mod reason {
    pub const SUCCESS: u8 = 0x00;
    pub const NO_MATCHING_SUBSCRIBERS: u8 = 0x10;
    pub const UNSPECIFIED: u8 = 0x80;
    pub const MALFORMED: u8 = 0x81;
    pub const PROTOCOL_ERROR: u8 = 0x82;
    pub const UNSUPPORTED_VERSION: u8 = 0x84;
    pub const CLIENT_ID_NOT_VALID: u8 = 0x85;
    pub const BAD_CREDENTIALS: u8 = 0x86;
    pub const NOT_AUTHORIZED: u8 = 0x87;
    pub const BAD_AUTH_METHOD: u8 = 0x8C;
    pub const KEEP_ALIVE_TIMEOUT: u8 = 0x8D;
    pub const SESSION_TAKEN_OVER: u8 = 0x8E;
    pub const TOPIC_FILTER_INVALID: u8 = 0x8F;
    pub const NO_SUBSCRIPTION_EXISTED: u8 = 0x11;
    pub const PACKET_TOO_LARGE: u8 = 0x95;
    pub const TOPIC_ALIAS_INVALID: u8 = 0x94;
    pub const QUOTA_EXCEEDED: u8 = 0x97;
}

/// MQTT 5 property identifiers the broker reads or writes.
pub mod prop {
    pub const MESSAGE_EXPIRY: u8 = 0x02;
    pub const SUBSCRIPTION_ID: u8 = 0x0B;
    pub const SESSION_EXPIRY: u8 = 0x11;
    pub const ASSIGNED_CLIENT_ID: u8 = 0x12;
    pub const AUTH_METHOD: u8 = 0x15;
    pub const WILL_DELAY: u8 = 0x18;
    pub const REASON_STRING: u8 = 0x1F;
    pub const RECEIVE_MAXIMUM: u8 = 0x21;
    pub const TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
    pub const TOPIC_ALIAS: u8 = 0x23;
    pub const MAXIMUM_QOS: u8 = 0x24;
    pub const RETAIN_AVAILABLE: u8 = 0x25;
    pub const MAXIMUM_PACKET_SIZE: u8 = 0x27;
    pub const WILDCARD_AVAILABLE: u8 = 0x28;
    pub const SUBSCRIPTION_IDS_AVAILABLE: u8 = 0x29;
    pub const SHARED_AVAILABLE: u8 = 0x2A;
}

/// Default limit on the size of a packet (its remaining length), 1 MiB.
pub const MAX_PACKET: usize = 1 << 20;

/// Parses one MQTT 3.1.1 packet at the start of `buf`, of at most `MAX_PACKET` bytes.
/// `Ok(None)`: incomplete, wait for more bytes. `Ok(Some((packet, len)))`: `len` bytes consumed.
pub fn parse(buf: &[u8]) -> Result<Option<(Packet<'_>, usize)>, Error> {
    parse_limited(buf, MAX_PACKET, false)
}

/// `parse` for MQTT 5 packets.
pub fn parse_v5(buf: &[u8]) -> Result<Option<(Packet<'_>, usize)>, Error> {
    parse_limited(buf, MAX_PACKET, true)
}

/// Total size of the packet starting `buf`, once its fixed header is complete.
pub fn packet_len(buf: &[u8]) -> Option<usize> {
    let (rem, var_len) = remaining_length(buf.get(1..)?).ok()??;
    Some(1 + var_len + rem)
}

/// `parse` with a limit of `max` bytes on the remaining length, for MQTT 5 packets if `v5`
/// (CONNECT tells its own version).
pub fn parse_limited(buf: &[u8], max: usize, v5: bool) -> Result<Option<(Packet<'_>, usize)>, Error> {
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
            check(body.len() >= 2, "connack length")?;
            let props = if v5 && body.len() > 2 { props_at(body, 2)?.0 } else { &[] };
            Packet::ConnAck { session_present: body[0] & 1 == 1, code: body[1], props }
        }
        3 => parse_publish(flags, body, v5)?,
        // Acknowledgements: MQTT 5 may add a reason code and properties, skipped here.
        4 => Packet::PubAck { packet_id: u16_at(body, 0)? },
        5 => Packet::PubRec { packet_id: u16_at(body, 0)? },
        6 => {
            check(flags == 2, "pubrel flags")?;
            Packet::PubRel { packet_id: u16_at(body, 0)? }
        }
        7 => Packet::PubComp { packet_id: u16_at(body, 0)? },
        8 => parse_subscribe(flags, body, v5)?,
        9 => {
            let packet_id = u16_at(body, 0)?;
            let at = if v5 { skip_props(body, 2)? } else { 2 };
            Packet::SubAck { packet_id, codes: body[at..].to_vec() }
        }
        10 => parse_unsubscribe(flags, body, v5)?,
        11 => Packet::UnsubAck { packet_id: u16_at(body, 0)? },
        12 => Packet::PingReq,
        13 => Packet::PingResp,
        14 => {
            let reason = body.first().copied().unwrap_or(0);
            let mut session_expiry = None;
            if v5 && body.len() > 1 {
                for_each_prop(props_at(body, 1)?.0, |id, v| {
                    if id == prop::SESSION_EXPIRY {
                        session_expiry = Some(be32(v));
                    }
                })?;
            }
            Packet::Disconnect { reason, session_expiry }
        }
        15 if v5 => Packet::Auth,
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

/// A variable byte integer at `at`; returns it and the offset after it.
fn varint_at(buf: &[u8], at: usize) -> Result<(usize, usize), Error> {
    match remaining_length(buf.get(at..).unwrap_or(&[]))? {
        Some((v, n)) => Ok((v, at + n)),
        None => Err(Error::Malformed("truncated varint")),
    }
}

/// The properties at `at` (length, then the properties); returns them and the offset after.
fn props_at(buf: &[u8], at: usize) -> Result<(&[u8], usize), Error> {
    let (len, start) = varint_at(buf, at)?;
    let props = buf.get(start..start + len).ok_or(Error::Malformed("truncated properties"))?;
    Ok((props, start + len))
}

fn skip_props(buf: &[u8], at: usize) -> Result<usize, Error> {
    props_at(buf, at).map(|(_, next)| next)
}

fn be32(v: &[u8]) -> u32 {
    u32::from_be_bytes(v.try_into().unwrap())
}

fn be16(v: &[u8]) -> u16 {
    u16::from_be_bytes(v.try_into().unwrap())
}

/// Calls `f` with each property's identifier and value bytes (for strings and binary data, with
/// their length prefix; a string pair is one value). Returns the property boundaries checked.
fn for_each_prop<'a>(props: &'a [u8], mut f: impl FnMut(u8, &'a [u8])) -> Result<(), Error> {
    let mut at = 0;
    while at < props.len() {
        let id = props[at];
        let start = at + 1;
        let end = match id {
            0x01 | 0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => start + 1,
            0x13 | 0x21 | 0x22 | 0x23 => start + 2,
            0x02 | 0x11 | 0x18 | 0x27 => start + 4,
            0x0B => varint_at(props, start)?.1,
            0x03 | 0x08 | 0x09 | 0x12 | 0x15 | 0x16 | 0x1A | 0x1C | 0x1F => start + 2 + u16_at(props, start)? as usize,
            0x26 => {
                let k = start + 2 + u16_at(props, start)? as usize;
                k + 2 + u16_at(props, k)? as usize
            }
            _ => return Err(Error::Malformed("unknown property")),
        };
        let value = props.get(start..end).ok_or(Error::Malformed("truncated property"))?;
        f(id, value);
        at = end;
    }
    Ok(())
}

/// The value of property `id` in `props` (strings and binary data without their length), if
/// present.
pub fn prop_value(props: &[u8], id: u8) -> Option<&[u8]> {
    let mut found = None;
    for_each_prop(props, |i, v| {
        if i == id && found.is_none() {
            found = Some(v);
        }
    })
    .ok()?;
    let v = found?;
    Some(match id {
        0x03 | 0x08 | 0x09 | 0x12 | 0x15 | 0x16 | 0x1A | 0x1C | 0x1F => &v[2..],
        _ => v,
    })
}

/// The value of a string property as read by `for_each_prop`.
fn prop_value_str(v: &[u8]) -> Result<&str, Error> {
    std::str::from_utf8(&v[2..]).map_err(|_| Error::Malformed("invalid UTF-8"))
}

/// `props` without the properties `drop` lists (a borrow when there are none).
fn props_without<'a>(props: &'a [u8], drop: &[u8]) -> Result<Cow<'a, [u8]>, Error> {
    let mut kept: Option<Vec<u8>> = None;
    let mut at = 0;
    for_each_prop(props, |id, v| {
        let len = 1 + v.len();
        if drop.contains(&id) {
            kept.get_or_insert_with(|| props[..at].to_vec());
        } else if let Some(k) = kept.as_mut() {
            k.extend_from_slice(&props[at..at + len]);
        }
        at += len;
    })?;
    Ok(kept.map_or(Cow::Borrowed(props), Cow::Owned))
}

fn parse_connect(body: &[u8]) -> Result<Packet<'_>, Error> {
    let (name, at) = str_at(body, 0)?;
    check(name == "MQTT" || name == "MQIsdp", "protocol name")?;
    let level = *body.get(at).ok_or(Error::Malformed("truncated connect"))?;
    let flags = *body.get(at + 1).ok_or(Error::Malformed("truncated connect"))?;
    check(flags & 0x01 == 0, "connect reserved flag")?;
    let keep_alive = u16_at(body, at + 2)?;
    let mut c = Connect { level, keep_alive, clean_session: flags & 0x02 != 0, ..Connect::default() };
    let mut at = at + 4;
    if level == 5 {
        let (props, next) = props_at(body, at)?;
        at = next;
        let mut bad = None;
        for_each_prop(props, |id, v| match id {
            prop::SESSION_EXPIRY => c.session_expiry = Some(be32(v)),
            prop::RECEIVE_MAXIMUM => c.receive_maximum = Some(be16(v)),
            prop::MAXIMUM_PACKET_SIZE => c.maximum_packet_size = Some(be32(v)),
            prop::TOPIC_ALIAS_MAXIMUM => c.topic_alias_maximum = Some(be16(v)),
            prop::AUTH_METHOD => match prop_value_str(v) {
                Ok(m) => c.auth_method = Some(m),
                Err(e) => bad = Some(e),
            },
            _ => {}
        })?;
        if let Some(e) = bad {
            return Err(e);
        }
    }
    let (client_id, next) = str_at(body, at)?;
    c.client_id = client_id;
    at = next;
    let will = if flags & 0x04 != 0 {
        let (mut props, mut delay, mut expiry) = (Vec::new(), None, None);
        if level == 5 {
            let (raw, next) = props_at(body, at)?;
            at = next;
            for_each_prop(raw, |id, v| match id {
                prop::WILL_DELAY => delay = Some(be32(v)),
                prop::MESSAGE_EXPIRY => expiry = Some(be32(v)),
                _ => {}
            })?;
            props = props_without(raw, &[prop::WILL_DELAY, prop::MESSAGE_EXPIRY])?.into_owned();
        }
        let (topic, next) = str_at(body, at)?;
        let (payload, next) = bytes_at(body, next)?;
        at = next;
        let qos = (flags >> 3) & 3;
        check(qos < 3, "will qos")?;
        check(!topic.is_empty() && !topic.contains(['+', '#']), "will topic")?;
        Some(Will { topic, payload, qos, retain: flags & 0x20 != 0, props, delay, expiry })
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
    Ok(Packet::Connect(Connect { will, username, password, ..c }))
}

fn parse_publish(flags: u8, body: &[u8], v5: bool) -> Result<Packet<'_>, Error> {
    let qos = (flags >> 1) & 3;
    check(qos < 3, "qos")?;
    let (topic, mut at) = str_at(body, 0)?;
    // MQTT 5: an empty topic with a topic alias.
    check((v5 || !topic.is_empty()) && !topic.contains(['+', '#']), "publish topic")?;
    let packet_id = if qos > 0 {
        let id = u16_at(body, at)?;
        at += 2;
        Some(id)
    } else {
        None
    };
    let (mut props, mut topic_alias, mut expiry) = (Cow::Borrowed(&[][..]), None, None);
    if v5 {
        let (raw, next) = props_at(body, at)?;
        at = next;
        for_each_prop(raw, |id, v| match id {
            prop::TOPIC_ALIAS => topic_alias = Some(be16(v)),
            prop::MESSAGE_EXPIRY => expiry = Some(be32(v)),
            _ => {}
        })?;
        props = props_without(raw, &[prop::TOPIC_ALIAS, prop::MESSAGE_EXPIRY, prop::SUBSCRIPTION_ID])?;
    }
    Ok(Packet::Publish {
        topic,
        payload: &body[at..],
        qos,
        retain: flags & 1 == 1,
        dup: flags & 8 != 0,
        packet_id,
        props,
        topic_alias,
        expiry,
    })
}

fn parse_subscribe(flags: u8, body: &[u8], v5: bool) -> Result<Packet<'_>, Error> {
    check(flags == 2, "subscribe flags")?;
    let packet_id = u16_at(body, 0)?;
    let mut at = if v5 { skip_props(body, 2)? } else { 2 };
    let mut filters = Vec::new();
    while at < body.len() {
        let (filter, next) = str_at(body, at)?;
        let options = *body.get(next).ok_or(Error::Malformed("truncated subscribe"))?;
        check(valid_filter(filter), "topic filter")?;
        check(options & 3 < 3 && options & if v5 { 0xC0 } else { 0xFC } == 0, "subscription options")?;
        filters.push((filter, options));
        at = next + 1;
    }
    check(!filters.is_empty(), "empty subscribe")?;
    Ok(Packet::Subscribe { packet_id, filters })
}

fn parse_unsubscribe(flags: u8, body: &[u8], v5: bool) -> Result<Packet<'_>, Error> {
    check(flags == 2, "unsubscribe flags")?;
    let packet_id = u16_at(body, 0)?;
    let mut at = if v5 { skip_props(body, 2)? } else { 2 };
    let mut filters = Vec::new();
    while at < body.len() {
        let (filter, next) = str_at(body, at)?;
        filters.push(filter);
        at = next;
    }
    Ok(Packet::Unsubscribe { packet_id, filters })
}

/// `+` must fill a whole level; `#` must be last and fill a whole level. A shared subscription
/// `$share/<group>/<filter>` needs a group name without wildcards and a valid filter.
pub fn valid_filter(filter: &str) -> bool {
    if let Some(rest) = filter.strip_prefix("$share/") {
        return match rest.split_once('/') {
            Some((group, f)) => !group.is_empty() && !group.contains(['+', '#']) && valid_filter(f),
            None => false,
        };
    }
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
    let v5 = c.level == 5;
    let mut props = Vec::new();
    let mut will_props = Vec::new();
    if v5 {
        if let Some(v) = c.session_expiry {
            prop_u32(&mut props, prop::SESSION_EXPIRY, v);
        }
        if let Some(v) = c.receive_maximum {
            prop_u16(&mut props, prop::RECEIVE_MAXIMUM, v);
        }
        if let Some(v) = c.maximum_packet_size {
            prop_u32(&mut props, prop::MAXIMUM_PACKET_SIZE, v);
        }
        if let Some(v) = c.topic_alias_maximum {
            prop_u16(&mut props, prop::TOPIC_ALIAS_MAXIMUM, v);
        }
        if let Some(m) = c.auth_method {
            prop_str(&mut props, prop::AUTH_METHOD, m);
        }
        if let Some(w) = &c.will {
            if let Some(d) = w.delay {
                prop_u32(&mut will_props, prop::WILL_DELAY, d);
            }
            if let Some(e) = w.expiry {
                prop_u32(&mut will_props, prop::MESSAGE_EXPIRY, e);
            }
            will_props.extend_from_slice(&w.props);
        }
    }
    let mut flags = (c.clean_session as u8) << 1;
    let mut len = 10 + 2 + c.client_id.len();
    if v5 {
        len += varint_len(props.len()) + props.len();
    }
    if let Some(w) = &c.will {
        flags |= 0x04 | (w.qos << 3) | (w.retain as u8) << 5;
        len += 4 + w.topic.len() + w.payload.len();
        if v5 {
            len += varint_len(will_props.len()) + will_props.len();
        }
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
    if v5 {
        put_varint(out, props.len());
        out.extend_from_slice(&props);
    }
    put_str(out, c.client_id);
    if let Some(w) = &c.will {
        if v5 {
            put_varint(out, will_props.len());
            out.extend_from_slice(&will_props);
        }
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

/// MQTT 5 CONNACK with a reason code and properties.
pub fn connack_v5(out: &mut Vec<u8>, session_present: bool, reason: u8, props: &[u8]) {
    put_header(out, 0x20, 2 + varint_len(props.len()) + props.len());
    out.push(session_present as u8);
    out.push(reason);
    put_varint(out, props.len());
    out.extend_from_slice(props);
}

/// MQTT 5 PUBLISH with properties.
#[allow(clippy::too_many_arguments)]
pub fn publish_v5(
    out: &mut Vec<u8>,
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
    packet_id: Option<u16>,
    props: &[u8],
) {
    let id_len = if qos > 0 { 2 } else { 0 };
    let plen = varint_len(props.len()) + props.len();
    put_header(out, 0x30 | (qos << 1) | retain as u8, 2 + topic.len() + id_len + plen + payload.len());
    put_str(out, topic);
    if qos > 0 {
        out.extend_from_slice(&packet_id.unwrap_or(1).to_be_bytes());
    }
    put_varint(out, props.len());
    out.extend_from_slice(props);
    out.extend_from_slice(payload);
}

/// Re-encodes a QoS 0 MQTT 3.1.1 PUBLISH built by `publish` as an MQTT 5 PUBLISH at QoS `qos`
/// (with `packet_id` if above 0) with properties `props` and, if given, a message expiry.
pub fn requalify_v5(out: &mut Vec<u8>, packet: &[u8], qos: u8, packet_id: u16, props: &[u8], expiry: Option<u32>) {
    let (rem, var_len) = remaining_length(&packet[1..]).ok().flatten().expect("a packet built by `publish`");
    let body = &packet[1 + var_len..1 + var_len + rem];
    let topic_end = 2 + u16::from_be_bytes([body[0], body[1]]) as usize;
    let plen = props.len() + if expiry.is_some() { 5 } else { 0 };
    let id_len = if qos > 0 { 2 } else { 0 };
    put_header(out, 0x30 | (qos << 1) | (packet[0] & 0x01), rem + id_len + varint_len(plen) + plen);
    out.extend_from_slice(&body[..topic_end]);
    if qos > 0 {
        out.extend_from_slice(&packet_id.to_be_bytes());
    }
    put_varint(out, plen);
    if let Some(e) = expiry {
        prop_u32(out, prop::MESSAGE_EXPIRY, e);
    }
    out.extend_from_slice(props);
    out.extend_from_slice(&body[topic_end..]);
}

/// MQTT 5 PUBACK / PUBREC / PUBREL / PUBCOMP with a reason code (`first`: 0x40, 0x50, 0x62,
/// 0x70); the short form for success.
pub fn ack_v5(out: &mut Vec<u8>, first: u8, packet_id: u16, reason: u8) {
    if reason == reason::SUCCESS {
        return ack(out, first, packet_id);
    }
    out.extend_from_slice(&[first, 3]);
    out.extend_from_slice(&packet_id.to_be_bytes());
    out.push(reason);
}

pub fn suback_v5(out: &mut Vec<u8>, packet_id: u16, codes: &[u8]) {
    put_header(out, 0x90, 3 + codes.len());
    out.extend_from_slice(&packet_id.to_be_bytes());
    out.push(0);
    out.extend_from_slice(codes);
}

pub fn unsuback_v5(out: &mut Vec<u8>, packet_id: u16, codes: &[u8]) {
    put_header(out, 0xb0, 3 + codes.len());
    out.extend_from_slice(&packet_id.to_be_bytes());
    out.push(0);
    out.extend_from_slice(codes);
}

/// MQTT 5 SUBSCRIBE, with each filter's options byte.
pub fn subscribe_v5(out: &mut Vec<u8>, packet_id: u16, filters: &[(&str, u8)]) {
    let len = 3 + filters.iter().map(|(f, _)| 3 + f.len()).sum::<usize>();
    put_header(out, 0x82, len);
    out.extend_from_slice(&packet_id.to_be_bytes());
    out.push(0);
    for (f, options) in filters {
        put_str(out, f);
        out.push(*options);
    }
}

pub fn unsubscribe_v5(out: &mut Vec<u8>, packet_id: u16, filters: &[&str]) {
    let len = 3 + filters.iter().map(|f| 2 + f.len()).sum::<usize>();
    put_header(out, 0xa2, len);
    out.extend_from_slice(&packet_id.to_be_bytes());
    out.push(0);
    for f in filters {
        put_str(out, f);
    }
}

/// MQTT 5 DISCONNECT with a reason code.
pub fn disconnect_v5(out: &mut Vec<u8>, reason: u8) {
    out.extend_from_slice(&[0xe0, 1, reason]);
}

pub fn put_varint(out: &mut Vec<u8>, mut x: usize) {
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

fn varint_len(x: usize) -> usize {
    match x {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

pub fn prop_u8(out: &mut Vec<u8>, id: u8, v: u8) {
    out.extend_from_slice(&[id, v]);
}

pub fn prop_u16(out: &mut Vec<u8>, id: u8, v: u16) {
    out.push(id);
    out.extend_from_slice(&v.to_be_bytes());
}

pub fn prop_u32(out: &mut Vec<u8>, id: u8, v: u32) {
    out.push(id);
    out.extend_from_slice(&v.to_be_bytes());
}

pub fn prop_str(out: &mut Vec<u8>, id: u8, v: &str) {
    out.push(id);
    put_str(out, v);
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
            will: Some(Will { topic: "dev/status", payload: b"offline", qos: 1, retain: true, ..Will::default() }),
            username: Some("user"),
            password: Some(b"secret"),
            ..Connect::default()
        };
        connect_with(&mut b, &full);
        assert_eq!(roundtrip(&b), Packet::Connect(full));

        let mut b = Vec::new();
        publish(&mut b, "a/b", b"hello", 1, true, Some(7));
        assert_eq!(
            roundtrip(&b),
            Packet::Publish {
                topic: "a/b",
                payload: b"hello",
                qos: 1,
                retain: true,
                dup: false,
                packet_id: Some(7),
                props: Cow::Borrowed(&[]),
                topic_alias: None,
                expiry: None
            }
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
            (disconnect, Packet::Disconnect { reason: 0, session_expiry: None }),
        ] {
            let mut b = Vec::new();
            enc(&mut b);
            assert_eq!(roundtrip(&b), want);
        }
    }

    #[test]
    fn mqtt5_packets() {
        // CONNECT with properties and a will with properties.
        let mut user = Vec::new();
        prop_u8(&mut user, 0x01, 1); // payload format indicator, forwarded
        let c = Connect {
            level: 5,
            client_id: "v5",
            keep_alive: 30,
            clean_session: true,
            session_expiry: Some(3600),
            receive_maximum: Some(10),
            maximum_packet_size: Some(4096),
            topic_alias_maximum: Some(5),
            will: Some(Will {
                topic: "w",
                payload: b"bye",
                qos: 1,
                retain: false,
                props: user.clone(),
                delay: Some(5),
                expiry: Some(60),
            }),
            ..Connect::default()
        };
        let mut b = Vec::new();
        connect_with(&mut b, &c);
        assert_eq!(roundtrip(&b), Packet::Connect(c));
        // PUBLISH: the topic alias and the message expiry are taken out of the forwarded
        // properties.
        let mut props = Vec::new();
        prop_u16(&mut props, prop::TOPIC_ALIAS, 3);
        prop_str(&mut props, 0x03, "text/plain"); // content type
        prop_u32(&mut props, prop::MESSAGE_EXPIRY, 30);
        let mut b = Vec::new();
        publish_v5(&mut b, "t", b"x", 1, false, Some(9), &props);
        let (p, _) = parse_v5(&b).unwrap().unwrap();
        let Packet::Publish { topic_alias, expiry, props: fwd, packet_id, payload, .. } = p else { panic!() };
        assert_eq!((topic_alias, expiry, packet_id, payload), (Some(3), Some(30), Some(9), &b"x"[..]));
        assert_eq!(prop_value(&fwd, 0x03), Some(&b"text/plain"[..]));
        assert_eq!(prop_value(&fwd, prop::TOPIC_ALIAS), None);
        // requalify_v5 from a 3.1.1 encoding.
        let mut q0 = Vec::new();
        publish(&mut q0, "t", b"x", 0, false, None);
        let mut b = Vec::new();
        requalify_v5(&mut b, &q0, 2, 11, &fwd, Some(20));
        let (p, len) = parse_v5(&b).unwrap().unwrap();
        assert_eq!(len, b.len());
        assert!(matches!(p, Packet::Publish { qos: 2, packet_id: Some(11), expiry: Some(20), .. }));
        // SUBSCRIBE options, SUBACK, DISCONNECT with a session expiry.
        let mut b = Vec::new();
        subscribe_v5(&mut b, 4, &[("a/#", 0x2D)]);
        assert_eq!(parse_v5(&b).unwrap().unwrap().0, Packet::Subscribe { packet_id: 4, filters: vec![("a/#", 0x2D)] });
        let mut b = Vec::new();
        suback_v5(&mut b, 4, &[1, 0x87]);
        assert_eq!(parse_v5(&b).unwrap().unwrap().0, Packet::SubAck { packet_id: 4, codes: vec![1, 0x87] });
        let b = [0xe0, 7, 0x04, 5, prop::SESSION_EXPIRY, 0, 0, 0, 9];
        assert_eq!(parse_v5(&b).unwrap().unwrap().0, Packet::Disconnect { reason: 4, session_expiry: Some(9) });
        let mut b = Vec::new();
        connack_v5(&mut b, true, 0, &[prop::ASSIGNED_CLIENT_ID, 0, 1, b'z']);
        let (Packet::ConnAck { session_present: true, code: 0, props }, _) = parse_v5(&b).unwrap().unwrap() else {
            panic!()
        };
        assert_eq!(prop_value(props, prop::ASSIGNED_CLIENT_ID), Some(&b"z"[..]));
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
