//! A minimal blocking MQTT 3.1.1 client, for tests and the load generator.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

pub use rustls::ClientConfig;

use crate::codec::{self, Connect, Packet};

pub struct Client {
    pub stream: TcpStream,
    buf: Vec<u8>,
    start: usize,
    out: Vec<u8>,
    acks: Vec<u8>,
    next_id: u16,
    /// PUBACK every QoS 1 message as it is read (default).
    pub auto_ack: bool,
    /// Packet id of the last message returned by `next_publish` (QoS 1).
    pub last_packet_id: Option<u16>,
    /// DUP flag of the last message returned by `next_publish` (a redelivery).
    pub last_dup: bool,
    /// Ids of QoS 2 messages received and not released by the broker yet.
    qos2: Vec<u16>,
    /// TLS over `stream`, if connected with `connect_tls`.
    tls: Option<Box<rustls::ClientConnection>>,
}

/// A TLS client configuration trusting the CA certificates of a PEM file's contents.
pub fn tls_config(ca_pem: &[u8]) -> io::Result<Arc<ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut &ca_pem[..]) {
        roots.add(cert?).map_err(io::Error::other)?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// A packet read from the broker, owned.
#[derive(Debug, PartialEq, Eq)]
pub enum Incoming {
    Publish { topic: String, payload: Vec<u8>, qos: u8, retain: bool },
    Other(String),
}

impl Client {
    /// Connects with a clean session and a 60 s keep-alive, and completes the CONNECT / CONNACK
    /// exchange.
    pub fn connect(addr: SocketAddr, client_id: &str) -> io::Result<Client> {
        let c = Connect { level: 4, client_id, keep_alive: 60, clean_session: true, ..Connect::default() };
        Client::connect_with(addr, &c).map(|(client, _)| client)
    }

    /// Connects with any CONNECT packet. Returns the client and the CONNACK's session-present
    /// flag; a refusing CONNACK is an error of kind `ConnectionRefused` naming the code.
    pub fn connect_with(addr: SocketAddr, connect: &Connect<'_>) -> io::Result<(Client, bool)> {
        Client::open(addr, connect, None)
    }

    /// Connects over TLS, checking that the broker's certificate is valid for `server_name`.
    pub fn connect_tls(
        addr: SocketAddr,
        client_id: &str,
        config: Arc<ClientConfig>,
        server_name: &str,
    ) -> io::Result<Client> {
        let name = rustls::pki_types::ServerName::try_from(server_name.to_string()).map_err(io::Error::other)?;
        let tls = rustls::ClientConnection::new(config, name).map_err(io::Error::other)?;
        let c = Connect { level: 4, client_id, keep_alive: 60, clean_session: true, ..Connect::default() };
        Client::open(addr, &c, Some(Box::new(tls))).map(|(client, _)| client)
    }

    fn open(
        addr: SocketAddr,
        connect: &Connect<'_>,
        tls: Option<Box<rustls::ClientConnection>>,
    ) -> io::Result<(Client, bool)> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let mut c = Client {
            stream,
            buf: Vec::with_capacity(64 * 1024),
            start: 0,
            out: Vec::new(),
            acks: Vec::new(),
            next_id: 1,
            auto_ack: true,
            last_packet_id: None,
            last_dup: false,
            qos2: Vec::new(),
            tls,
        };
        codec::connect_with(&mut c.out, connect);
        c.flush()?;
        let mut ack = None;
        c.read_packet(|p| match p {
            Packet::ConnAck { session_present, code } => {
                ack = Some((*session_present, *code));
                true
            }
            _ => false,
        })?;
        match ack {
            Some((session_present, 0)) => Ok((c, session_present)),
            Some((_, code)) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, format!("CONNACK code {code}"))),
            None => Err(io::Error::other("expected CONNACK")),
        }
    }

    /// Sends DISCONNECT (the broker then drops the will) and closes.
    pub fn disconnect(mut self) -> io::Result<()> {
        codec::disconnect(&mut self.out);
        self.flush()
    }

    /// Sends PINGREQ and waits for PINGRESP.
    pub fn ping(&mut self) -> io::Result<()> {
        codec::pingreq(&mut self.out);
        self.flush()?;
        self.read_packet(|p| matches!(p, Packet::PingResp))?;
        Ok(())
    }

    pub fn set_timeout(&self, t: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(t)
    }

    fn id(&mut self) -> u16 {
        self.next_id = self.next_id.wrapping_add(1).max(1);
        self.next_id
    }

    /// Queues a PUBLISH; call `flush` to send queued packets.
    /// Returns the packet id (QoS 1 and 2).
    pub fn queue_publish(&mut self, topic: &str, payload: &[u8], qos: u8, retain: bool) -> Option<u16> {
        let id = if qos > 0 { Some(self.id()) } else { None };
        codec::publish(&mut self.out, topic, payload, qos, retain, id);
        id
    }

    pub fn publish(&mut self, topic: &str, payload: &[u8], qos: u8, retain: bool) -> io::Result<()> {
        let id = self.queue_publish(topic, payload, qos, retain);
        self.flush()?;
        match (qos, id) {
            (1, _) => {
                self.read_packet(|p| matches!(p, Packet::PubAck { .. }))?;
            }
            (2, Some(id)) => {
                self.read_packet(|p| matches!(p, Packet::PubRec { packet_id } if *packet_id == id))?;
                codec::pubrel(&mut self.out, id);
                self.flush()?;
                self.read_packet(|p| matches!(p, Packet::PubComp { packet_id } if *packet_id == id))?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Waits for the PUBACK of a QoS 1 publish sent with `queue_publish`.
    pub fn wait_puback(&mut self) -> io::Result<()> {
        self.read_packet(|p| matches!(p, Packet::PubAck { .. })).map(|_| ())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let out = std::mem::take(&mut self.out);
        let written = self.write_raw(&out);
        self.out = out;
        self.out.clear();
        written
    }

    fn write_raw(&mut self, data: &[u8]) -> io::Result<()> {
        match self.tls.as_mut() {
            None => self.stream.write_all(data),
            Some(tls) => {
                tls.writer().write_all(data)?;
                while tls.wants_write() {
                    tls.write_tls(&mut self.stream)?;
                }
                Ok(())
            }
        }
    }

    /// Subscribes at QoS 0 and waits for the SUBACK.
    pub fn subscribe(&mut self, filters: &[&str]) -> io::Result<()> {
        self.subscribe_qos(filters, 0).map(|_| ())
    }

    /// Subscribes at QoS `qos`, waits for the SUBACK and returns the granted QoS.
    pub fn subscribe_qos(&mut self, filters: &[&str], qos: u8) -> io::Result<Vec<u8>> {
        let id = self.id();
        let f: Vec<(&str, u8)> = filters.iter().map(|f| (*f, qos)).collect();
        codec::subscribe(&mut self.out, id, &f);
        self.flush()?;
        let mut granted = Vec::new();
        self.read_packet(|p| match p {
            Packet::SubAck { codes, .. } => {
                granted = codes.clone();
                true
            }
            _ => false,
        })?;
        Ok(granted)
    }

    pub fn unsubscribe(&mut self, filters: &[&str]) -> io::Result<()> {
        let id = self.id();
        codec::unsubscribe(&mut self.out, id, filters);
        self.flush()?;
        self.read_packet(|p| matches!(p, Packet::UnsubAck { .. }))?;
        Ok(())
    }

    /// Reads packets until one satisfies `want` (returns true) or a non-matching one arrives
    /// first (returns false). PUBLISH packets read on the way are dropped.
    fn read_packet(&mut self, mut want: impl FnMut(&Packet<'_>) -> bool) -> io::Result<bool> {
        loop {
            if let Some(found) = self.try_parse(|p| match p {
                Packet::Publish { .. } => None,
                other => Some(want(&other)),
            })? {
                return Ok(found);
            }
        }
    }

    /// Next PUBLISH from the broker, blocking.
    pub fn next_publish(&mut self) -> io::Result<Incoming> {
        enum Got {
            Msg(Incoming, u8, Option<u16>, bool),
            Rel(u16),
            Other(String),
        }
        loop {
            let got = self.try_parse(|p| {
                Some(match p {
                    Packet::Publish { topic, payload, qos, retain, packet_id, dup } => {
                        let m = Incoming::Publish { topic: topic.to_string(), payload: payload.to_vec(), qos, retain };
                        Got::Msg(m, qos, packet_id, dup)
                    }
                    Packet::PubRel { packet_id } => Got::Rel(packet_id),
                    other => Got::Other(format!("{other:?}")),
                })
            })?;
            match got {
                None => {}
                Some(Got::Rel(id)) => self.released(id)?,
                Some(Got::Other(s)) => return Ok(Incoming::Other(s)),
                Some(Got::Msg(m, qos, packet_id, dup)) => {
                    self.last_packet_id = packet_id;
                    self.last_dup = dup;
                    if self.on_publish(qos, packet_id.unwrap_or(0)) {
                        self.send_acks()?;
                        return Ok(m);
                    }
                    self.send_acks()?;
                }
            }
        }
    }

    /// Acknowledges a QoS 1 or 2 PUBLISH (with `auto_ack`); false for a QoS 2 redelivery of a
    /// message already returned (exactly once).
    fn on_publish(&mut self, qos: u8, packet_id: u16) -> bool {
        match qos {
            1 if self.auto_ack => codec::puback(&mut self.acks, packet_id),
            2 => {
                if self.auto_ack {
                    codec::pubrec(&mut self.acks, packet_id);
                }
                if self.qos2.contains(&packet_id) {
                    return false;
                }
                self.qos2.push(packet_id);
            }
            _ => {}
        }
        true
    }

    /// PUBREL from the broker: answered with PUBCOMP.
    fn released(&mut self, packet_id: u16) -> io::Result<()> {
        self.qos2.retain(|&i| i != packet_id);
        codec::pubcomp(&mut self.acks, packet_id);
        self.send_acks()
    }

    /// Sends PUBREC for a QoS 2 message (when `auto_ack` is off).
    pub fn pubrec(&mut self, packet_id: u16) -> io::Result<()> {
        codec::pubrec(&mut self.acks, packet_id);
        self.send_acks()
    }

    /// Calls `f` on each packet already buffered or read in one `read`; returns the first
    /// `Some`. Lets the load generator count PUBLISH packets without copying them.
    pub fn read_publishes(&mut self, mut on_publish: impl FnMut(&[u8])) -> io::Result<usize> {
        self.fill()?;
        let mut n = 0;
        while let Some((packet, len)) = codec::parse(&self.buf[self.start..]).map_err(io::Error::other)? {
            match packet {
                Packet::Publish { payload, qos, packet_id, .. } => {
                    let pid = packet_id.unwrap_or(0);
                    let fresh = match qos {
                        1 if self.auto_ack => {
                            codec::puback(&mut self.acks, pid);
                            true
                        }
                        2 => {
                            codec::pubrec(&mut self.acks, pid);
                            let fresh = !self.qos2.contains(&pid);
                            if fresh {
                                self.qos2.push(pid);
                            }
                            fresh
                        }
                        _ => true,
                    };
                    if fresh {
                        on_publish(payload);
                        n += 1;
                    }
                }
                Packet::PubRel { packet_id } => {
                    self.qos2.retain(|&i| i != packet_id);
                    codec::pubcomp(&mut self.acks, packet_id);
                }
                _ => {}
            }
            self.start += len;
        }
        self.send_acks()?;
        Ok(n)
    }

    /// Acknowledges a QoS 1 message (when `auto_ack` is off).
    pub fn puback(&mut self, packet_id: u16) -> io::Result<()> {
        codec::puback(&mut self.acks, packet_id);
        self.send_acks()
    }

    /// Writes pending PUBACKs; on a non-blocking stream, what does not fit waits for the next
    /// call.
    fn send_acks(&mut self) -> io::Result<()> {
        if self.tls.is_some() {
            let acks = std::mem::take(&mut self.acks);
            return self.write_raw(&acks);
        }
        let mut done = 0;
        while done < self.acks.len() {
            match self.stream.write(&self.acks[done..]) {
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        self.acks.drain(..done);
        Ok(())
    }

    fn try_parse<T>(&mut self, mut f: impl FnMut(Packet<'_>) -> Option<T>) -> io::Result<Option<T>> {
        loop {
            match codec::parse(&self.buf[self.start..]).map_err(io::Error::other)? {
                Some((packet, len)) => {
                    self.start += len;
                    if let Some(t) = f(packet) {
                        return Ok(Some(t));
                    }
                }
                None => {
                    self.fill()?;
                    return Ok(None);
                }
            }
        }
    }

    fn fill(&mut self) -> io::Result<()> {
        if self.start > 0 {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        let len = self.buf.len();
        self.buf.resize(len + 64 * 1024, 0);
        if let Some(tls) = self.tls.as_mut() {
            loop {
                match tls.reader().read(&mut self.buf[len..]) {
                    Ok(0) => {
                        self.buf.truncate(len);
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                    Ok(n) => {
                        self.buf.truncate(len + n);
                        return Ok(());
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => {
                        self.buf.truncate(len);
                        return Err(e);
                    }
                }
                // No plaintext yet: more ciphertext.
                match tls.read_tls(&mut self.stream) {
                    Ok(0) => {
                        self.buf.truncate(len);
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                    Ok(_) => {
                        tls.process_new_packets().map_err(io::Error::other)?;
                        while tls.wants_write() {
                            tls.write_tls(&mut self.stream)?;
                        }
                    }
                    Err(e) => {
                        self.buf.truncate(len);
                        return Err(e);
                    }
                }
            }
        }
        let n = self.stream.read(&mut self.buf[len..]);
        self.buf.truncate(len + *n.as_ref().unwrap_or(&0));
        match n {
            Ok(0) => Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
