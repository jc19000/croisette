//! A minimal blocking MQTT 3.1.1 client, for tests and the load generator.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

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
    pub fn queue_publish(&mut self, topic: &str, payload: &[u8], qos: u8, retain: bool) {
        let id = if qos > 0 { Some(self.id()) } else { None };
        codec::publish(&mut self.out, topic, payload, qos, retain, id);
    }

    pub fn publish(&mut self, topic: &str, payload: &[u8], qos: u8, retain: bool) -> io::Result<()> {
        self.queue_publish(topic, payload, qos, retain);
        self.flush()?;
        if qos == 1 {
            self.read_packet(|p| matches!(p, Packet::PubAck { .. }))?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.stream.write_all(&self.out)?;
        self.out.clear();
        Ok(())
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
        loop {
            let mut ack = None;
            let got = self.try_parse(|p| match p {
                Packet::Publish { topic, payload, qos, retain, packet_id, .. } => {
                    if qos == 1 {
                        ack = packet_id;
                    }
                    Some(Incoming::Publish { topic: topic.to_string(), payload: payload.to_vec(), qos, retain })
                }
                other => Some(Incoming::Other(format!("{other:?}"))),
            })?;
            if got.is_some() {
                self.last_packet_id = ack;
            }
            if let (Some(id), true) = (ack, self.auto_ack) {
                codec::puback(&mut self.acks, id);
                self.send_acks()?;
            }
            if let Some(i) = got {
                return Ok(i);
            }
        }
    }

    /// Calls `f` on each packet already buffered or read in one `read`; returns the first
    /// `Some`. Lets the load generator count PUBLISH packets without copying them.
    pub fn read_publishes(&mut self, mut on_publish: impl FnMut(&[u8])) -> io::Result<usize> {
        self.fill()?;
        let mut n = 0;
        while let Some((packet, len)) = codec::parse(&self.buf[self.start..]).map_err(io::Error::other)? {
            if let Packet::Publish { payload, qos, packet_id, .. } = packet {
                on_publish(payload);
                n += 1;
                if qos == 1 && self.auto_ack {
                    codec::puback(&mut self.acks, packet_id.unwrap_or(0));
                }
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
        let n = self.stream.read(&mut self.buf[len..]);
        self.buf.truncate(len + *n.as_ref().unwrap_or(&0));
        match n {
            Ok(0) => Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
