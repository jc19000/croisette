//! Snapshots of the context tier on disk: retained messages and persistent sessions (their
//! subscriptions and undelivered QoS 1/2 messages).
//!
//! Each core writes `partition-<core>.db` in the persistence directory, every `autosave` period
//! (with io_uring) and at shutdown: a new file, then renamed over the old one, so a crash leaves
//! the previous snapshot whole. At start-up every snapshot in the directory is read and its
//! entries go to the partitions of the current cores (their number may have changed). Messages
//! received after the last snapshot are lost on a crash; nothing is fsynced.

use std::io;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"CRSTDB2\n";

/// A message as it travels between tiers and waits in queues: its QoS 0 MQTT 3.1.1 encoding
/// (with the retain flag for retained deliveries), and what MQTT 5 receivers also get.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct Msg {
    pub packet: Vec<u8>,
    /// MQTT 5 properties to forward (user properties, content type, response topic...).
    pub props: Vec<u8>,
    /// UNIX time (seconds) after which the message is dropped (MQTT 5 message expiry).
    pub expires: Option<u64>,
    /// Published with the retain flag (for Retain As Published subscriptions).
    pub retained: bool,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct SavedRetained {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub props: Vec<u8>,
    pub expires: Option<u64>,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct Saved {
    pub retained: Vec<SavedRetained>,
    pub sessions: Vec<SavedSession>,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct SavedSession {
    pub client_id: String,
    /// Session expiry interval, seconds (u32::MAX: never), and when it went offline (UNIX time).
    pub expiry: u32,
    pub offline_since: u64,
    /// Filters and their subscription options.
    pub filters: Vec<(String, u8)>,
    /// Packet id and encoding (empty: at the PUBREL step).
    pub inflight: Vec<(u16, Vec<u8>)>,
    /// Delivery options (QoS...) and message.
    pub queue: Vec<(u8, Msg)>,
    /// QoS 2 ids received from the client and not released.
    pub received: Vec<u16>,
}

pub fn partition_file(dir: &Path, core: usize) -> PathBuf {
    dir.join(format!("partition-{core}.db"))
}

struct Writer(Vec<u8>);

impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn opt_u64(&mut self, v: Option<u64>) {
        self.u8(v.is_some() as u8);
        self.u64(v.unwrap_or(0));
    }
    fn msg(&mut self, m: &Msg) {
        self.bytes(&m.packet);
        self.bytes(&m.props);
        self.opt_u64(m.expires);
        self.u8(m.retained as u8);
    }
    fn u32(&mut self, v: usize) {
        self.0.extend_from_slice(&(v as u32).to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u32(b.len());
        self.0.extend_from_slice(b);
    }
}

struct Reader<'a>(&'a [u8]);

fn corrupt() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "corrupt snapshot")
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> io::Result<&[u8]> {
        if self.0.len() < n {
            return Err(corrupt());
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> io::Result<usize> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize)
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn opt_u64(&mut self) -> io::Result<Option<u64>> {
        let some = self.u8()? != 0;
        let v = self.u64()?;
        Ok(some.then_some(v))
    }
    fn msg(&mut self) -> io::Result<Msg> {
        Ok(Msg { packet: self.bytes()?, props: self.bytes()?, expires: self.opt_u64()?, retained: self.u8()? != 0 })
    }
    fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let n = self.u32()?;
        Ok(self.take(n)?.to_vec())
    }
    fn string(&mut self) -> io::Result<String> {
        String::from_utf8(self.bytes()?).map_err(|_| corrupt())
    }
}

pub fn encode(s: &Saved) -> Vec<u8> {
    let mut w = Writer(MAGIC.to_vec());
    w.u32(s.retained.len());
    for r in &s.retained {
        w.bytes(r.topic.as_bytes());
        w.bytes(&r.payload);
        w.u8(r.qos);
        w.bytes(&r.props);
        w.opt_u64(r.expires);
    }
    w.u32(s.sessions.len());
    for x in &s.sessions {
        w.bytes(x.client_id.as_bytes());
        w.u32(x.expiry as usize);
        w.u64(x.offline_since);
        w.u32(x.filters.len());
        for (f, q) in &x.filters {
            w.bytes(f.as_bytes());
            w.u8(*q);
        }
        w.u32(x.inflight.len());
        for (id, p) in &x.inflight {
            w.u16(*id);
            w.bytes(p);
        }
        w.u32(x.queue.len());
        for (q, m) in &x.queue {
            w.u8(*q);
            w.msg(m);
        }
        w.u32(x.received.len());
        for id in &x.received {
            w.u16(*id);
        }
    }
    w.0
}

pub fn decode(data: &[u8]) -> io::Result<Saved> {
    let mut r = Reader(data);
    if r.take(MAGIC.len())? != MAGIC {
        return Err(corrupt());
    }
    let mut s = Saved::default();
    for _ in 0..r.u32()? {
        s.retained.push(SavedRetained {
            topic: r.string()?,
            payload: r.bytes()?,
            qos: r.u8()?,
            props: r.bytes()?,
            expires: r.opt_u64()?,
        });
    }
    for _ in 0..r.u32()? {
        let mut x = SavedSession {
            client_id: r.string()?,
            expiry: r.u32()? as u32,
            offline_since: r.u64()?,
            ..SavedSession::default()
        };
        for _ in 0..r.u32()? {
            x.filters.push((r.string()?, r.u8()?));
        }
        for _ in 0..r.u32()? {
            x.inflight.push((r.u16()?, r.bytes()?));
        }
        for _ in 0..r.u32()? {
            x.queue.push((r.u8()?, r.msg()?));
        }
        for _ in 0..r.u32()? {
            x.received.push(r.u16()?);
        }
        s.sessions.push(x);
    }
    if !r.0.is_empty() {
        return Err(corrupt());
    }
    Ok(s)
}

/// Every snapshot of `dir` merged, with the paths read.
pub fn load_dir(dir: &Path) -> io::Result<(Saved, Vec<PathBuf>)> {
    let mut all = Saved::default();
    let mut paths = Vec::new();
    if !dir.exists() {
        std::fs::create_dir_all(dir)?;
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with("partition-") && name.ends_with(".db") {
            let mut s = decode(&std::fs::read(&path)?)
                .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
            all.retained.append(&mut s.retained);
            all.sessions.append(&mut s.sessions);
            paths.push(path);
        }
    }
    Ok((all, paths))
}

/// Writes `data` to `path` through a temporary file and a rename.
pub fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let s = Saved {
            retained: vec![SavedRetained {
                topic: "a/b".into(),
                payload: b"v".to_vec(),
                qos: 1,
                props: vec![3, 0, 1, b'x'],
                expires: Some(99),
            }],
            sessions: vec![SavedSession {
                client_id: "dev".into(),
                expiry: 3600,
                offline_since: 12,
                filters: vec![("cmd/#".into(), 1), ("x".into(), 2)],
                inflight: vec![(7, b"pkt".to_vec()), (8, Vec::new())],
                queue: vec![(2, Msg { packet: b"q".to_vec(), props: Vec::new(), expires: None, retained: true })],
                received: vec![3, 4],
            }],
        };
        let bytes = encode(&s);
        assert_eq!(decode(&bytes).unwrap(), s);
        assert!(decode(&bytes[..bytes.len() - 1]).is_err());
        assert!(decode(b"garbage!").is_err());
    }
}
