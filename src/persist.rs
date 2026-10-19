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

const MAGIC: &[u8; 8] = b"CRSTDB1\n";

#[derive(Default, Debug, PartialEq, Eq)]
pub struct Saved {
    pub retained: Vec<(String, Vec<u8>, u8)>,
    pub sessions: Vec<SavedSession>,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct SavedSession {
    pub client_id: String,
    pub filters: Vec<(String, u8)>,
    /// Packet id and encoding (empty: at the PUBREL step).
    pub inflight: Vec<(u16, Vec<u8>)>,
    /// QoS and QoS 0 encoding.
    pub queue: Vec<(u8, Vec<u8>)>,
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
    for (topic, payload, qos) in &s.retained {
        w.bytes(topic.as_bytes());
        w.bytes(payload);
        w.u8(*qos);
    }
    w.u32(s.sessions.len());
    for x in &s.sessions {
        w.bytes(x.client_id.as_bytes());
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
        for (q, p) in &x.queue {
            w.u8(*q);
            w.bytes(p);
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
        s.retained.push((r.string()?, r.bytes()?, r.u8()?));
    }
    for _ in 0..r.u32()? {
        let mut x = SavedSession { client_id: r.string()?, ..SavedSession::default() };
        for _ in 0..r.u32()? {
            x.filters.push((r.string()?, r.u8()?));
        }
        for _ in 0..r.u32()? {
            x.inflight.push((r.u16()?, r.bytes()?));
        }
        for _ in 0..r.u32()? {
            x.queue.push((r.u8()?, r.bytes()?));
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
            retained: vec![("a/b".into(), b"v".to_vec(), 1)],
            sessions: vec![SavedSession {
                client_id: "dev".into(),
                filters: vec![("cmd/#".into(), 1), ("x".into(), 2)],
                inflight: vec![(7, b"pkt".to_vec()), (8, Vec::new())],
                queue: vec![(2, b"q".to_vec())],
                received: vec![3, 4],
            }],
        };
        let bytes = encode(&s);
        assert_eq!(decode(&bytes).unwrap(), s);
        assert!(decode(&bytes[..bytes.len() - 1]).is_err());
        assert!(decode(b"garbage!").is_err());
    }
}
