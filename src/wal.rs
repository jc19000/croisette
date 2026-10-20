//! Write-ahead log of the storage tier (`--storage wal`).
//!
//! Each core appends to its own segment file `wal-<core>-<seq>.log`:
//! - the QoS 1/2 messages it receives (at least `durable_qos`);
//! - changes of retained messages;
//! - and of persistent sessions (subscriptions, end).
//!
//! The appends of a loop iteration go out as one io_uring write, and the PUBACK / PUBREC of the
//! messages it holds are sent once it completes (group commit: one write per iteration, however
//! many messages). In `batch` mode the file is opened O_DSYNC, so a write completes once on disk:
//! an acknowledged message survives a power cut. In the other modes it survives a broker crash.
//!
//! Snapshots (`persist`) are the checkpoints: at each one every core starts a new segment and
//! deletes its segments older than the previous checkpoint, whose effects are in the snapshots
//! by then. At start-up the remaining segments are replayed on top of the snapshots: messages
//! go to the queues of the persistent sessions whose filters match, so a message logged before a
//! crash may be delivered twice (at least once), never lost.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::codec;
use crate::persist::{Msg, Saved, SavedRetained, SavedSession};

/// When the log reaches the disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fsync {
    /// Every write is durable when it completes (O_DSYNC), and acknowledgements wait for it.
    Batch,
    /// A background thread flushes the log every period: a power cut loses up to one period.
    Interval(std::time::Duration),
    /// The kernel writes back when it wants: safe against a broker crash, not a power cut.
    Never,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalConfig {
    pub fsync: Fsync,
    /// Messages of at least this QoS are logged (1 or 2).
    pub durable_qos: u8,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig { fsync: Fsync::Batch, durable_qos: 1 }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Record {
    Publish {
        topic: String,
        qos: u8,
        msg: Msg,
    },
    Retain(SavedRetained),
    /// A persistent session's subscriptions and expiry (created or changed).
    Session {
        client_id: String,
        expiry: u32,
        filters: Vec<(String, u8)>,
    },
    SessionEnd {
        client_id: String,
    },
}

const PUBLISH: u8 = 1;
const RETAIN: u8 = 2;
const SESSION: u8 = 3;
const SESSION_END: u8 = 4;

pub fn segment_path(dir: &Path, core: usize, seq: u64) -> PathBuf {
    dir.join(format!("wal-{core}-{seq:010}.log"))
}

/// The segments of `dir`: (sequence number, core, path), in order.
pub fn segments(dir: &Path) -> io::Result<Vec<(u64, usize, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
        let Some(rest) = name.strip_prefix("wal-").and_then(|r| r.strip_suffix(".log")) else { continue };
        let Some((core, seq)) = rest.split_once('-') else { continue };
        if let (Ok(core), Ok(seq)) = (core.parse(), seq.parse()) {
            out.push((seq, core, path));
        }
    }
    out.sort();
    Ok(out)
}

// ---- Encoding --------------------------------------------------------------------------------

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}
fn put_opt_u64(out: &mut Vec<u8>, v: Option<u64>) {
    out.push(v.is_some() as u8);
    out.extend_from_slice(&v.unwrap_or(0).to_le_bytes());
}

/// Appends a record: length, type and body, then the CRC-32 of type and body.
pub fn append(out: &mut Vec<u8>, r: &Record) {
    let start = out.len();
    out.extend_from_slice(&[0; 4]);
    match r {
        Record::Publish { topic, qos, msg } => {
            out.push(PUBLISH);
            put_bytes(out, topic.as_bytes());
            out.push(*qos);
            put_bytes(out, &msg.packet);
            put_bytes(out, &msg.props);
            put_opt_u64(out, msg.expires);
            out.push(msg.retained as u8);
        }
        Record::Retain(x) => {
            out.push(RETAIN);
            put_bytes(out, x.topic.as_bytes());
            put_bytes(out, &x.payload);
            out.push(x.qos);
            put_bytes(out, &x.props);
            put_opt_u64(out, x.expires);
        }
        Record::Session { client_id, expiry, filters } => {
            out.push(SESSION);
            put_bytes(out, client_id.as_bytes());
            put_u32(out, *expiry);
            put_u16(out, filters.len() as u16);
            for (f, o) in filters {
                put_bytes(out, f.as_bytes());
                out.push(*o);
            }
        }
        Record::SessionEnd { client_id } => {
            out.push(SESSION_END);
            put_bytes(out, client_id.as_bytes());
        }
    }
    let len = (out.len() - start - 4) as u32;
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    let crc = crc32(&out[start + 4..]);
    put_u32(out, crc);
}

/// A publish record from the parts the connection tier has at hand (no `Msg` to build).
pub fn append_publish(
    out: &mut Vec<u8>,
    topic: &str,
    qos: u8,
    packet: &[u8],
    props: &[u8],
    expires: Option<u64>,
    retained: bool,
) {
    let start = out.len();
    out.extend_from_slice(&[0; 4]);
    out.push(PUBLISH);
    put_bytes(out, topic.as_bytes());
    out.push(qos);
    put_bytes(out, packet);
    put_bytes(out, props);
    put_opt_u64(out, expires);
    out.push(retained as u8);
    let len = (out.len() - start - 4) as u32;
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    let crc = crc32(&out[start + 4..]);
    put_u32(out, crc);
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.0.len() < n {
            return None;
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Some(a)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn bytes(&mut self) -> Option<Vec<u8>> {
        let n = self.u32()? as usize;
        Some(self.take(n)?.to_vec())
    }
    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.bytes()?).ok()
    }
    fn opt_u64(&mut self) -> Option<Option<u64>> {
        let some = self.u8()? != 0;
        let v = u64::from_le_bytes(self.take(8)?.try_into().ok()?);
        Some(some.then_some(v))
    }
}

fn decode(body: &[u8]) -> Option<Record> {
    let mut r = Reader(body);
    let record = match r.u8()? {
        PUBLISH => {
            let topic = r.string()?;
            let qos = r.u8()?;
            let msg = Msg { packet: r.bytes()?, props: r.bytes()?, expires: r.opt_u64()?, retained: r.u8()? != 0 };
            Record::Publish { topic, qos, msg }
        }
        RETAIN => Record::Retain(SavedRetained {
            topic: r.string()?,
            payload: r.bytes()?,
            qos: r.u8()?,
            props: r.bytes()?,
            expires: r.opt_u64()?,
        }),
        SESSION => {
            let client_id = r.string()?;
            let expiry = r.u32()?;
            let n = r.u16()?;
            let mut filters = Vec::new();
            for _ in 0..n {
                filters.push((r.string()?, r.u8()?));
            }
            Record::Session { client_id, expiry, filters }
        }
        SESSION_END => Record::SessionEnd { client_id: r.string()? },
        _ => return None,
    };
    r.0.is_empty().then_some(record)
}

/// The records of a segment, up to the first incomplete or corrupt one (a write cut by a
/// crash).
pub fn read(data: &[u8]) -> Vec<Record> {
    let mut out = Vec::new();
    let mut at = 0;
    while data.len() >= at + 8 {
        let len = u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as usize;
        if data.len() < at + 8 + len {
            break;
        }
        let body = &data[at + 4..at + 4 + len];
        let crc = u32::from_le_bytes(data[at + 4 + len..at + 8 + len].try_into().unwrap());
        let Some(record) = (crc == crc32(body)).then(|| decode(body)).flatten() else { break };
        out.push(record);
        at += 8 + len;
    }
    out
}

/// Applies records to restored state: retained changes, session changes, and messages to the
/// queues of the matching persistent sessions (offline after a restart).
///
/// The segments of different cores are not ordered with respect to each other, so session
/// records go first: a message reaches every session it may have been meant for (at least once),
/// whichever core logged the session.
pub fn replay(saved: &mut Saved, records: impl IntoIterator<Item = Record>) {
    let (sessions, others): (Vec<Record>, Vec<Record>) =
        records.into_iter().partition(|r| matches!(r, Record::Session { .. } | Record::SessionEnd { .. }));
    apply(saved, sessions.into_iter().chain(others));
}

fn apply(saved: &mut Saved, records: impl IntoIterator<Item = Record>) {
    let mut retained: HashMap<String, SavedRetained> =
        std::mem::take(&mut saved.retained).into_iter().map(|r| (r.topic.clone(), r)).collect();
    let mut sessions: HashMap<String, SavedSession> =
        std::mem::take(&mut saved.sessions).into_iter().map(|s| (s.client_id.clone(), s)).collect();
    for record in records {
        match record {
            Record::Retain(r) if r.payload.is_empty() => {
                retained.remove(&r.topic);
            }
            Record::Retain(r) => {
                retained.insert(r.topic.clone(), r);
            }
            Record::Session { client_id, expiry, filters } => {
                let s = sessions
                    .entry(client_id.clone())
                    .or_insert_with(|| SavedSession { client_id, ..SavedSession::default() });
                s.expiry = expiry;
                s.filters = filters;
            }
            Record::SessionEnd { client_id } => {
                sessions.remove(&client_id);
            }
            Record::Publish { topic, qos, msg } => {
                for s in sessions.values_mut() {
                    let options = s
                        .filters
                        .iter()
                        .filter(|(f, _)| codec::matches(crate::index::shared(f).map_or(f, |(_, f)| f), &topic))
                        .map(|(_, o)| *o)
                        .max_by_key(|o| o & 3);
                    if let Some(o) = options {
                        let q = (o & 3).min(qos);
                        if q > 0 {
                            s.queue.push(((o & !3) | q, msg.clone()));
                        }
                    }
                }
            }
        }
    }
    saved.retained = retained.into_values().collect();
    saved.sessions = sessions.into_values().collect();
}

// ---- CRC-32 (IEEE) ---------------------------------------------------------------------------

const fn crc_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

const CRC: [u32; 256] = crc_table();

pub fn crc32(data: &[u8]) -> u32 {
    !data.iter().fold(!0u32, |c, &b| CRC[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_known_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn records_roundtrip_and_torn_tail() {
        let msg = Msg { packet: vec![0x30, 3, 0, 1, b'a'], props: vec![], expires: Some(5), retained: false };
        let records = vec![
            Record::Session { client_id: "d".into(), expiry: 60, filters: vec![("a".into(), 1)] },
            Record::Publish { topic: "a".into(), qos: 1, msg: msg.clone() },
            Record::Retain(SavedRetained { topic: "r".into(), payload: b"v".to_vec(), qos: 0, ..Default::default() }),
            Record::SessionEnd { client_id: "x".into() },
        ];
        let mut b = Vec::new();
        for r in &records {
            append(&mut b, r);
        }
        assert_eq!(read(&b), records);
        // A cut write: the records before it are kept.
        assert_eq!(read(&b[..b.len() - 3]).len(), 3);
        let mut flipped = b.clone();
        let last = flipped.len() - 6;
        flipped[last] ^= 1;
        assert_eq!(read(&flipped).len(), 3, "CRC catches the corruption");
        let mut p = Vec::new();
        append_publish(&mut p, "a", 1, &msg.packet, &msg.props, msg.expires, false);
        assert_eq!(read(&p), vec![Record::Publish { topic: "a".into(), qos: 1, msg }]);
    }

    #[test]
    fn replay_applies_sessions_before_messages() {
        let mut saved = Saved::default();
        let msg = Msg { packet: b"m".to_vec(), ..Msg::default() };
        // The publisher's core segment comes before the subscriber's.
        replay(
            &mut saved,
            vec![
                Record::Publish { topic: "t".into(), qos: 1, msg: msg.clone() },
                Record::Session { client_id: "s".into(), expiry: u32::MAX, filters: vec![("t".into(), 1)] },
            ],
        );
        assert_eq!(saved.sessions[0].queue, vec![(1, msg)]);
    }

    #[test]
    fn replay_fills_matching_session_queues() {
        let mut saved = Saved::default();
        let msg = |p: &str| Msg { packet: p.as_bytes().to_vec(), ..Msg::default() };
        replay(
            &mut saved,
            vec![
                Record::Session { client_id: "a".into(), expiry: u32::MAX, filters: vec![("t/#".into(), 1)] },
                Record::Session { client_id: "b".into(), expiry: 60, filters: vec![("t/x".into(), 0)] },
                Record::Publish { topic: "t/x".into(), qos: 1, msg: msg("1") },
                Record::Publish { topic: "u".into(), qos: 1, msg: msg("2") },
                Record::Retain(SavedRetained { topic: "r".into(), payload: b"v".to_vec(), ..Default::default() }),
                Record::Retain(SavedRetained { topic: "r".into(), payload: vec![], ..Default::default() }),
                Record::SessionEnd { client_id: "gone".into() },
            ],
        );
        assert!(saved.retained.is_empty());
        let a = saved.sessions.iter().find(|s| s.client_id == "a").unwrap();
        assert_eq!(a.queue, vec![(1, msg("1"))]);
        let b = saved.sessions.iter().find(|s| s.client_id == "b").unwrap();
        assert!(b.queue.is_empty(), "QoS 0 subscription: not queued");
    }
}
