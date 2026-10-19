//! The broker: three tiers of actors on every core (colocated, router replicated).
//!
//! - Tier 1, [`Connections`]: owns the core's listening socket (SO_REUSEPORT) and every
//!   connection accepted on it, drives them through the core's io_uring, parses MQTT.
//! - Tier 2, [`Router`]: a full replica of the subscription index on every core. The connection
//!   tier calls it synchronously through an `ActorRef` (same core, no event, no hop); SUBSCRIBE
//!   and UNSUBSCRIBE are applied locally and broadcast to the other cores' replicas.
//! - Tier 3, [`Contexts`]: retained messages and the client-id registry, partitioned by hash
//!   over the cores; reached by events.
//!
//! A PUBLISH is matched on the core that received it, encoded once, appended to the output
//! buffer of local subscribers and sent as one `Deliver` event per other core that has
//! subscribers. Output buffers are flushed at the end of the loop iteration (one `send` per
//! connection with pending data, all submitted to io_uring together).
//!
//! PoC scope: MQTT 3.1.1; subscriptions are granted QoS 0 (allowed by the protocol), so every
//! delivery is QoS 0; QoS 1 publishes are acknowledged once routed (no persistence); clean
//! sessions only; keep-alive is not enforced.

use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Duration;

use cannes::{Actor, ActorId, ActorRef, Context, CoreId, CoreMode, Engine, Handler, IoCompletion, StartSequence};

use crate::auth::{self, Auth, Rule};
use crate::codec::{self, Packet};
use crate::index::{Entry, Sub, SubIndex};
use crate::net;

const ROUTER: u32 = 0;
const CONTEXT: u32 = 1;
const CONNECTIONS: u32 = 2;

const RECV_BUF: usize = 1024;

fn actor(core: CoreId, index: u32) -> ActorId {
    ActorId { core: core as u16, generation: 0, index }
}

fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ b as u64).wrapping_mul(0x100_0000_01b3))
}

/// The broker's cores, shared by every actor.
struct Topology {
    cores: Vec<CoreId>,
}

impl Topology {
    fn partition(&self, key: &str) -> CoreId {
        self.cores[(fnv(key) % self.cores.len() as u64) as usize]
    }
}

// ---- Events between cores ------------------------------------------------------------------

struct SubAdd {
    filter: Box<str>,
    sub: Sub,
    qos: u8,
}

struct SubRemove {
    filter: Box<str>,
    sub: Sub,
}

/// A message for connections of the receiving core: its QoS 0 encoding, and each connection with
/// the QoS it gets (min of the message's and the subscription's).
struct Deliver {
    packet: Vec<u8>,
    conns: Vec<(u64, u8)>,
}

struct Retain {
    topic: Box<str>,
    payload: Vec<u8>,
    qos: u8,
}

struct RetainedQuery {
    filter: Box<str>,
    conn: u64,
    qos: u8,
}

/// connection -> context: a CONNECT with a client id. Answered by `Opened`.
struct Open {
    client_id: Box<str>,
    sub: Sub,
    clean: bool,
}

/// context -> connection: the session is the connection's. `present`: a persistent session
/// existed, its subscriptions and undelivered messages come with it.
struct Opened {
    client_id: Box<str>,
    conn: u64,
    sid: u32,
    present: bool,
    filters: Vec<(Box<str>, u8)>,
    state: Undelivered,
}

/// QoS 1 and 2 state of a session: messages sent and not acknowledged (packet id and encoding,
/// redelivered with DUP; an empty encoding is a QoS 2 message at the PUBREL step), messages not
/// sent yet (QoS and QoS 0 encoding), and the ids of QoS 2 messages received from the client and
/// not released yet.
#[derive(Default)]
struct Undelivered {
    inflight: Vec<(u16, Vec<u8>)>,
    queue: VecDeque<(u8, Vec<u8>)>,
    received: Vec<u16>,
}

/// connection -> context: a clean-session connection with a client id closed.
struct Unregister {
    client_id: Box<str>,
    sub: Sub,
}

/// connection -> context: a persistent-session connection closed (or could not take its session),
/// handing back the session's state. `filters`: its current subscriptions, if it had the session.
struct Park {
    client_id: Box<str>,
    sub: Sub,
    filters: Option<Vec<(Box<str>, u8)>>,
    state: Undelivered,
}

/// Counters of a core's connection tier, sent every `sys_interval` to the first core, which
/// publishes the totals under `$SYS/broker/`.
#[derive(Clone, Copy, Default)]
struct Stats {
    round: u32,
    connections: u64,
    received: u64,
    delivered: u64,
    dropped: u64,
    bytes_in: u64,
    bytes_out: u64,
    retained: u64,
    sessions: u64,
}

/// Close a connection: its client id was taken over by a new connection.
struct Kick {
    conn: u64,
}

// ---- Tier 2: router ----------------------------------------------------------------------------

pub struct Router {
    index: SubIndex,
}

impl Actor for Router {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<SubAdd, Self>();
        ctx.register_handler::<SubRemove, Self>();
    }
}

impl Handler<SubAdd> for Router {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, e: &SubAdd) {
        self.index.add(&e.filter, e.sub, e.qos);
    }
}

impl Handler<SubRemove> for Router {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, e: &SubRemove) {
        self.index.remove(&e.filter, e.sub);
    }
}

// ---- Tier 3: retained messages and client-id registry ------------------------------------------

pub struct Contexts {
    topology: Arc<Topology>,
    retained: HashMap<Box<str>, (Vec<u8>, u8)>,
    by_client: HashMap<Box<str>, u32>,
    sessions: HashMap<u32, Session>,
    next_sid: u32,
    max_queued: usize,
}

/// A client id's session, on the context partition of the client id.
struct Session {
    client_id: Box<str>,
    persistent: bool,
    state: SessionState,
    /// Subscriptions, kept while the session is offline (persistent sessions).
    filters: Vec<(Box<str>, u8)>,
    undelivered: Undelivered,
    queued_bytes: usize,
}

enum SessionState {
    Online(Sub),
    /// Persistent session without connection: its subscriptions route to it (session targets).
    Offline,
    /// `old` was kicked for `new`, which waits for `old` to hand the session back.
    TakingOver {
        old: Sub,
        new: Sub,
        clean: bool,
    },
}

/// Session targets: index entries `(context core, SESSION | sid)` route messages for an offline
/// persistent session to its context partition.
const SESSION: u64 = 1 << 60;

impl Actor for Contexts {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Retain, Self>();
        ctx.register_handler::<RetainedQuery, Self>();
        ctx.register_handler::<Open, Self>();
        ctx.register_handler::<Unregister, Self>();
        ctx.register_handler::<Park, Self>();
    }
}

impl Contexts {
    /// Gives session `sid` to connection `sub` (fresh session if `clean` or none): replies
    /// `Opened`, with the state of a persistent session left offline.
    fn open(&mut self, ctx: &mut Context, client_id: &str, sub: Sub, clean: bool) {
        let existing = self.by_client.get(client_id).copied();
        if let Some(sid) = existing {
            let s = self.sessions.get_mut(&sid).unwrap();
            match s.state {
                SessionState::Online(old) if old != sub => {
                    // Takeover: the old connection hands the session back when it closes.
                    s.state = SessionState::TakingOver { old, new: sub, clean };
                    ctx.send(actor(old.0 as CoreId, CONNECTIONS), Kick { conn: old.1 });
                    return;
                }
                SessionState::TakingOver { old, new, .. } => {
                    // A third connection: it replaces the one waiting.
                    s.state = SessionState::TakingOver { old, new: sub, clean };
                    ctx.send(actor(new.0 as CoreId, CONNECTIONS), Kick { conn: new.1 });
                    return;
                }
                SessionState::Offline if !clean => {
                    s.state = SessionState::Online(sub);
                    s.queued_bytes = 0;
                    let state = std::mem::take(&mut s.undelivered);
                    let filters = s.filters.clone();
                    ctx.send(
                        actor(sub.0 as CoreId, CONNECTIONS),
                        Opened { client_id: client_id.into(), conn: sub.1, sid, present: true, filters, state },
                    );
                    return;
                }
                _ => self.discard(ctx, sid),
            }
        }
        let sid = self.next_sid;
        self.next_sid = self.next_sid.wrapping_add(1);
        self.by_client.insert(client_id.into(), sid);
        self.sessions.insert(
            sid,
            Session {
                client_id: client_id.into(),
                persistent: !clean,
                state: SessionState::Online(sub),
                filters: Vec::new(),
                undelivered: Undelivered::default(),
                queued_bytes: 0,
            },
        );
        let state = Undelivered::default();
        ctx.send(
            actor(sub.0 as CoreId, CONNECTIONS),
            Opened { client_id: client_id.into(), conn: sub.1, sid, present: false, filters: Vec::new(), state },
        );
    }

    /// Deletes a session, and its session targets if it was offline.
    fn discard(&mut self, ctx: &mut Context, sid: u32) {
        let Some(s) = self.sessions.remove(&sid) else { return };
        self.by_client.remove(&s.client_id);
        if matches!(s.state, SessionState::Offline) {
            let target = (ctx.core() as u16, SESSION | sid as u64);
            for (filter, _) in &s.filters {
                for &core in &self.topology.cores {
                    ctx.send(actor(core, ROUTER), SubRemove { filter: filter.clone(), sub: target });
                }
            }
        }
    }

    /// The connection `sub` let go of the session of `client_id`, with `state` if persistent.
    fn released(&mut self, ctx: &mut Context, client_id: &str, sub: Sub, park: Option<&Park>) {
        let Some(&sid) = self.by_client.get(client_id) else { return };
        let s = self.sessions.get_mut(&sid).unwrap();
        let (owner, next) = match s.state {
            SessionState::Online(o) => (o, None),
            SessionState::TakingOver { old, new, clean } => (old, Some((new, clean))),
            // A connection closed before it got its session: the session comes back from it.
            SessionState::Offline if park.is_some() => (sub, None),
            SessionState::Offline => return,
        };
        if owner != sub {
            return; // stale
        }
        match park {
            Some(p) if s.persistent => {
                if let Some(filters) = &p.filters {
                    s.filters = filters.clone();
                }
                // What comes back is older than what was queued meanwhile.
                let mut inflight = p.state.inflight.clone();
                inflight.append(&mut s.undelivered.inflight);
                let mut queue = p.state.queue.clone();
                queue.append(&mut s.undelivered.queue);
                s.queued_bytes = queue.iter().map(|q| q.1.len()).sum();
                let received = if p.state.received.is_empty() {
                    std::mem::take(&mut s.undelivered.received)
                } else {
                    p.state.received.clone()
                };
                s.undelivered = Undelivered { inflight, queue, received };
                s.state = SessionState::Offline;
            }
            _ => {
                self.sessions.remove(&sid);
                self.by_client.remove(client_id);
            }
        }
        if let Some((new, clean)) = next {
            self.open(ctx, client_id, new, clean);
        }
    }

    /// A message for session target `sid` (synchronous call from the connection tier of this
    /// core): forwarded to the session's connection, or queued (QoS 1) while it is offline.
    fn session_message(&mut self, ctx: &mut Context, sid: u32, packet: &[u8], qos: u8) {
        let Some(s) = self.sessions.get_mut(&sid) else { return };
        match s.state {
            SessionState::Online(sub) => {
                ctx.send(
                    actor(sub.0 as CoreId, CONNECTIONS),
                    Deliver { packet: packet.to_vec(), conns: vec![(sub.1, qos)] },
                );
            }
            _ if qos > 0 && s.queued_bytes < self.max_queued => {
                s.queued_bytes += packet.len();
                s.undelivered.queue.push_back((qos, packet.to_vec()));
            }
            _ => {} // QoS 0 messages are not kept for offline sessions
        }
    }
}

impl Handler<Retain> for Contexts {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, e: &Retain) {
        if e.payload.is_empty() {
            self.retained.remove(&e.topic);
        } else {
            self.retained.insert(e.topic.clone(), (e.payload.clone(), e.qos));
        }
    }
}

impl Handler<RetainedQuery> for Contexts {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, q: &RetainedQuery) {
        for (topic, (payload, qos)) in &self.retained {
            if codec::matches(&q.filter, topic) {
                let mut packet = Vec::new();
                codec::publish(&mut packet, topic, payload, 0, true, None);
                ctx.send(src, Deliver { packet, conns: vec![(q.conn, (*qos).min(q.qos))] });
            }
        }
    }
}

impl Handler<Open> for Contexts {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, o: &Open) {
        self.open(ctx, &o.client_id, o.sub, o.clean);
    }
}

impl Handler<Unregister> for Contexts {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, u: &Unregister) {
        self.released(ctx, &u.client_id, u.sub, None);
    }
}

impl Handler<Park> for Contexts {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, p: &Park) {
        self.released(ctx, &p.client_id, p.sub, Some(p));
    }
}

// ---- Tier 1: connections -----------------------------------------------------------------------

const ACCEPT: u64 = 0;
const RECV: u64 = 1;
const SEND: u64 = 2;
const CLOSE: u64 = 3;
const TIMER: u64 = 4;

/// Period of the connection tier's sweep (keep-alive and CONNECT timeouts).
const TICK: Duration = Duration::from_secs(1);

fn token(kind: u64, conn: u64) -> u64 {
    kind << 56 | conn
}

struct OwnedWill {
    topic: Box<str>,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
}

/// QoS 1 and 2 state of a connection, allocated with its first QoS 1 or 2 message: messages sent
/// and waiting for PUBACK / PUBREC / PUBCOMP, messages waiting for room in the in-flight window,
/// and QoS 2 messages received and not released.
#[derive(Default)]
struct Outbound {
    last_id: u16,
    /// Packet id and encoding, kept for redelivery; empty once a QoS 2 message is at PUBREL.
    inflight: VecDeque<(u16, Vec<u8>)>,
    /// QoS and QoS 0 encoding, given an id when they enter the window.
    pending: VecDeque<(u8, Vec<u8>)>,
    pending_bytes: usize,
    /// Ids of QoS 2 messages received from the client, until its PUBREL.
    received: Vec<u16>,
}

impl Outbound {
    fn next_id(&mut self) -> u16 {
        loop {
            self.last_id = self.last_id.wrapping_add(1).max(1);
            if !self.inflight.iter().any(|(id, _)| *id == self.last_id) {
                return self.last_id;
            }
        }
    }

    /// Moves `packet` into the window: encoded at `qos` with a fresh id at the end of `out`.
    fn send(&mut self, out: &mut Vec<u8>, packet: &[u8], qos: u8) {
        let id = self.next_id();
        let start = out.len();
        codec::requalify(out, packet, qos, id);
        self.inflight.push_back((id, out[start..].to_vec()));
    }
}

/// What `handle` asks of the connection after a packet.
enum Next {
    Continue,
    /// Close now.
    Close,
    /// Stop reading, send what is queued (a CONNACK refusing the connection), then close.
    CloseAfterFlush,
    /// Stop reading until the session is opened (`Opened`).
    WaitSession,
}

struct Conn {
    fd: RawFd,
    /// CONNECT accepted.
    connected: bool,
    keep_alive: u16,
    /// Tick of the last bytes received (or of the accept before that).
    last_rx: u32,
    will: Option<Box<OwnedWill>>,
    close_after_flush: bool,
    outbound: Option<Box<Outbound>>,
    /// CONNECT asked for a persistent session.
    persistent: bool,
    /// Session id, once `Opened`.
    session: Option<u32>,
    /// Bytes received after CONNECT, held until the session is opened.
    held: Option<Vec<u8>>,
    /// Access rules, when the broker has an ACL.
    rules: Option<Box<[Rule]>>,
    /// Bytes waiting to be sent. (The receive buffer is always owned by the recv in flight.)
    out: Vec<u8>,
    sending: bool,
    dirty: bool,
    filters: Vec<(Box<str>, u8)>,
    client_id: Option<Box<str>>,
}

pub struct Connections {
    topology: Arc<Topology>,
    listener: RawFd,
    timer: RawFd,
    tick: u32,
    connect_timeout: u32,
    max_packet: usize,
    max_queued: usize,
    max_inflight: usize,
    /// Messages dropped because the subscriber's output queue was full.
    pub dropped: u64,
    /// PUBLISH packets received from clients.
    pub received_msgs: u64,
    /// Messages queued to clients.
    pub delivered: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// `$SYS` period in ticks (0: none), start time, and on the first core the last stats of
    /// each core.
    sys_interval: u32,
    started: std::time::Instant,
    stats: Vec<Option<Stats>>,
    router: Option<ActorRef<Router>>,
    contexts: Option<ActorRef<Contexts>>,
    auth: Option<Arc<Auth>>,
    /// Persistent-session connections closed in the last ticks: (context core, session id,
    /// tick). Messages routed to them before every router saw them go are sent on to the
    /// session.
    forwards: HashMap<u64, (CoreId, u32, u32)>,
    conns: Vec<Option<Conn>>,
    generations: Vec<u32>,
    free: Vec<u32>,
    dirty: Vec<u64>,
    callback_armed: bool,
    // Scratch buffers reused across publishes.
    packet: Vec<u8>,
    targets: Vec<Entry>,
    /// Connections accepted and currently open on this core.
    pub open: usize,
}

impl Connections {
    fn id(&self, index: u32) -> u64 {
        (self.generations[index as usize] as u64 & 0xff_ffff) << 32 | index as u64
    }

    fn get(&mut self, id: u64) -> Option<&mut Conn> {
        let index = (id & 0xffff_ffff) as usize;
        if index < self.conns.len() && self.id(index as u32) == id & 0x00ff_ffff_ffff_ffff {
            self.conns[index].as_mut()
        } else {
            None
        }
    }

    fn core(&self, ctx: &Context) -> u16 {
        ctx.core() as u16
    }

    fn mark_dirty(&mut self, ctx: &mut Context, id: u64) {
        if let Some(c) = self.get(id) {
            if !c.dirty {
                c.dirty = true;
                self.dirty.push(id);
            }
        }
        if !self.callback_armed {
            self.callback_armed = true;
            ctx.register_callback();
        }
    }

    fn accept(&mut self, ctx: &mut Context, fd: RawFd) {
        let index = self.free.pop().unwrap_or_else(|| {
            self.conns.push(None);
            self.generations.push(0);
            self.conns.len() as u32 - 1
        });
        self.conns[index as usize] = Some(Conn {
            fd,
            connected: false,
            keep_alive: 0,
            last_rx: self.tick,
            will: None,
            close_after_flush: false,
            outbound: None,
            persistent: false,
            session: None,
            held: None,
            rules: None,
            out: Vec::new(),
            sending: false,
            dirty: false,
            filters: Vec::new(),
            client_id: None,
        });
        self.open += 1;
        let id = self.id(index);
        ctx.io().unwrap().recv(fd, Vec::with_capacity(RECV_BUF), token(RECV, id));
    }

    /// Adds a subscription to this core's index and broadcasts it to the other cores.
    fn add_sub(&mut self, ctx: &mut Context, filter: &str, sub: Sub, qos: u8) {
        self.router.as_ref().unwrap().borrow_mut().index.add(filter, sub, qos);
        for &core in &self.topology.cores {
            if core != ctx.core() {
                ctx.send(actor(core, ROUTER), SubAdd { filter: filter.into(), sub, qos });
            }
        }
    }

    fn remove_sub(&mut self, ctx: &mut Context, filter: &str, sub: Sub) {
        self.router.as_ref().unwrap().borrow_mut().index.remove(filter, sub);
        for &core in &self.topology.cores {
            if core != ctx.core() {
                ctx.send(actor(core, ROUTER), SubRemove { filter: filter.into(), sub });
            }
        }
    }

    /// Closes connection `id`, publishing its will if it still has one (DISCONNECT clears it).
    fn close(&mut self, ctx: &mut Context, id: u64) {
        let index = (id & 0xffff_ffff) as usize;
        let Some(conn) = self.get(id).map(|_| ()).and_then(|_| self.conns[index].take()) else { return };
        self.generations[index] = self.generations[index].wrapping_add(1);
        self.free.push(index as u32);
        self.open -= 1;
        let sub = (self.core(ctx), id);
        let parked = match (conn.persistent, conn.session, &conn.client_id) {
            (true, Some(sid), Some(client_id)) => Some((self.topology.partition(client_id), sid)),
            _ => None,
        };
        if let Some((partition, sid)) = parked {
            self.forwards.insert(id, (partition, sid, self.tick));
        }
        for (filter, qos) in &conn.filters {
            if let Some((partition, sid)) = parked {
                // The session's subscriptions now route to the session, added before the
                // connection's are removed so that no message falls in between.
                self.add_sub(ctx, filter, (partition as u16, SESSION | sid as u64), *qos);
            }
            self.remove_sub(ctx, filter, sub);
        }
        if let Some(will) = conn.will {
            if will.retain {
                let partition = self.topology.partition(&will.topic);
                ctx.send(
                    actor(partition, CONTEXT),
                    Retain { topic: will.topic.clone(), payload: will.payload.clone(), qos: will.qos },
                );
            }
            self.publish(ctx, &will.topic, &will.payload, will.qos);
        }
        if let Some(client_id) = conn.client_id {
            let partition = self.topology.partition(&client_id);
            if conn.persistent {
                let mut state = Undelivered::default();
                if let Some(ob) = conn.outbound {
                    state.inflight = ob.inflight.into();
                    state.queue = ob.pending;
                    state.received = ob.received;
                }
                let filters = conn.session.map(|_| conn.filters);
                ctx.send(actor(partition, CONTEXT), Park { client_id, sub, filters, state });
            } else {
                ctx.send(actor(partition, CONTEXT), Unregister { client_id, sub });
            }
        }
        net::shutdown(conn.fd); // ends the pending recv and sends FIN
        ctx.io().unwrap().close(conn.fd, token(CLOSE, id));
    }

    fn received(&mut self, ctx: &mut Context, id: u64, result: i32, mut buf: Vec<u8>) {
        if result <= 0 {
            self.close(ctx, id);
            return;
        }
        self.bytes_in += result as u64;
        let mut consumed = 0;
        loop {
            match codec::parse_limited(&buf[consumed..], self.max_packet) {
                Ok(Some((packet, len))) => {
                    consumed += len;
                    match self.handle(ctx, id, packet) {
                        Next::Continue => {}
                        Next::Close => {
                            self.close(ctx, id);
                            return;
                        }
                        Next::WaitSession => {
                            buf.drain(..consumed);
                            self.get(id).unwrap().held = Some(buf);
                            return;
                        }
                        Next::CloseAfterFlush => {
                            // No more reads; `sent` closes once the output is out.
                            self.get(id).unwrap().close_after_flush = true;
                            self.mark_dirty(ctx, id);
                            return;
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    self.close(ctx, id);
                    return;
                }
            }
        }
        buf.drain(..consumed);
        // Room for the rest of a large packet in one go, else for the next small ones.
        let want = codec::packet_len(&buf).map_or(RECV_BUF, |n| n.saturating_sub(buf.len()).max(RECV_BUF / 2));
        if buf.capacity() - buf.len() < want {
            buf.reserve(want.max(RECV_BUF));
        }
        let tick = self.tick;
        let Some(conn) = self.get(id) else { return };
        conn.last_rx = tick;
        let fd = conn.fd;
        ctx.io().unwrap().recv(fd, buf, token(RECV, id));
    }

    /// Once per `TICK`: closes connections silent for 1.5 keep-alive periods, and connections
    /// that did not send CONNECT in time.
    fn sweep(&mut self, ctx: &mut Context) {
        self.tick += 1;
        let tick = self.tick;
        let connect_timeout = self.connect_timeout;
        let expired: Vec<u64> = (0..self.conns.len() as u32)
            .filter(|&i| {
                let Some(c) = &self.conns[i as usize] else { return false };
                let limit = match (c.connected, c.keep_alive) {
                    (true, 0) => return false,
                    (true, ka) => (ka as u32 * 3).div_ceil(2),
                    (false, _) => connect_timeout,
                };
                tick.wrapping_sub(c.last_rx) > limit
            })
            .map(|i| self.id(i))
            .collect();
        for id in expired {
            self.close(ctx, id);
        }
        self.forwards.retain(|_, f| tick.wrapping_sub(f.2) <= 2);
        if self.sys_interval > 0 && tick % self.sys_interval == 0 {
            let (retained, sessions) = {
                let c = self.contexts.as_ref().unwrap().borrow();
                (c.retained.len() as u64, c.sessions.len() as u64)
            };
            let stats = Stats {
                round: tick / self.sys_interval,
                connections: self.open as u64,
                received: self.received_msgs,
                delivered: self.delivered,
                dropped: self.dropped,
                bytes_in: self.bytes_in,
                bytes_out: self.bytes_out,
                retained,
                sessions,
            };
            ctx.send(actor(self.topology.cores[0], CONNECTIONS), stats);
        }
    }

    /// Handles one packet from connection `id`.
    fn handle(&mut self, ctx: &mut Context, id: u64, packet: Packet<'_>) -> Next {
        let core = self.core(ctx);
        let auth = if matches!(packet, Packet::Connect(_)) { self.auth.clone() } else { None };
        let conn = self.get(id).unwrap();
        if conn.connected == matches!(packet, Packet::Connect(_)) {
            return Next::Close; // anything before CONNECT, or a second CONNECT
        }
        match packet {
            Packet::Connect(c) => {
                let code = if c.level != 4 && c.level != 3 {
                    1 // unacceptable protocol version
                } else if c.client_id.is_empty() && !c.clean_session {
                    2 // identifier rejected
                } else if let Some(a) = &auth {
                    a.check(c.username, c.password)
                } else {
                    0
                };
                if code != 0 {
                    codec::connack(&mut conn.out, false, code);
                    return Next::CloseAfterFlush;
                }
                conn.connected = true;
                conn.keep_alive = c.keep_alive;
                conn.rules = auth.and_then(|a| a.rules(c.username, c.client_id)).map(Vec::into_boxed_slice);
                let will_allowed = |topic: &str| conn.rules.as_ref().is_none_or(|r| auth::can_write(r, topic));
                let will = c.will.filter(|w| will_allowed(w.topic));
                conn.will = will.map(|w| {
                    Box::new(OwnedWill {
                        topic: w.topic.into(),
                        payload: w.payload.to_vec(),
                        qos: w.qos,
                        retain: w.retain,
                    })
                });
                conn.persistent = !c.clean_session;
                if c.client_id.is_empty() {
                    codec::connack(&mut conn.out, false, 0);
                    self.mark_dirty(ctx, id);
                } else {
                    // CONNACK once the context partition of the client id has the session.
                    conn.client_id = Some(c.client_id.into());
                    let partition = self.topology.partition(c.client_id);
                    let open = Open { client_id: c.client_id.into(), sub: (core, id), clean: c.clean_session };
                    ctx.send(actor(partition, CONTEXT), open);
                    return Next::WaitSession;
                }
            }
            Packet::Publish { topic, payload, qos, retain, packet_id, .. } => {
                // A publish the ACL denies is acknowledged as usual and not routed.
                let allowed = conn.rules.as_ref().is_none_or(|r| auth::can_write(r, topic));
                if qos == 2 {
                    // Routed once: a copy with the same id before PUBREL is a redelivery.
                    let pid = packet_id.unwrap_or(0);
                    let conn = self.get(id).unwrap();
                    let ob = conn.outbound.get_or_insert_with(Default::default);
                    let again = ob.received.contains(&pid);
                    if !again {
                        ob.received.push(pid);
                    }
                    codec::pubrec(&mut conn.out, pid);
                    self.mark_dirty(ctx, id);
                    if again || !allowed {
                        return Next::Continue;
                    }
                }
                if !allowed {
                    if qos == 1 {
                        codec::puback(&mut self.get(id).unwrap().out, packet_id.unwrap_or(0));
                        self.mark_dirty(ctx, id);
                    }
                    return Next::Continue;
                }
                self.received_msgs += 1;
                if retain {
                    let partition = self.topology.partition(topic);
                    ctx.send(actor(partition, CONTEXT), Retain { topic: topic.into(), payload: payload.to_vec(), qos });
                }
                self.publish(ctx, topic, payload, qos);
                if qos == 1 {
                    codec::puback(&mut self.get(id).unwrap().out, packet_id.unwrap_or(0));
                    self.mark_dirty(ctx, id);
                }
            }
            Packet::Subscribe { packet_id, filters } => {
                let sub = (core, id);
                let rules = conn.rules.as_deref();
                let granted: Vec<u8> = filters
                    .iter()
                    .map(|&(f, q)| if rules.is_none_or(|r| auth::can_read(r, f)) { q.min(2) } else { 0x80 })
                    .collect();
                for (&(filter, _), &qos) in filters.iter().zip(&granted) {
                    if qos == 0x80 {
                        continue; // refused
                    }
                    self.add_sub(ctx, filter, sub, qos);
                    if crate::index::shared(filter).is_none() {
                        // Retained messages are not sent for shared subscriptions.
                        for &other in &self.topology.cores {
                            ctx.send(actor(other, CONTEXT), RetainedQuery { filter: filter.into(), conn: id, qos });
                        }
                    }
                }
                let conn = self.get(id).unwrap();
                for (&(f, _), &qos) in filters.iter().zip(&granted).filter(|(_, &q)| q != 0x80) {
                    match conn.filters.iter_mut().find(|g| &*g.0 == f) {
                        Some(g) => g.1 = qos,
                        None => conn.filters.push((f.into(), qos)),
                    }
                }
                codec::suback(&mut conn.out, packet_id, &granted);
                self.mark_dirty(ctx, id);
            }
            Packet::Unsubscribe { packet_id, filters } => {
                let sub = (core, id);
                for filter in &filters {
                    self.remove_sub(ctx, filter, sub);
                }
                let conn = self.get(id).unwrap();
                conn.filters.retain(|(f, _)| !filters.contains(&&**f));
                codec::unsuback(&mut conn.out, packet_id);
                self.mark_dirty(ctx, id);
            }
            Packet::PingReq => {
                codec::pingresp(&mut self.get(id).unwrap().out);
                self.mark_dirty(ctx, id);
            }
            Packet::PubAck { packet_id } | Packet::PubComp { packet_id } => self.acked(ctx, id, packet_id),
            Packet::PubRec { packet_id } => {
                // QoS 2 to the client, step 2: keep the id until PUBCOMP, the message is theirs.
                let conn = self.get(id).unwrap();
                if let Some(ob) = conn.outbound.as_mut() {
                    if let Some(e) = ob.inflight.iter_mut().find(|e| e.0 == packet_id) {
                        e.1 = Vec::new();
                    }
                }
                codec::pubrel(&mut conn.out, packet_id);
                self.mark_dirty(ctx, id);
            }
            Packet::PubRel { packet_id } => {
                // QoS 2 from the client, step 3: the message may come again with a new id.
                let conn = self.get(id).unwrap();
                if let Some(ob) = conn.outbound.as_mut() {
                    ob.received.retain(|&r| r != packet_id);
                }
                codec::pubcomp(&mut conn.out, packet_id);
                self.mark_dirty(ctx, id);
            }
            Packet::Disconnect => {
                self.get(id).unwrap().will = None; // a clean disconnect discards the will
                return Next::Close;
            }
            _ => return Next::Close,
        }
        Next::Continue
    }

    /// Routes a message: synchronous match on this core's router replica, local delivery, one
    /// `Deliver` event per other core with subscribers.
    fn publish(&mut self, ctx: &mut Context, topic: &str, payload: &[u8], qos: u8) {
        let mut targets = std::mem::take(&mut self.targets);
        targets.clear();
        self.router.as_ref().unwrap().borrow().index.for_each_match(topic, |s, q| targets.push((s, q.min(qos))));
        if targets.is_empty() {
            self.targets = targets;
            return;
        }
        // One delivery per connection, whatever the number of matching filters, at the highest
        // of their QoS.
        targets.sort_unstable_by_key(|e| e.0);
        targets.dedup_by(|a, b| {
            let same = a.0 == b.0;
            if same {
                b.1 = b.1.max(a.1);
            }
            same
        });
        let mut packet = std::mem::take(&mut self.packet);
        packet.clear();
        codec::publish(&mut packet, topic, payload, 0, false, None);
        let here = self.core(ctx);
        let mut i = 0;
        while i < targets.len() {
            let core = targets[i].0 .0;
            let j = targets[i..].iter().position(|e| e.0 .0 != core).map_or(targets.len(), |n| i + n);
            if core == here {
                for &((_, id), q) in &targets[i..j] {
                    self.deliver(ctx, id, &packet, q);
                }
            } else {
                let conns = targets[i..j].iter().map(|&((_, id), q)| (id, q)).collect();
                ctx.send(actor(core as CoreId, CONNECTIONS), Deliver { packet: packet.clone(), conns });
            }
            i = j;
        }
        self.packet = packet;
        self.targets = targets;
    }

    /// Queues the message encoded (QoS 0) in `packet` for connection `id` at QoS `qos`.
    ///
    /// QoS 0 messages are dropped if the connection already has `max_queued` bytes waiting (a
    /// subscriber slower than its publishers). QoS 1 messages enter the in-flight window, or
    /// wait for room in it, or are dropped once `max_queued` bytes wait.
    #[inline]
    fn deliver(&mut self, ctx: &mut Context, id: u64, packet: &[u8], qos: u8) {
        if id & SESSION != 0 {
            let sid = (id & !SESSION) as u32;
            self.contexts.as_ref().unwrap().borrow_mut().session_message(ctx, sid, packet, qos);
            return;
        }
        let (max_queued, max_inflight) = (self.max_queued, self.max_inflight);
        let Some(c) = self.get(id) else {
            if let Some(&(partition, sid, _)) = self.forwards.get(&id) {
                let target = SESSION | sid as u64;
                if partition == ctx.core() {
                    self.deliver(ctx, target, packet, qos);
                } else {
                    ctx.send(
                        actor(partition, CONNECTIONS),
                        Deliver { packet: packet.to_vec(), conns: vec![(target, qos)] },
                    );
                }
            }
            return;
        };
        // 0: dropped, 1: waits for room in the window, 2: queued for sending.
        let outcome = if qos == 0 {
            if c.out.len() >= max_queued {
                0
            } else {
                c.out.extend_from_slice(packet);
                2
            }
        } else {
            let ob = c.outbound.get_or_insert_with(Default::default);
            if ob.inflight.len() < max_inflight {
                ob.send(&mut c.out, packet, qos);
                2
            } else if ob.pending_bytes + c.out.len() < max_queued {
                ob.pending_bytes += packet.len();
                ob.pending.push_back((qos, packet.to_vec()));
                1
            } else {
                0
            }
        };
        match outcome {
            0 => self.dropped += 1,
            1 => self.delivered += 1,
            _ => {
                self.delivered += 1;
                self.mark_dirty(ctx, id);
            }
        }
    }

    /// PUBACK or PUBCOMP from connection `id`: frees a slot of its window for the next pending
    /// message.
    fn acked(&mut self, ctx: &mut Context, id: u64, packet_id: u16) {
        let max_inflight = self.max_inflight;
        let Some(c) = self.get(id) else { return };
        let Some(ob) = c.outbound.as_mut() else { return };
        let Some(at) = ob.inflight.iter().position(|(i, _)| *i == packet_id) else { return };
        ob.inflight.remove(at);
        let mut sent = false;
        while ob.inflight.len() < max_inflight {
            let Some((qos, next)) = ob.pending.pop_front() else { break };
            ob.pending_bytes -= next.len();
            ob.send(&mut c.out, &next, qos);
            sent = true;
        }
        if sent {
            self.mark_dirty(ctx, id);
        }
    }

    fn sent(&mut self, ctx: &mut Context, id: u64, result: i32, mut buf: Vec<u8>) {
        if result < 0 {
            self.close(ctx, id);
            return;
        }
        self.bytes_out += result as u64;
        let Some(conn) = self.get(id) else { return };
        buf.drain(..result as usize);
        let fd = conn.fd;
        if !buf.is_empty() {
            ctx.io().unwrap().send(fd, buf, token(SEND, id)); // partial send: the rest
            return;
        }
        conn.sending = false;
        if conn.out.is_empty() {
            if conn.close_after_flush {
                self.close(ctx, id);
                return;
            }
            conn.out = buf; // keep the capacity
        } else {
            let next = std::mem::replace(&mut conn.out, buf);
            conn.sending = true;
            ctx.io().unwrap().send(fd, next, token(SEND, id));
        }
    }
}

impl Actor for Connections {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<IoCompletion, Self>();
        ctx.register_handler::<Deliver, Self>();
        ctx.register_handler::<Kick, Self>();
        ctx.register_handler::<Opened, Self>();
        ctx.register_handler::<Stats, Self>();
        let router = actor(ctx.core(), ROUTER);
        self.router = Some(ctx.reference_local_actor::<Router>(router).expect("router on this core"));
        let contexts = actor(ctx.core(), CONTEXT);
        self.contexts = Some(ctx.reference_local_actor::<Contexts>(contexts).expect("contexts on this core"));
        let mut io = ctx.io().expect("io_uring on broker cores");
        io.accept(self.listener, token(ACCEPT, 0));
        io.read(self.timer, Vec::with_capacity(8), 0, token(TIMER, 0));
    }

    /// End of the loop iteration: one send per connection with pending output.
    fn on_callback(&mut self, ctx: &mut Context) {
        self.callback_armed = false;
        let dirty = std::mem::take(&mut self.dirty);
        for &id in &dirty {
            let Some(conn) = self.get(id) else { continue };
            conn.dirty = false;
            if conn.sending || conn.out.is_empty() {
                continue;
            }
            conn.sending = true;
            let fd = conn.fd;
            let out = std::mem::take(&mut conn.out);
            ctx.io().unwrap().send(fd, out, token(SEND, id));
        }
        self.dirty = dirty;
        self.dirty.clear();
    }

    fn on_destroy_request(&mut self, ctx: &mut Context) -> bool {
        let ids: Vec<u64> =
            (0..self.conns.len() as u32).filter(|&i| self.conns[i as usize].is_some()).map(|i| self.id(i)).collect();
        for id in ids {
            if let Some(c) = self.get(id) {
                c.will = None; // broker shutdown, not a client failure
            }
            self.close(ctx, id);
        }
        true
    }
}

impl Handler<IoCompletion> for Connections {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, c: &IoCompletion) {
        let kind = c.token >> 56;
        let id = c.token & 0x00ff_ffff_ffff_ffff;
        match kind {
            ACCEPT => {
                if c.result >= 0 {
                    self.accept(ctx, c.result);
                }
                ctx.io().unwrap().accept(self.listener, token(ACCEPT, 0));
            }
            TIMER => {
                self.sweep(ctx);
                let timer = self.timer;
                let mut buf = c.take_buf().unwrap_or_default();
                buf.clear();
                buf.reserve(8);
                ctx.io().unwrap().read(timer, buf, 0, token(TIMER, 0));
            }
            // Completions of a connection closed meanwhile are dropped, with their buffer.
            RECV if self.get(id).is_some() => self.received(ctx, id, c.result, c.take_buf().unwrap()),
            SEND if self.get(id).is_some() => self.sent(ctx, id, c.result, c.take_buf().unwrap()),
            _ => {}
        }
    }
}

impl Handler<Deliver> for Connections {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, d: &Deliver) {
        for &(id, qos) in &d.conns {
            self.deliver(ctx, id, &d.packet, qos);
        }
    }
}

impl Handler<Opened> for Connections {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, o: &Opened) {
        let Some(conn) = self.get(o.conn) else {
            // Closed meanwhile (it parked an empty session): hand the session back.
            if o.present {
                let client_id = o.client_id.clone();
                let partition = self.topology.partition(&client_id);
                let state = Undelivered {
                    inflight: o.state.inflight.clone(),
                    queue: o.state.queue.clone(),
                    received: o.state.received.clone(),
                };
                ctx.send(
                    actor(partition, CONTEXT),
                    Park { client_id, sub: (ctx.core() as u16, o.conn), filters: None, state },
                );
            }
            return;
        };
        codec::connack(&mut conn.out, o.present, 0);
        conn.session = Some(o.sid);
        let held = conn.held.take().unwrap_or_default();
        let core = ctx.core() as u16;
        let partition = self.topology.partition(&o.client_id);
        let target = (partition as u16, SESSION | o.sid as u64);
        for (filter, qos) in &o.filters {
            // The connection's entries first, then the session targets go: no gap.
            self.add_sub(ctx, filter, (core, o.conn), *qos);
            self.remove_sub(ctx, filter, target);
        }
        let max_inflight = self.max_inflight;
        let conn = self.get(o.conn).unwrap();
        conn.filters = o.filters.clone();
        if !o.state.inflight.is_empty() || !o.state.queue.is_empty() || !o.state.received.is_empty() {
            let ob = conn.outbound.get_or_insert_with(Default::default);
            ob.received = o.state.received.clone();
            for (pid, packet) in &o.state.inflight {
                if packet.is_empty() {
                    codec::pubrel(&mut conn.out, *pid); // QoS 2 at the PUBREL step
                    ob.inflight.push_back((*pid, Vec::new()));
                } else {
                    let mut p = packet.clone();
                    codec::set_dup(&mut p);
                    conn.out.extend_from_slice(&p);
                    ob.inflight.push_back((*pid, p));
                }
                ob.last_id = ob.last_id.max(*pid);
            }
            for (qos, packet) in &o.state.queue {
                if ob.inflight.len() < max_inflight {
                    ob.send(&mut conn.out, packet, *qos);
                } else {
                    ob.pending_bytes += packet.len();
                    ob.pending.push_back((*qos, packet.clone()));
                }
            }
        }
        self.mark_dirty(ctx, o.conn);
        self.received(ctx, o.conn, 1, held); // what came after CONNECT, then reads resume
    }
}

impl Handler<Stats> for Connections {
    /// First core: once every core reported for a round, publishes the totals, retained.
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, st: &Stats) {
        let cores = self.topology.cores.len();
        self.stats.resize(cores, None);
        let Some(i) = self.topology.cores.iter().position(|&c| c == src.core as CoreId) else { return };
        self.stats[i] = Some(*st);
        if !self.stats.iter().all(|s| s.is_some_and(|s| s.round == st.round)) {
            return;
        }
        let total = self.stats.iter().flatten().fold(Stats::default(), |a, s| Stats {
            round: s.round,
            connections: a.connections + s.connections,
            received: a.received + s.received,
            delivered: a.delivered + s.delivered,
            dropped: a.dropped + s.dropped,
            bytes_in: a.bytes_in + s.bytes_in,
            bytes_out: a.bytes_out + s.bytes_out,
            retained: a.retained + s.retained,
            sessions: a.sessions + s.sessions,
        });
        let subscriptions = self.router.as_ref().unwrap().borrow().index.len() as u64;
        let values = [
            ("version", concat!("croisette ", env!("CARGO_PKG_VERSION")).to_string()),
            ("uptime", format!("{} seconds", self.started.elapsed().as_secs())),
            ("clients/connected", total.connections.to_string()),
            ("messages/received", total.received.to_string()),
            ("messages/sent", total.delivered.to_string()),
            ("messages/dropped", total.dropped.to_string()),
            ("bytes/received", total.bytes_in.to_string()),
            ("bytes/sent", total.bytes_out.to_string()),
            ("subscriptions/count", subscriptions.to_string()),
            ("retained messages/count", total.retained.to_string()),
            ("sessions/count", total.sessions.to_string()),
        ];
        for (name, value) in values {
            let topic = format!("$SYS/broker/{name}");
            let partition = self.topology.partition(&topic);
            let retain = Retain { topic: topic.as_str().into(), payload: value.clone().into_bytes(), qos: 0 };
            ctx.send(actor(partition, CONTEXT), retain);
            self.publish(ctx, &topic, value.as_bytes(), 0);
        }
    }
}

impl Handler<Kick> for Connections {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, k: &Kick) {
        self.close(ctx, k.conn);
    }
}

// ---- Start-up ----------------------------------------------------------------------------------

pub struct Config {
    pub ip: Ipv4Addr,
    pub port: u16,
    pub cores: Vec<CoreId>,
    pub mode: CoreMode,
    /// A connection that has not sent CONNECT after this long is closed.
    pub connect_timeout: Duration,
    /// Largest packet accepted from a client (remaining length); larger ones close the connection.
    pub max_packet: usize,
    /// Output bytes a connection may have waiting before messages to it are dropped.
    pub max_queued: usize,
    /// QoS 1 messages sent to a connection and not acknowledged yet, at most.
    pub max_inflight: usize,
    /// Period of the `$SYS/broker/...` statistics (rounded to seconds; zero: none).
    pub sys_interval: Duration,
    /// Credentials and ACL; `None`: anonymous access to everything.
    pub auth: Option<Arc<Auth>>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            ip: Ipv4Addr::UNSPECIFIED,
            port: 1883,
            cores: vec![0],
            mode: CoreMode::Spin,
            connect_timeout: Duration::from_secs(10),
            max_packet: codec::MAX_PACKET,
            max_queued: 4 << 20,
            max_inflight: 32,
            sys_interval: Duration::from_secs(10),
            auth: None,
        }
    }
}

/// Starts the broker: on every core, a router replica, a context partition and the connection
/// tier with its own SO_REUSEPORT listener and io_uring.
pub fn start(config: &Config) -> std::io::Result<Engine> {
    let topology = Arc::new(Topology { cores: config.cores.clone() });
    let mut seq = StartSequence::new();
    for &core in &config.cores {
        let listener = net::reuseport_listener(config.ip, config.port, 4096)?;
        let timer = net::timerfd(TICK)?;
        let connect_timeout = config.connect_timeout.as_secs().max(1) as u32;
        let (max_packet, max_queued, max_inflight) = (config.max_packet, config.max_queued, config.max_inflight);
        let sys_interval = config.sys_interval.as_secs() as u32;
        let auth = config.auth.clone();
        seq.core_mode(core, config.mode);
        seq.io_uring(core, 4096);
        // Added in this order on every core, so that their ids are known up front.
        let r = seq.add_actor(core, || Router { index: SubIndex::new() });
        let t = topology.clone();
        let c = seq.add_actor(core, move || Contexts {
            topology: t,
            retained: HashMap::new(),
            by_client: HashMap::new(),
            sessions: HashMap::new(),
            next_sid: 0,
            max_queued,
        });
        let t = topology.clone();
        let n = seq.add_actor(core, move || Connections {
            topology: t,
            listener,
            timer,
            tick: 0,
            connect_timeout,
            max_packet,
            max_queued,
            max_inflight,
            dropped: 0,
            received_msgs: 0,
            delivered: 0,
            bytes_in: 0,
            bytes_out: 0,
            sys_interval,
            started: std::time::Instant::now(),
            stats: Vec::new(),
            router: None,
            contexts: None,
            auth: auth.clone(),
            forwards: HashMap::new(),
            conns: Vec::new(),
            generations: Vec::new(),
            free: Vec::new(),
            dirty: Vec::new(),
            callback_armed: false,
            packet: Vec::new(),
            targets: Vec::new(),
            open: 0,
        });
        assert_eq!((r, c, n), (actor(core, ROUTER), actor(core, CONTEXT), actor(core, CONNECTIONS)));
    }
    Ok(Engine::start(seq))
}

/// The adaptive idle mode used for "efficient" runs: spin while traffic flows, sleep after
/// 2000 idle iterations.
pub fn adaptive() -> CoreMode {
    CoreMode::Adaptive { idle_loops: 2_000, sleep: Duration::from_micros(50) }
}
