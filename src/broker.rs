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

struct Register {
    client_id: Box<str>,
    sub: Sub,
}

struct Unregister {
    client_id: Box<str>,
    sub: Sub,
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
    retained: HashMap<Box<str>, (Vec<u8>, u8)>,
    clients: HashMap<Box<str>, Sub>,
}

impl Actor for Contexts {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Retain, Self>();
        ctx.register_handler::<RetainedQuery, Self>();
        ctx.register_handler::<Register, Self>();
        ctx.register_handler::<Unregister, Self>();
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

impl Handler<Register> for Contexts {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, r: &Register) {
        if let Some(old) = self.clients.insert(r.client_id.clone(), r.sub) {
            if old != r.sub {
                // Session takeover: the previous connection with this client id is closed.
                ctx.send(actor(old.0 as CoreId, CONNECTIONS), Kick { conn: old.1 });
            }
        }
    }
}

impl Handler<Unregister> for Contexts {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, u: &Unregister) {
        if self.clients.get(&u.client_id) == Some(&u.sub) {
            self.clients.remove(&u.client_id);
        }
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

/// QoS 1 messages to a connection: sent and waiting for PUBACK, or waiting for room in the
/// in-flight window. Allocated with the first QoS 1 delivery.
#[derive(Default)]
struct Outbound {
    last_id: u16,
    /// Packet id and encoding, kept for redelivery.
    inflight: VecDeque<(u16, Vec<u8>)>,
    /// QoS 0 encodings, given an id when they enter the window.
    pending: VecDeque<Vec<u8>>,
    pending_bytes: usize,
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

    /// Moves `packet` into the window: encoded with a fresh id at the end of `out`.
    fn send(&mut self, out: &mut Vec<u8>, packet: &[u8]) {
        let id = self.next_id();
        let start = out.len();
        codec::requalify(out, packet, 1, id);
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
}

struct Conn {
    fd: RawFd,
    /// CONNECT accepted.
    connected: bool,
    keep_alive: u16,
    /// Tick of the last bytes received (or of the accept before that).
    last_rx: u32,
    will: Option<OwnedWill>,
    close_after_flush: bool,
    outbound: Option<Box<Outbound>>,
    /// Bytes waiting to be sent. (The receive buffer is always owned by the recv in flight.)
    out: Vec<u8>,
    sending: bool,
    dirty: bool,
    filters: Vec<Box<str>>,
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
    router: Option<ActorRef<Router>>,
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

    /// Closes connection `id`, publishing its will if it still has one (DISCONNECT clears it).
    fn close(&mut self, ctx: &mut Context, id: u64) {
        let index = (id & 0xffff_ffff) as usize;
        let Some(conn) = self.get(id).map(|_| ()).and_then(|_| self.conns[index].take()) else { return };
        self.generations[index] = self.generations[index].wrapping_add(1);
        self.free.push(index as u32);
        self.open -= 1;
        let sub = (self.core(ctx), id);
        for filter in &conn.filters {
            self.router.as_ref().unwrap().borrow_mut().index.remove(filter, sub);
            for &core in &self.topology.cores {
                if core != ctx.core() {
                    ctx.send(actor(core, ROUTER), SubRemove { filter: filter.clone(), sub });
                }
            }
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
            ctx.send(actor(partition, CONTEXT), Unregister { client_id, sub });
        }
        net::shutdown(conn.fd); // ends the pending recv and sends FIN
        ctx.io().unwrap().close(conn.fd, token(CLOSE, id));
    }

    fn received(&mut self, ctx: &mut Context, id: u64, result: i32, mut buf: Vec<u8>) {
        if result <= 0 {
            self.close(ctx, id);
            return;
        }
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
    }

    /// Handles one packet from connection `id`.
    fn handle(&mut self, ctx: &mut Context, id: u64, packet: Packet<'_>) -> Next {
        let core = self.core(ctx);
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
                } else {
                    0
                };
                codec::connack(&mut conn.out, false, code);
                if code != 0 {
                    return Next::CloseAfterFlush;
                }
                conn.connected = true;
                conn.keep_alive = c.keep_alive;
                conn.will = c.will.map(|w| OwnedWill {
                    topic: w.topic.into(),
                    payload: w.payload.to_vec(),
                    qos: w.qos.min(1),
                    retain: w.retain,
                });
                let client_id = c.client_id;
                if !client_id.is_empty() {
                    conn.client_id = Some(client_id.into());
                    let partition = self.topology.partition(client_id);
                    ctx.send(actor(partition, CONTEXT), Register { client_id: client_id.into(), sub: (core, id) });
                }
                self.mark_dirty(ctx, id);
            }
            Packet::Publish { topic, payload, qos, retain, packet_id, .. } => {
                if retain {
                    let partition = self.topology.partition(topic);
                    ctx.send(actor(partition, CONTEXT), Retain { topic: topic.into(), payload: payload.to_vec(), qos });
                }
                self.publish(ctx, topic, payload, qos);
                if qos == 1 {
                    codec::puback(&mut self.get(id).unwrap().out, packet_id.unwrap_or(0));
                    self.mark_dirty(ctx, id);
                } else if qos == 2 {
                    return Next::Close; // not supported yet
                }
            }
            Packet::Subscribe { packet_id, filters } => {
                let sub = (core, id);
                // QoS 2 is granted as QoS 1 (allowed by the protocol).
                let granted: Vec<u8> = filters.iter().map(|(_, q)| (*q).min(1)).collect();
                for (&(filter, _), &qos) in filters.iter().zip(&granted) {
                    self.router.as_ref().unwrap().borrow_mut().index.add(filter, sub, qos);
                    for &other in &self.topology.cores {
                        if other != ctx.core() {
                            ctx.send(actor(other, ROUTER), SubAdd { filter: filter.into(), sub, qos });
                        }
                        ctx.send(actor(other, CONTEXT), RetainedQuery { filter: filter.into(), conn: id, qos });
                    }
                }
                let conn = self.get(id).unwrap();
                for (f, _) in &filters {
                    if !conn.filters.iter().any(|g| &**g == *f) {
                        conn.filters.push((*f).into());
                    }
                }
                codec::suback(&mut conn.out, packet_id, &granted);
                self.mark_dirty(ctx, id);
            }
            Packet::Unsubscribe { packet_id, filters } => {
                let sub = (core, id);
                for filter in &filters {
                    self.router.as_ref().unwrap().borrow_mut().index.remove(filter, sub);
                    for &other in &self.topology.cores {
                        if other != ctx.core() {
                            ctx.send(actor(other, ROUTER), SubRemove { filter: (*filter).into(), sub });
                        }
                    }
                }
                let conn = self.get(id).unwrap();
                conn.filters.retain(|f| !filters.contains(&&**f));
                codec::unsuback(&mut conn.out, packet_id);
                self.mark_dirty(ctx, id);
            }
            Packet::PingReq => {
                codec::pingresp(&mut self.get(id).unwrap().out);
                self.mark_dirty(ctx, id);
            }
            Packet::PubAck { packet_id } => self.acked(ctx, id, packet_id),
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
        let (max_queued, max_inflight) = (self.max_queued, self.max_inflight);
        let Some(c) = self.get(id) else { return };
        if qos == 0 {
            if c.out.len() >= max_queued {
                self.dropped += 1;
                return;
            }
            c.out.extend_from_slice(packet);
        } else {
            let ob = c.outbound.get_or_insert_with(Default::default);
            if ob.inflight.len() < max_inflight {
                ob.send(&mut c.out, packet);
            } else if ob.pending_bytes + c.out.len() < max_queued {
                ob.pending_bytes += packet.len();
                ob.pending.push_back(packet.to_vec());
                return;
            } else {
                self.dropped += 1;
                return;
            }
        }
        self.mark_dirty(ctx, id);
    }

    /// PUBACK from connection `id`: frees a slot of its window for the next pending message.
    fn acked(&mut self, ctx: &mut Context, id: u64, packet_id: u16) {
        let max_inflight = self.max_inflight;
        let Some(c) = self.get(id) else { return };
        let Some(ob) = c.outbound.as_mut() else { return };
        let Some(at) = ob.inflight.iter().position(|(i, _)| *i == packet_id) else { return };
        ob.inflight.remove(at);
        let mut sent = false;
        while ob.inflight.len() < max_inflight {
            let Some(next) = ob.pending.pop_front() else { break };
            ob.pending_bytes -= next.len();
            ob.send(&mut c.out, &next);
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
        let router = actor(ctx.core(), ROUTER);
        self.router = Some(ctx.reference_local_actor::<Router>(router).expect("router on this core"));
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
        seq.core_mode(core, config.mode);
        seq.io_uring(core, 4096);
        // Added in this order on every core, so that their ids are known up front.
        let r = seq.add_actor(core, || Router { index: SubIndex::new() });
        let c = seq.add_actor(core, || Contexts { retained: HashMap::new(), clients: HashMap::new() });
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
            router: None,
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
