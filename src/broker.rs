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

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Duration;

use cannes::{Actor, ActorId, ActorRef, Context, CoreId, CoreMode, Engine, Handler, IoCompletion, StartSequence};

use crate::codec::{self, Packet};
use crate::index::{Sub, SubIndex};
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
}

struct SubRemove {
    filter: Box<str>,
    sub: Sub,
}

/// Encoded packets for connections of the receiving core.
struct Deliver {
    packets: Vec<u8>,
    conns: Vec<u64>,
}

struct Retain {
    topic: Box<str>,
    payload: Vec<u8>,
}

struct RetainedQuery {
    filter: Box<str>,
    conn: u64,
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
        self.index.add(&e.filter, e.sub);
    }
}

impl Handler<SubRemove> for Router {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, e: &SubRemove) {
        self.index.remove(&e.filter, e.sub);
    }
}

// ---- Tier 3: retained messages and client-id registry ------------------------------------------

pub struct Contexts {
    retained: HashMap<Box<str>, Vec<u8>>,
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
            self.retained.insert(e.topic.clone(), e.payload.clone());
        }
    }
}

impl Handler<RetainedQuery> for Contexts {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, q: &RetainedQuery) {
        let mut packets = Vec::new();
        for (topic, payload) in &self.retained {
            if codec::matches(&q.filter, topic) {
                codec::publish(&mut packets, topic, payload, 0, true, None);
            }
        }
        if !packets.is_empty() {
            ctx.send(src, Deliver { packets, conns: vec![q.conn] });
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

fn token(kind: u64, conn: u64) -> u64 {
    kind << 56 | conn
}

struct Conn {
    fd: RawFd,
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
    router: Option<ActorRef<Router>>,
    conns: Vec<Option<Conn>>,
    generations: Vec<u32>,
    free: Vec<u32>,
    dirty: Vec<u64>,
    callback_armed: bool,
    // Scratch buffers reused across publishes.
    packet: Vec<u8>,
    targets: Vec<Sub>,
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
        self.conns[index as usize] =
            Some(Conn { fd, out: Vec::new(), sending: false, dirty: false, filters: Vec::new(), client_id: None });
        self.open += 1;
        let id = self.id(index);
        ctx.io().unwrap().recv(fd, Vec::with_capacity(RECV_BUF), token(RECV, id));
    }

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
            match codec::parse(&buf[consumed..]) {
                Ok(Some((packet, len))) => {
                    consumed += len;
                    if !self.handle(ctx, id, packet) {
                        self.close(ctx, id);
                        return;
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
        if buf.capacity() - buf.len() < RECV_BUF / 2 {
            buf.reserve(RECV_BUF);
        }
        let Some(conn) = self.get(id) else { return };
        let fd = conn.fd;
        ctx.io().unwrap().recv(fd, buf, token(RECV, id));
    }

    /// Handles one packet from connection `id`; `false` closes the connection.
    fn handle(&mut self, ctx: &mut Context, id: u64, packet: Packet<'_>) -> bool {
        let core = self.core(ctx);
        match packet {
            Packet::Connect { client_id, .. } => {
                let conn = self.get(id).unwrap();
                codec::connack(&mut conn.out, false, 0);
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
                    ctx.send(actor(partition, CONTEXT), Retain { topic: topic.into(), payload: payload.to_vec() });
                }
                self.publish(ctx, topic, payload);
                if qos == 1 {
                    codec::puback(&mut self.get(id).unwrap().out, packet_id.unwrap_or(0));
                    self.mark_dirty(ctx, id);
                } else if qos == 2 {
                    return false; // not supported by the PoC
                }
            }
            Packet::Subscribe { packet_id, filters } => {
                let sub = (core, id);
                for (filter, _) in &filters {
                    self.router.as_ref().unwrap().borrow_mut().index.add(filter, sub);
                    for &other in &self.topology.cores {
                        if other != ctx.core() {
                            ctx.send(actor(other, ROUTER), SubAdd { filter: (*filter).into(), sub });
                        }
                        ctx.send(actor(other, CONTEXT), RetainedQuery { filter: (*filter).into(), conn: id });
                    }
                }
                let conn = self.get(id).unwrap();
                conn.filters.extend(filters.iter().map(|(f, _)| Box::<str>::from(*f)));
                let granted = vec![0u8; filters.len()]; // QoS 0 granted
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
            Packet::PubAck { .. } => {}
            Packet::Disconnect => return false,
            _ => return false,
        }
        true
    }

    /// Routes a message: synchronous match on this core's router replica, local delivery, one
    /// `Deliver` event per other core with subscribers.
    fn publish(&mut self, ctx: &mut Context, topic: &str, payload: &[u8]) {
        let mut targets = std::mem::take(&mut self.targets);
        targets.clear();
        self.router.as_ref().unwrap().borrow().index.for_each_match(topic, |s| targets.push(s));
        if targets.is_empty() {
            self.targets = targets;
            return;
        }
        targets.sort_unstable();
        targets.dedup(); // one delivery per connection, whatever the number of matching filters
        let mut packet = std::mem::take(&mut self.packet);
        packet.clear();
        codec::publish(&mut packet, topic, payload, 0, false, None);
        let here = self.core(ctx);
        let mut i = 0;
        while i < targets.len() {
            let core = targets[i].0;
            let j = targets[i..].iter().position(|s| s.0 != core).map_or(targets.len(), |n| i + n);
            if core == here {
                for &(_, id) in &targets[i..j] {
                    if let Some(c) = self.get(id) {
                        c.out.extend_from_slice(&packet);
                        self.mark_dirty(ctx, id);
                    }
                }
            } else {
                let conns = targets[i..j].iter().map(|s| s.1).collect();
                ctx.send(actor(core as CoreId, CONNECTIONS), Deliver { packets: packet.clone(), conns });
            }
            i = j;
        }
        self.packet = packet;
        self.targets = targets;
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
        ctx.io().expect("io_uring on broker cores").accept(self.listener, token(ACCEPT, 0));
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
            // Completions of a connection closed meanwhile are dropped, with their buffer.
            RECV if self.get(id).is_some() => self.received(ctx, id, c.result, c.take_buf().unwrap()),
            SEND if self.get(id).is_some() => self.sent(ctx, id, c.result, c.take_buf().unwrap()),
            _ => {}
        }
    }
}

impl Handler<Deliver> for Connections {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, d: &Deliver) {
        for &id in &d.conns {
            if let Some(c) = self.get(id) {
                c.out.extend_from_slice(&d.packets);
                self.mark_dirty(ctx, id);
            }
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
}

impl Default for Config {
    fn default() -> Self {
        Config { ip: Ipv4Addr::UNSPECIFIED, port: 1883, cores: vec![0], mode: CoreMode::Spin }
    }
}

/// Starts the broker: on every core, a router replica, a context partition and the connection
/// tier with its own SO_REUSEPORT listener and io_uring.
pub fn start(config: &Config) -> std::io::Result<Engine> {
    let topology = Arc::new(Topology { cores: config.cores.clone() });
    let mut seq = StartSequence::new();
    for &core in &config.cores {
        let listener = net::reuseport_listener(config.ip, config.port, 4096)?;
        seq.core_mode(core, config.mode);
        seq.io_uring(core, 4096);
        // Added in this order on every core, so that their ids are known up front.
        let r = seq.add_actor(core, || Router { index: SubIndex::new() });
        let c = seq.add_actor(core, || Contexts { retained: HashMap::new(), clients: HashMap::new() });
        let t = topology.clone();
        let n = seq.add_actor(core, move || Connections {
            topology: t,
            listener,
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
