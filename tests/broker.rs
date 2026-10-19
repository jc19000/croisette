//! End-to-end tests of the broker on two cores, with real TCP clients.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use cannes::CoreMode;
use croisette::broker::{self, Config};
use croisette::client::{Client, Incoming};
use croisette::codec::{self, Connect, Will};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn start(cores: Vec<usize>) -> (cannes::Engine, SocketAddr) {
    start_with(Config { cores, ..Config::default() })
}

fn start_with(config: Config) -> (cannes::Engine, SocketAddr) {
    let port = free_port();
    let config = Config {
        ip: Ipv4Addr::LOCALHOST,
        port,
        mode: CoreMode::Spin,
        connect_timeout: Duration::from_secs(1),
        ..config
    };
    let engine = broker::start(&config).unwrap();
    (engine, SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

fn client(addr: SocketAddr, id: &str) -> Client {
    let c = Client::connect(addr, id).unwrap();
    c.set_timeout(Some(Duration::from_secs(5))).unwrap();
    c
}

fn publish_msg(topic: &str, payload: &[u8]) -> Incoming {
    Incoming::Publish { topic: topic.into(), payload: payload.into(), qos: 0, retain: false }
}

#[test]
fn pub_sub_across_cores_with_wildcards() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    // Many subscribers: SO_REUSEPORT spreads them over both cores.
    let mut subs: Vec<Client> = (0..16).map(|i| client(addr, &format!("sub-{i}"))).collect();
    for (i, s) in subs.iter_mut().enumerate() {
        let filter = if i % 2 == 0 { "sensors/+/temp" } else { "sensors/#" };
        s.subscribe(&[filter]).unwrap();
    }
    let mut publisher = client(addr, "pub");
    publisher.publish("sensors/k1/temp", b"21.5", 0, false).unwrap();
    publisher.publish("sensors/k1/hum", b"40", 1, false).unwrap();
    for (i, s) in subs.iter_mut().enumerate() {
        assert_eq!(s.next_publish().unwrap(), publish_msg("sensors/k1/temp", b"21.5"), "sub {i}");
        if i % 2 == 1 {
            assert_eq!(s.next_publish().unwrap(), publish_msg("sensors/k1/hum", b"40"), "sub {i}");
        }
    }
    // Nothing more for the `+/temp` subscribers.
    subs[0].set_timeout(Some(Duration::from_millis(200))).unwrap();
    assert!(subs[0].next_publish().is_err());
    engine.shutdown();
}

#[test]
fn one_delivery_per_connection_with_overlapping_filters() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut s = client(addr, "s");
    s.subscribe(&["a/#", "a/+", "#"]).unwrap();
    let mut p = client(addr, "p");
    p.publish("a/b", b"1", 0, false).unwrap();
    p.publish("a/c", b"2", 0, false).unwrap();
    assert_eq!(s.next_publish().unwrap(), publish_msg("a/b", b"1"));
    assert_eq!(s.next_publish().unwrap(), publish_msg("a/c", b"2"));
    engine.shutdown();
}

#[test]
fn retained_message_is_sent_on_subscribe_and_cleared() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut p = client(addr, "p");
    p.publish("status/door", b"open", 1, true).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let mut s = client(addr, "s1");
    s.subscribe(&["status/#"]).unwrap();
    assert_eq!(
        s.next_publish().unwrap(),
        Incoming::Publish { topic: "status/door".into(), payload: b"open".to_vec(), qos: 0, retain: true }
    );
    p.publish("status/door", b"", 1, true).unwrap(); // empty retained payload clears it
    std::thread::sleep(Duration::from_millis(50));
    let mut s2 = client(addr, "s2");
    s2.subscribe(&["status/#"]).unwrap();
    s2.set_timeout(Some(Duration::from_millis(200))).unwrap();
    assert!(s2.next_publish().is_err(), "retained message should be gone");
    engine.shutdown();
}

#[test]
fn unsubscribe_and_disconnect_remove_subscriptions() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut s = client(addr, "s");
    s.subscribe(&["x"]).unwrap();
    let gone = {
        let mut g = client(addr, "gone");
        g.subscribe(&["x"]).unwrap();
        g
    };
    drop(gone);
    std::thread::sleep(Duration::from_millis(50));
    s.unsubscribe(&["x"]).unwrap();
    let mut p = client(addr, "p");
    p.publish("x", b"1", 1, false).unwrap();
    s.set_timeout(Some(Duration::from_millis(200))).unwrap();
    assert!(s.next_publish().is_err());
    engine.shutdown();
}

#[test]
fn same_client_id_takes_over_the_session() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut first = client(addr, "device-42");
    first.subscribe(&["cmd"]).unwrap();
    let _second = client(addr, "device-42");
    // The first connection is closed by the broker.
    first.set_timeout(Some(Duration::from_secs(2))).unwrap();
    let err = first.next_publish().unwrap_err();
    assert!(matches!(err.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset), "{err:?}");
    engine.shutdown();
}

#[test]
fn many_messages_keep_their_order() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut subs: Vec<Client> = (0..4).map(|i| client(addr, &format!("s{i}"))).collect();
    for s in subs.iter_mut() {
        s.subscribe(&["seq"]).unwrap();
    }
    let mut p = client(addr, "p");
    for i in 0..20_000u32 {
        p.queue_publish("seq", &i.to_be_bytes(), 0, false);
        if i % 100 == 99 {
            p.flush().unwrap();
        }
    }
    p.flush().unwrap();
    for s in subs.iter_mut() {
        let mut next = 0u32;
        while next < 20_000 {
            s.read_publishes(|payload| {
                assert_eq!(u32::from_be_bytes(payload.try_into().unwrap()), next);
                next += 1;
            })
            .unwrap();
        }
    }
    engine.shutdown();
}

// ---- CONNECT, will, keep-alive ------------------------------------------------------------------

fn with_will<'a>(id: &'a str, topic: &'a str, retain: bool, keep_alive: u16) -> Connect<'a> {
    Connect {
        level: 4,
        client_id: id,
        keep_alive,
        clean_session: true,
        will: Some(Will { topic, payload: b"gone", qos: 0, retain, ..Will::default() }),
        ..Connect::default()
    }
}

fn assert_closed_by_broker(stream: &mut TcpStream, within: Duration) {
    stream.set_read_timeout(Some(within)).unwrap();
    let mut b = [0u8; 64];
    loop {
        match stream.read(&mut b) {
            Ok(0) => return,
            Ok(_) => continue, // e.g. a CONNACK before the close
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return,
            Err(e) => panic!("connection still open: {e}"),
        }
    }
}

#[test]
fn will_is_published_on_abrupt_close_only() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut watcher = client(addr, "watcher");
    watcher.subscribe(&["status/#"]).unwrap();
    let (dev, _) = Client::connect_with(addr, &with_will("dev1", "status/dev1", false, 60)).unwrap();
    drop(dev); // no DISCONNECT
    assert_eq!(watcher.next_publish().unwrap(), publish_msg("status/dev1", b"gone"));
    let (dev, _) = Client::connect_with(addr, &with_will("dev2", "status/dev2", false, 60)).unwrap();
    dev.disconnect().unwrap();
    watcher.set_timeout(Some(Duration::from_millis(300))).unwrap();
    assert!(watcher.next_publish().is_err(), "no will after DISCONNECT");
    // A retained will stays for later subscribers.
    let (dev, _) = Client::connect_with(addr, &with_will("dev3", "status/dev3", true, 60)).unwrap();
    drop(dev);
    std::thread::sleep(Duration::from_millis(100));
    let mut late = client(addr, "late");
    late.subscribe(&["status/dev3"]).unwrap();
    assert_eq!(
        late.next_publish().unwrap(),
        Incoming::Publish { topic: "status/dev3".into(), payload: b"gone".to_vec(), qos: 0, retain: true }
    );
    engine.shutdown();
}

#[test]
fn keep_alive_expiry_closes_and_publishes_the_will() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut watcher = client(addr, "watcher");
    watcher.subscribe(&["status/#"]).unwrap();
    let (mut silent, _) = Client::connect_with(addr, &with_will("silent", "status/silent", false, 1)).unwrap();
    let (mut pinger, _) = Client::connect_with(addr, &with_will("pinger", "status/pinger", false, 1)).unwrap();
    let t = Instant::now();
    // The pinger stays alive; the silent client is closed after 1.5 s (rounded up to the tick).
    for _ in 0..8 {
        pinger.ping().unwrap();
        std::thread::sleep(Duration::from_millis(400));
    }
    assert_eq!(watcher.next_publish().unwrap(), publish_msg("status/silent", b"gone"));
    assert_closed_by_broker(&mut silent.stream, Duration::from_secs(1));
    assert!(t.elapsed() >= Duration::from_millis(1500));
    pinger.ping().unwrap();
    engine.shutdown();
}

#[test]
fn connect_is_validated() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let refused = |c: &Connect<'_>| Client::connect_with(addr, c).err().map(|e| (e.kind(), e.to_string()));
    let bad_level = Connect { level: 6, client_id: "x", clean_session: true, ..Connect::default() };
    assert_eq!(refused(&bad_level), Some((std::io::ErrorKind::ConnectionRefused, "CONNACK code 1".into())));
    let no_id = Connect { level: 4, client_id: "", clean_session: false, ..Connect::default() };
    assert_eq!(refused(&no_id), Some((std::io::ErrorKind::ConnectionRefused, "CONNACK code 2".into())));
    // An empty client id with a clean session is accepted.
    let anonymous = Connect { level: 4, client_id: "", clean_session: true, ..Connect::default() };
    assert!(refused(&anonymous).is_none());
    // A packet before CONNECT closes the connection.
    let mut raw = TcpStream::connect(addr).unwrap();
    let mut b = Vec::new();
    codec::pingreq(&mut b);
    raw.write_all(&b).unwrap();
    assert_closed_by_broker(&mut raw, Duration::from_secs(2));
    // So does a second CONNECT.
    let mut c = client(addr, "twice");
    let mut b = Vec::new();
    codec::connect(&mut b, "twice", 60, true);
    c.stream.write_all(&b).unwrap();
    assert_closed_by_broker(&mut c.stream, Duration::from_secs(2));
    // And no CONNECT at all, after the connect timeout (1 s in the tests).
    let mut idle = TcpStream::connect(addr).unwrap();
    assert_closed_by_broker(&mut idle, Duration::from_secs(4));
    engine.shutdown();
}

// ---- Limits ---------------------------------------------------------------------------------

#[test]
fn slow_subscriber_loses_qos0_messages_without_slowing_the_others() {
    let _g = serial();
    let (engine, addr) = start_with(Config { cores: vec![1, 2], max_queued: 1 << 20, ..Config::default() });
    let mut slow = client(addr, "slow");
    slow.subscribe(&["flood"]).unwrap();
    let mut fast = client(addr, "fast");
    fast.subscribe(&["flood"]).unwrap();
    let mut p = client(addr, "p");
    let payload = [7u8; 1000];
    let n = 20_000; // 20 MB, far more than the socket buffers and the 1 MiB queue
    let reader = std::thread::spawn(move || {
        let mut got = 0;
        while got < n {
            got += fast.read_publishes(|_| {}).unwrap();
        }
        fast
    });
    for i in 0..n {
        p.queue_publish("flood", &payload, 0, false);
        if i % 100 == 99 {
            p.flush().unwrap();
        }
    }
    p.flush().unwrap();
    let _fast = reader.join().unwrap(); // the fast subscriber got every message
                                        // The slow one now reads what was kept for it: a bounded part of the flood.
    slow.set_timeout(Some(Duration::from_millis(500))).unwrap();
    let mut kept = 0;
    while slow.next_publish().is_ok() {
        kept += 1;
    }
    // Its connection survived: it gets the next message.
    p.publish("flood", b"end", 1, false).unwrap();
    slow.set_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(slow.next_publish().unwrap(), publish_msg("flood", b"end"));
    assert!(kept < n / 2, "kept {kept} of {n}");
    engine.shutdown();
}

#[test]
fn oversized_packet_closes_the_connection() {
    let _g = serial();
    let (engine, addr) = start_with(Config { cores: vec![1, 2], max_packet: 1000, ..Config::default() });
    let mut s = client(addr, "s");
    s.subscribe(&["big"]).unwrap();
    let mut p = client(addr, "p");
    p.publish("big", &[1u8; 900], 0, false).unwrap();
    assert_eq!(s.next_publish().unwrap(), publish_msg("big", &[1u8; 900]));
    p.queue_publish("big", &[1u8; 1100], 0, false);
    p.flush().unwrap();
    assert_closed_by_broker(&mut p.stream, Duration::from_secs(2));
    engine.shutdown();
}

#[test]
fn large_packets_are_received_whole() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut s = client(addr, "s");
    s.subscribe(&["big"]).unwrap();
    let mut p = client(addr, "p");
    let payload: Vec<u8> = (0..900_000u32).map(|i| i as u8).collect();
    p.publish("big", &payload, 1, false).unwrap();
    assert_eq!(s.next_publish().unwrap(), publish_msg("big", &payload));
    engine.shutdown();
}

// ---- QoS 1 --------------------------------------------------------------------------------------

fn qos1_msg(topic: &str, payload: &[u8], retain: bool) -> Incoming {
    Incoming::Publish { topic: topic.into(), payload: payload.into(), qos: 1, retain }
}

#[test]
fn delivery_qos_is_the_lower_of_publish_and_subscription() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut s1 = client(addr, "s1");
    assert_eq!(s1.subscribe_qos(&["q/1"], 1).unwrap(), vec![1]);
    assert_eq!(s1.subscribe_qos(&["q/2"], 2).unwrap(), vec![2]);
    let mut s0 = client(addr, "s0");
    s0.subscribe(&["q/1"]).unwrap();
    let mut p = client(addr, "p");
    p.publish("q/1", b"a", 1, false).unwrap();
    p.publish("q/1", b"b", 0, false).unwrap();
    p.publish("q/2", b"c", 1, false).unwrap();
    assert_eq!(s1.next_publish().unwrap(), qos1_msg("q/1", b"a", false));
    assert_eq!(s1.next_publish().unwrap(), publish_msg("q/1", b"b"));
    assert_eq!(s1.next_publish().unwrap(), qos1_msg("q/2", b"c", false));
    assert_eq!(s0.next_publish().unwrap(), publish_msg("q/1", b"a"));
    assert_eq!(s0.next_publish().unwrap(), publish_msg("q/1", b"b"));
    // Overlapping filters: one delivery, at the highest QoS.
    let mut both = client(addr, "both");
    both.subscribe(&["o/#"]).unwrap();
    both.subscribe_qos(&["o/+"], 1).unwrap();
    p.publish("o/x", b"d", 1, false).unwrap();
    assert_eq!(both.next_publish().unwrap(), qos1_msg("o/x", b"d", false));
    both.set_timeout(Some(Duration::from_millis(200))).unwrap();
    assert!(both.next_publish().is_err());
    engine.shutdown();
}

#[test]
fn inflight_window_waits_for_acks() {
    let _g = serial();
    let (engine, addr) = start_with(Config { cores: vec![1, 2], max_inflight: 4, ..Config::default() });
    let mut s = client(addr, "s");
    s.auto_ack = false;
    s.subscribe_qos(&["w"], 1).unwrap();
    let mut p = client(addr, "p");
    for i in 0..10u8 {
        p.publish("w", &[i], 1, false).unwrap();
    }
    let mut ids = Vec::new();
    for i in 0..4u8 {
        assert_eq!(s.next_publish().unwrap(), qos1_msg("w", &[i], false));
        ids.push(s.last_packet_id.unwrap());
    }
    s.set_timeout(Some(Duration::from_millis(200))).unwrap();
    assert!(s.next_publish().is_err(), "window full");
    // Acknowledging two messages lets two more through, in order.
    s.puback(ids[1]).unwrap();
    s.puback(ids[0]).unwrap();
    s.set_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(s.next_publish().unwrap(), qos1_msg("w", &[4], false));
    assert_eq!(s.next_publish().unwrap(), qos1_msg("w", &[5], false));
    s.set_timeout(Some(Duration::from_millis(200))).unwrap();
    assert!(s.next_publish().is_err(), "window full again");
    s.auto_ack = true;
    for &id in &ids[2..] {
        s.puback(id).unwrap();
    }
    s.puback(s.last_packet_id.unwrap()).unwrap();
    s.set_timeout(Some(Duration::from_secs(5))).unwrap();
    for i in 6..10u8 {
        assert_eq!(s.next_publish().unwrap(), qos1_msg("w", &[i], false));
    }
    engine.shutdown();
}

#[test]
fn retained_message_keeps_its_qos() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut p = client(addr, "p");
    p.publish("cfg/a", b"1", 1, true).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let mut s1 = client(addr, "s1");
    s1.subscribe_qos(&["cfg/#"], 1).unwrap();
    assert_eq!(s1.next_publish().unwrap(), qos1_msg("cfg/a", b"1", true));
    let mut s0 = client(addr, "s0");
    s0.subscribe(&["cfg/#"]).unwrap();
    assert_eq!(
        s0.next_publish().unwrap(),
        Incoming::Publish { topic: "cfg/a".into(), payload: b"1".to_vec(), qos: 0, retain: true }
    );
    engine.shutdown();
}

#[test]
fn many_qos1_messages_keep_their_order() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut subs: Vec<Client> = (0..4).map(|i| client(addr, &format!("s{i}"))).collect();
    for s in subs.iter_mut() {
        s.subscribe_qos(&["seq"], 1).unwrap();
    }
    let mut p = client(addr, "p");
    let readers: Vec<_> = subs
        .into_iter()
        .map(|mut s| {
            std::thread::spawn(move || {
                let mut next = 0u32;
                while next < 20_000 {
                    s.read_publishes(|payload| {
                        assert_eq!(u32::from_be_bytes(payload.try_into().unwrap()), next);
                        next += 1;
                    })
                    .unwrap();
                }
            })
        })
        .collect();
    let drain = {
        let mut acks = p.stream.try_clone().unwrap();
        std::thread::spawn(move || {
            let mut b = [0u8; 4096];
            while acks.read(&mut b).is_ok_and(|n| n > 0) {}
        })
    };
    for i in 0..20_000u32 {
        p.queue_publish("seq", &i.to_be_bytes(), 1, false);
        if i % 100 == 99 {
            p.flush().unwrap();
        }
    }
    p.flush().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    engine.shutdown();
    drop(p);
    drain.join().unwrap();
}

// ---- Persistent sessions ------------------------------------------------------------------------

fn persistent(addr: SocketAddr, id: &str) -> (Client, bool) {
    let c = Connect { level: 4, client_id: id, keep_alive: 60, clean_session: false, ..Connect::default() };
    let (client, present) = Client::connect_with(addr, &c).unwrap();
    client.set_timeout(Some(Duration::from_secs(5))).unwrap();
    (client, present)
}

fn assert_nothing(c: &mut Client) {
    c.set_timeout(Some(Duration::from_millis(200))).unwrap();
    assert!(c.next_publish().is_err(), "unexpected message");
    c.set_timeout(Some(Duration::from_secs(5))).unwrap();
}

#[test]
fn persistent_session_keeps_subscriptions_and_queues_qos1() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let (mut dev, present) = persistent(addr, "dev");
    assert!(!present);
    dev.subscribe_qos(&["cmd/#"], 1).unwrap();
    dev.subscribe(&["info"]).unwrap();
    dev.disconnect().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let mut p = client(addr, "p");
    p.publish("cmd/a", b"1", 1, false).unwrap();
    p.publish("cmd/b", b"2", 1, false).unwrap();
    p.publish("cmd/c", b"3", 0, false).unwrap(); // QoS 0: not kept
    p.publish("info", b"x", 1, false).unwrap(); // subscribed at QoS 0: not kept
    std::thread::sleep(Duration::from_millis(50));
    let (mut dev, present) = persistent(addr, "dev");
    assert!(present);
    assert_eq!(dev.next_publish().unwrap(), qos1_msg("cmd/a", b"1", false));
    assert_eq!(dev.next_publish().unwrap(), qos1_msg("cmd/b", b"2", false));
    assert_nothing(&mut dev);
    // The subscriptions are back, without a new SUBSCRIBE.
    p.publish("cmd/d", b"4", 1, false).unwrap();
    p.publish("info", b"y", 0, false).unwrap();
    assert_eq!(dev.next_publish().unwrap(), qos1_msg("cmd/d", b"4", false));
    assert_eq!(dev.next_publish().unwrap(), publish_msg("info", b"y"));
    assert_nothing(&mut dev);
    engine.shutdown();
}

#[test]
fn unacknowledged_messages_are_redelivered_with_dup() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let (mut dev, _) = persistent(addr, "dev");
    dev.auto_ack = false;
    dev.subscribe_qos(&["t"], 1).unwrap();
    let mut p = client(addr, "p");
    p.publish("t", b"1", 1, false).unwrap();
    p.publish("t", b"2", 1, false).unwrap();
    assert_eq!(dev.next_publish().unwrap(), qos1_msg("t", b"1", false));
    dev.puback(dev.last_packet_id.unwrap()).unwrap();
    assert_eq!(dev.next_publish().unwrap(), qos1_msg("t", b"2", false));
    let second = dev.last_packet_id.unwrap();
    std::thread::sleep(Duration::from_millis(50));
    drop(dev); // without acknowledging "2"
    std::thread::sleep(Duration::from_millis(50));
    let (mut dev, present) = persistent(addr, "dev");
    assert!(present);
    assert_eq!(dev.next_publish().unwrap(), qos1_msg("t", b"2", false));
    assert!(dev.last_dup);
    assert_eq!(dev.last_packet_id, Some(second), "same packet id");
    assert_nothing(&mut dev);
    engine.shutdown();
}

#[test]
fn clean_session_discards_the_persistent_one() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let (mut dev, _) = persistent(addr, "dev");
    dev.subscribe_qos(&["z"], 1).unwrap();
    dev.disconnect().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let mut p = client(addr, "p");
    p.publish("z", b"queued", 1, false).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let mut clean = client(addr, "dev");
    assert_nothing(&mut clean);
    clean.disconnect().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    p.publish("z", b"lost", 1, false).unwrap();
    let (mut dev, present) = persistent(addr, "dev");
    assert!(!present, "the clean session replaced it");
    assert_nothing(&mut dev);
    engine.shutdown();
}

#[test]
fn takeover_moves_the_persistent_session() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let (mut first, _) = persistent(addr, "dev");
    first.subscribe_qos(&["k"], 1).unwrap();
    let (mut second, present) = persistent(addr, "dev");
    assert!(present);
    assert_closed_by_broker(&mut first.stream, Duration::from_secs(2));
    let mut p = client(addr, "p");
    p.publish("k", b"1", 1, false).unwrap();
    assert_eq!(second.next_publish().unwrap(), qos1_msg("k", b"1", false));
    assert_nothing(&mut second);
    engine.shutdown();
}

#[test]
fn no_qos1_message_lost_across_reconnections() {
    no_message_lost_across_reconnections(1);
}

#[test]
fn no_qos2_message_lost_across_reconnections() {
    no_message_lost_across_reconnections(2);
}

fn no_message_lost_across_reconnections(qos: u8) {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let (mut dev, _) = persistent(addr, "dev");
    dev.subscribe_qos(&["s"], qos).unwrap();
    let n = 3000u32;
    let publisher = std::thread::spawn(move || {
        let mut p = client(addr, "p");
        for i in 0..n {
            // Batches of 10 without waiting, then the PUBACKs: the subscriber falls behind, so
            // reconnections find messages in flight and queued.
            if qos == 1 {
                p.queue_publish("s", &i.to_be_bytes(), 1, false);
                if i % 10 == 9 {
                    p.flush().unwrap();
                    for _ in 0..10 {
                        p.wait_puback().unwrap();
                    }
                }
            } else {
                p.publish("s", &i.to_be_bytes(), 2, false).unwrap();
            }
        }
    });
    let mut seen = vec![false; n as usize];
    let mut count = 0;
    let mut reconnects = 0;
    let mut last_reconnect = 0;
    let mut redelivered = 0;
    while count < n {
        match dev.next_publish() {
            Ok(Incoming::Publish { payload, .. }) => {
                let i = u32::from_be_bytes(payload[..4].try_into().unwrap()) as usize;
                if dev.last_dup {
                    redelivered += 1;
                }
                if !seen[i] {
                    seen[i] = true;
                    count += 1;
                }
                if count % 150 == 0 && reconnects < 20 && last_reconnect != count {
                    last_reconnect = count;
                    drop(dev); // abrupt, with messages in flight
                    reconnects += 1;
                    dev = persistent(addr, "dev").0;
                }
            }
            Ok(other) => panic!("{other:?}"),
            Err(e) => panic!(
                "message {:?} missing: {e}; count {count} reconnects {reconnects} seen up to {:?}",
                seen.iter().position(|&s| !s),
                seen.iter().rposition(|&s| s)
            ),
        }
    }
    publisher.join().unwrap();
    assert_eq!(reconnects, 20);
    // QoS 1 publishers pipeline, so some reconnections find unacknowledged messages; QoS 2
    // ones go one message at a time and may not.
    if qos == 1 {
        assert!(redelivered > 0, "reconnections happened with unacknowledged messages");
    }
    engine.shutdown();
}

// ---- QoS 2 --------------------------------------------------------------------------------------

#[test]
fn qos2_is_delivered_once() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut s2 = client(addr, "s2");
    s2.subscribe_qos(&["x"], 2).unwrap();
    let mut s1 = client(addr, "s1");
    s1.subscribe_qos(&["x"], 1).unwrap();
    let mut p = client(addr, "p");
    p.publish("x", b"once", 2, false).unwrap(); // PUBLISH, PUBREC, PUBREL, PUBCOMP
    let qos2 = Incoming::Publish { topic: "x".into(), payload: b"once".to_vec(), qos: 2, retain: false };
    assert_eq!(s2.next_publish().unwrap(), qos2);
    assert_eq!(s1.next_publish().unwrap(), qos1_msg("x", b"once", false));
    // A publisher resending the same packet before PUBREL is not routed twice.
    let mut raw = client(addr, "raw");
    let mut b = Vec::new();
    codec::publish(&mut b, "x", b"dup", 2, false, Some(7));
    codec::publish(&mut b, "x", b"dup", 2, false, Some(7));
    raw.stream.write_all(&b).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    b.clear();
    codec::pubrel(&mut b, 7);
    // After PUBREL the id is free: the same id is a new message.
    codec::publish(&mut b, "x", b"next", 2, false, Some(7));
    raw.stream.write_all(&b).unwrap();
    let dup = Incoming::Publish { topic: "x".into(), payload: b"dup".to_vec(), qos: 2, retain: false };
    let next = Incoming::Publish { topic: "x".into(), payload: b"next".to_vec(), qos: 2, retain: false };
    assert_eq!(s2.next_publish().unwrap(), dup);
    assert_eq!(s2.next_publish().unwrap(), next);
    assert_nothing(&mut s2);
    engine.shutdown();
}

#[test]
fn qos2_state_survives_reconnection() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let (mut dev, _) = persistent(addr, "dev");
    dev.auto_ack = false;
    dev.subscribe_qos(&["y"], 2).unwrap();
    let mut p = client(addr, "p");
    p.publish("y", b"1", 2, false).unwrap();
    p.publish("y", b"2", 2, false).unwrap();
    let msg = |v: &[u8]| Incoming::Publish { topic: "y".into(), payload: v.to_vec(), qos: 2, retain: false };
    assert_eq!(dev.next_publish().unwrap(), msg(b"1"));
    dev.pubrec(dev.last_packet_id.unwrap()).unwrap(); // "1" reaches the PUBREL step
    assert_eq!(dev.next_publish().unwrap(), msg(b"2")); // "2" is not acknowledged at all
    std::thread::sleep(Duration::from_millis(50));
    drop(dev);
    std::thread::sleep(Duration::from_millis(50));
    let (mut dev, present) = persistent(addr, "dev");
    assert!(present);
    // "1" only gets its PUBREL again (answered by the client), "2" comes back as a DUP.
    assert_eq!(dev.next_publish().unwrap(), msg(b"2"));
    assert!(dev.last_dup);
    assert_nothing(&mut dev);
    p.publish("y", b"3", 2, false).unwrap();
    assert_eq!(dev.next_publish().unwrap(), msg(b"3"));
    engine.shutdown();
}

// ---- $SYS ---------------------------------------------------------------------------------------

#[test]
fn sys_topics_report_the_totals_of_every_core() {
    let _g = serial();
    let (engine, addr) =
        start_with(Config { cores: vec![1, 2], sys_interval: Duration::from_secs(1), ..Config::default() });
    let mut all = client(addr, "all");
    all.subscribe(&["#"]).unwrap();
    let mut p = client(addr, "p");
    for _ in 0..5 {
        p.publish("t", b"x", 1, false).unwrap();
    }
    for _ in 0..5 {
        all.next_publish().unwrap();
    }
    let mut sys = client(addr, "sys");
    sys.subscribe(&["$SYS/broker/#"]).unwrap();
    let mut values = std::collections::HashMap::new();
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(4) {
        if let Ok(Incoming::Publish { topic, payload, .. }) = sys.next_publish() {
            values.insert(topic, String::from_utf8(payload).unwrap());
        }
        let received: u64 = values.get("$SYS/broker/messages/received").map_or(0, |v| v.parse().unwrap());
        if received >= 5 && values.len() >= 11 {
            break;
        }
    }
    assert!(values["$SYS/broker/version"].starts_with("croisette "), "{values:?}");
    assert_eq!(values["$SYS/broker/clients/connected"], "3");
    assert_eq!(values["$SYS/broker/messages/received"], "5");
    assert_eq!(values["$SYS/broker/messages/sent"], "5");
    assert_eq!(values["$SYS/broker/sessions/count"], "3");
    // `#` does not match `$SYS` topics.
    assert_nothing(&mut all);
    engine.shutdown();
}

// ---- Shared subscriptions -----------------------------------------------------------------------

#[test]
fn shared_subscription_spreads_messages_over_the_group() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut p = client(addr, "p");
    p.publish("jobs/old", b"retained", 0, true).unwrap();
    let mut workers: Vec<Client> = (0..3).map(|i| client(addr, &format!("w{i}"))).collect();
    for w in workers.iter_mut() {
        w.subscribe_qos(&["$share/workers/jobs/#"], 1).unwrap();
    }
    let mut monitor = client(addr, "monitor");
    monitor.subscribe(&["jobs/#"]).unwrap();
    assert_eq!(
        monitor.next_publish().unwrap(),
        Incoming::Publish { topic: "jobs/old".into(), payload: b"retained".to_vec(), qos: 0, retain: true }
    );
    std::thread::sleep(Duration::from_millis(50));
    let n = 300u32;
    for i in 0..n {
        p.publish("jobs/new", &i.to_be_bytes(), 1, false).unwrap();
    }
    for _ in 0..n {
        monitor.next_publish().unwrap();
    }
    let mut seen = vec![0u32; n as usize];
    let mut per_worker = Vec::new();
    for w in workers.iter_mut() {
        w.set_timeout(Some(Duration::from_millis(300))).unwrap();
        let mut got = 0;
        while let Ok(Incoming::Publish { payload, retain, .. }) = w.next_publish() {
            assert!(!retain, "no retained message for a shared subscription");
            seen[u32::from_be_bytes(payload[..4].try_into().unwrap()) as usize] += 1;
            got += 1;
        }
        per_worker.push(got);
    }
    assert!(seen.iter().all(|&c| c == 1), "each message to exactly one worker");
    assert!(per_worker.iter().all(|&c| c >= n / 4), "{per_worker:?}");
    engine.shutdown();
}

// ---- Authentication and ACL ---------------------------------------------------------------------

#[test]
fn credentials_and_acl_are_enforced() {
    let _g = serial();
    let passwd = format!("alice:wonder\n{}\nadmin:root\n", croisette::auth::hash_password("bob", "builder"));
    let acl = "user alice\ntopic readwrite sensors/#\nuser bob\ntopic write cmd/#\nuser admin\ntopic #\n";
    let auth = croisette::auth::Auth::from_strs(&passwd, Some(acl), false).unwrap();
    let config = Config { cores: vec![1, 2], auth: Some(std::sync::Arc::new(auth)), ..Config::default() };
    let (engine, addr) = start_with(config);
    let login = |user: &'static str, password: &'static [u8]| {
        let c = Connect {
            level: 4,
            client_id: user,
            keep_alive: 60,
            clean_session: true,
            username: Some(user),
            password: Some(password),
            ..Connect::default()
        };
        Client::connect_with(addr, &c).map(|(client, _)| client)
    };
    let refused = |r: std::io::Result<Client>| r.err().map(|e| e.to_string());
    assert_eq!(refused(Client::connect(addr, "anon")), Some("CONNACK code 5".into()));
    assert_eq!(refused(login("alice", b"nope")), Some("CONNACK code 4".into()));
    assert_eq!(refused(login("bob", b"builder")), None);
    let mut alice = login("alice", b"wonder").unwrap();
    assert_eq!(alice.subscribe_qos(&["sensors/#", "secret/#"], 1).unwrap(), vec![1, 0x80]);
    let mut admin = login("admin", b"root").unwrap();
    admin.subscribe(&["#"]).unwrap();
    admin.set_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut bob = login("bob", b"builder").unwrap();
    bob.publish("secret/x", b"denied", 1, false).unwrap(); // acknowledged, not routed
    bob.publish("cmd/go", b"ok", 1, false).unwrap();
    assert_eq!(admin.next_publish().unwrap(), publish_msg("cmd/go", b"ok"));
    alice.publish("sensors/t", b"21", 0, false).unwrap();
    assert_eq!(admin.next_publish().unwrap(), publish_msg("sensors/t", b"21"));
    alice.set_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(alice.next_publish().unwrap(), publish_msg("sensors/t", b"21"));
    assert_nothing(&mut admin);
    engine.shutdown();
}

// ---- TLS ----------------------------------------------------------------------------------------

type TlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

fn tls_connect(addr: SocketAddr) -> TlsStream {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut &include_bytes!("data/ca.pem")[..]) {
        roots.add(cert.unwrap()).unwrap();
    }
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let conn = rustls::ClientConnection::new(std::sync::Arc::new(config), name).unwrap();
    let tcp = TcpStream::connect(addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    rustls::StreamOwned::new(conn, tcp)
}

/// Reads packets until one PUBLISH, answering nothing (QoS 0 subscriptions).
fn tls_next_publish(s: &mut TlsStream, buf: &mut Vec<u8>) -> (String, Vec<u8>) {
    loop {
        if let Some((p, len)) = codec::parse(buf).unwrap() {
            let got = match p {
                codec::Packet::Publish { topic, payload, .. } => Some((topic.to_string(), payload.to_vec())),
                _ => None,
            };
            buf.drain(..len);
            if let Some(g) = got {
                return g;
            }
            continue;
        }
        let mut chunk = [0u8; 16384];
        let n = s.read(&mut chunk).unwrap();
        assert!(n > 0, "closed");
        buf.extend_from_slice(&chunk[..n]);
    }
}

#[test]
fn tls_clients_talk_with_tcp_clients() {
    let _g = serial();
    let tls = croisette::tls::server_config_from_pem(include_bytes!("data/cert.pem"), include_bytes!("data/key.pem"))
        .unwrap();
    let tls_port = free_port();
    let (engine, addr) = start_with(Config { cores: vec![1, 2], tls: Some(tls), tls_port, ..Config::default() });
    let tls_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, tls_port));
    // TLS subscriber, TCP publisher.
    let mut sub = tls_connect(tls_addr);
    let mut b = Vec::new();
    codec::connect(&mut b, "tls-sub", 60, true);
    codec::subscribe(&mut b, 1, &[("secure/#", 0)]);
    sub.write_all(&b).unwrap();
    let mut rx = Vec::new();
    std::thread::sleep(Duration::from_millis(200));
    let mut p = client(addr, "p");
    let big: Vec<u8> = (0..100_000u32).map(|i| i as u8).collect();
    p.publish("secure/a", b"hello", 1, false).unwrap();
    p.publish("secure/big", &big, 1, false).unwrap();
    assert_eq!(tls_next_publish(&mut sub, &mut rx), ("secure/a".into(), b"hello".to_vec()));
    assert_eq!(tls_next_publish(&mut sub, &mut rx), ("secure/big".into(), big.clone()));
    // TLS publisher, TCP subscriber.
    let mut s = client(addr, "s");
    s.subscribe(&["from/tls"]).unwrap();
    let mut publ = tls_connect(tls_addr);
    let mut b = Vec::new();
    codec::connect(&mut b, "tls-pub", 60, true);
    codec::publish(&mut b, "from/tls", &big, 0, false, None);
    publ.write_all(&b).unwrap();
    assert_eq!(s.next_publish().unwrap(), publish_msg("from/tls", &big));
    // Plain MQTT on the TLS port is not TLS: closed.
    let mut raw = TcpStream::connect(tls_addr).unwrap();
    let mut b = Vec::new();
    codec::connect(&mut b, "confused", 60, true);
    raw.write_all(&b).unwrap();
    assert_closed_by_broker(&mut raw, Duration::from_secs(2));
    engine.shutdown();
}

// ---- Persistence --------------------------------------------------------------------------------

#[test]
fn retained_and_sessions_survive_a_restart() {
    let _g = serial();
    let dir = std::env::temp_dir().join(format!("croisette-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = |cores: Vec<usize>| Config {
        cores,
        persist_dir: Some(dir.clone()),
        autosave: Duration::from_secs(1),
        ..Config::default()
    };
    let (engine, addr) = start_with(config(vec![1, 2]));
    let (mut dev, _) = persistent(addr, "dev");
    dev.subscribe_qos(&["p/#"], 1).unwrap();
    dev.disconnect().unwrap();
    let (mut online, _) = persistent(addr, "online");
    online.subscribe_qos(&["o/#"], 1).unwrap(); // stays connected until the broker stops
    let mut p = client(addr, "p");
    p.publish("r/x", b"kept", 1, true).unwrap();
    p.publish("p/a", b"queued", 1, false).unwrap();
    // The periodic snapshot has the retained message.
    std::thread::sleep(Duration::from_millis(2500));
    let (saved, _) = croisette::persist::load_dir(&dir).unwrap();
    assert!(saved.retained.iter().any(|r| r.topic == "r/x"), "{saved:?}");
    engine.shutdown(); // writes the final snapshots
    drop((p, online));

    // Restart on one core: the partitions are redistributed.
    let (engine, addr) = start_with(config(vec![1]));
    let mut s = client(addr, "s");
    s.subscribe(&["r/#"]).unwrap();
    assert_eq!(
        s.next_publish().unwrap(),
        Incoming::Publish { topic: "r/x".into(), payload: b"kept".to_vec(), qos: 0, retain: true }
    );
    let mut p = client(addr, "p");
    p.publish("p/b", b"after restart", 1, false).unwrap(); // routed to the restored session
    p.publish("o/a", b"for online", 1, false).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let (mut dev, present) = persistent(addr, "dev");
    assert!(present);
    assert_eq!(dev.next_publish().unwrap(), qos1_msg("p/a", b"queued", false));
    assert_eq!(dev.next_publish().unwrap(), qos1_msg("p/b", b"after restart", false));
    let (mut online, present) = persistent(addr, "online");
    assert!(present, "online sessions are saved with their subscriptions");
    assert_eq!(online.next_publish().unwrap(), qos1_msg("o/a", b"for online", false));
    engine.shutdown();
    let names: Vec<String> =
        std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
    assert_eq!(names, vec!["partition-1.db".to_string()], "the old partition is gone");
    std::fs::remove_dir_all(&dir).unwrap();
}

// ---- WebSocket ----------------------------------------------------------------------------------

/// A minimal MQTT-over-WebSocket client on any stream (TCP or TLS).
struct WsClient<S: Read + Write> {
    s: S,
    frames: Vec<u8>, // bytes of server frames not decoded yet
    mqtt: Vec<u8>,   // MQTT bytes from the frames
}

impl<S: Read + Write> WsClient<S> {
    fn new(mut s: S) -> Self {
        let req = "GET /mqtt HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n";
        s.write_all(req.as_bytes()).unwrap();
        let mut c = WsClient { s, frames: Vec::new(), mqtt: Vec::new() };
        while !c.frames.windows(4).any(|w| w == b"\r\n\r\n") {
            c.read_more();
        }
        let end = c.frames.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let response = String::from_utf8_lossy(&c.frames[..end]).to_string();
        assert!(response.starts_with("HTTP/1.1 101"), "{response}");
        assert!(response.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        c.frames.drain(..end);
        c
    }

    fn read_more(&mut self) {
        let mut chunk = [0u8; 65536];
        let n = self.s.read(&mut chunk).unwrap();
        assert!(n > 0, "closed");
        self.frames.extend_from_slice(&chunk[..n]);
    }

    fn send(&mut self, mqtt: &[u8]) {
        let mask = [9u8, 8, 7, 6];
        let mut f = vec![0x82];
        match mqtt.len() {
            n if n < 126 => f.push(0x80 | n as u8),
            n if n <= 0xffff => {
                f.push(0x80 | 126);
                f.extend_from_slice(&(n as u16).to_be_bytes());
            }
            n => {
                f.push(0x80 | 127);
                f.extend_from_slice(&(n as u64).to_be_bytes());
            }
        }
        f.extend_from_slice(&mask);
        f.extend(mqtt.iter().enumerate().map(|(i, x)| x ^ mask[i % 4]));
        self.s.write_all(&f).unwrap();
    }

    fn next_publish(&mut self) -> (String, Vec<u8>) {
        loop {
            if let Some((p, len)) = codec::parse(&self.mqtt).unwrap() {
                let got = match p {
                    codec::Packet::Publish { topic, payload, .. } => Some((topic.to_string(), payload.to_vec())),
                    _ => None,
                };
                self.mqtt.drain(..len);
                if let Some(g) = got {
                    return g;
                }
                continue;
            }
            // Decode one server frame (unmasked), or read more.
            let b = &self.frames;
            let header = if b.len() >= 2 {
                match b[1] & 0x7f {
                    126 if b.len() >= 4 => Some((u16::from_be_bytes([b[2], b[3]]) as usize, 4)),
                    127 if b.len() >= 10 => Some((u64::from_be_bytes(b[2..10].try_into().unwrap()) as usize, 10)),
                    126 | 127 => None,
                    n => Some((n as usize, 2)),
                }
            } else {
                None
            };
            match header {
                Some((len, at)) if self.frames.len() >= at + len => {
                    assert_eq!(self.frames[0] & 0x0f, 2, "binary frame");
                    let payload: Vec<u8> = self.frames[at..at + len].to_vec();
                    self.frames.drain(..at + len);
                    self.mqtt.extend_from_slice(&payload);
                }
                _ => self.read_more(),
            }
        }
    }
}

#[test]
fn websocket_and_secure_websocket_clients() {
    let _g = serial();
    let tls = croisette::tls::server_config_from_pem(include_bytes!("data/cert.pem"), include_bytes!("data/key.pem"))
        .unwrap();
    let (ws_port, wss_port) = (free_port(), free_port());
    let config = Config {
        cores: vec![1, 2],
        tls: Some(tls),
        tls_port: free_port(),
        ws_port: Some(ws_port),
        wss_port: Some(wss_port),
        ..Config::default()
    };
    let (engine, addr) = start_with(config);
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, ws_port)).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut ws = WsClient::new(tcp);
    let mut wss = WsClient::new(tls_connect(SocketAddr::from((Ipv4Addr::LOCALHOST, wss_port))));
    for (c, id) in [(&mut ws as &mut dyn WsSend, "ws"), (&mut wss as &mut dyn WsSend, "wss")] {
        let mut b = Vec::new();
        codec::connect(&mut b, id, 60, true);
        codec::subscribe(&mut b, 1, &[("web/#", 0)]);
        c.send_mqtt(&b);
    }
    std::thread::sleep(Duration::from_millis(200));
    let mut p = client(addr, "p");
    let big: Vec<u8> = (0..70_000u32).map(|i| i as u8).collect(); // a 64-bit length frame
    p.publish("web/a", b"hi", 1, false).unwrap();
    p.publish("web/big", &big, 1, false).unwrap();
    assert_eq!(ws.next_publish(), ("web/a".into(), b"hi".to_vec()));
    assert_eq!(ws.next_publish(), ("web/big".into(), big.clone()));
    assert_eq!(wss.next_publish(), ("web/a".into(), b"hi".to_vec()));
    assert_eq!(wss.next_publish(), ("web/big".into(), big.clone()));
    // WebSocket publisher, TCP subscriber; the packet split over two frames.
    let mut s = client(addr, "s");
    s.subscribe(&["from/ws"]).unwrap();
    let mut b = Vec::new();
    codec::publish(&mut b, "from/ws", b"split", 0, false, None);
    let (x, y) = b.split_at(3);
    ws.send(x);
    ws.send(y);
    assert_eq!(s.next_publish().unwrap(), publish_msg("from/ws", b"split"));
    engine.shutdown();
}

trait WsSend {
    fn send_mqtt(&mut self, b: &[u8]);
}

impl<S: Read + Write> WsSend for WsClient<S> {
    fn send_mqtt(&mut self, b: &[u8]) {
        self.send(b)
    }
}

// ---- MQTT 5 -------------------------------------------------------------------------------------

fn v5(addr: SocketAddr, id: &str, session_expiry: Option<u32>, clean: bool) -> (Client, bool) {
    let c =
        Connect { level: 5, client_id: id, keep_alive: 60, clean_session: clean, session_expiry, ..Connect::default() };
    let (client, present) = Client::connect_with(addr, &c).unwrap();
    client.set_timeout(Some(Duration::from_secs(5))).unwrap();
    (client, present)
}

fn content_type(ct: &str) -> Vec<u8> {
    let mut p = Vec::new();
    codec::prop_str(&mut p, 0x03, ct);
    p
}

#[test]
fn mqtt5_and_mqtt311_clients_share_messages() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let (mut s5, _) = v5(addr, "s5", None, true);
    assert_eq!(s5.subscribe_qos(&["m/#"], 2).unwrap(), vec![2]);
    let mut s3 = client(addr, "s3");
    s3.subscribe_qos(&["m/#"], 1).unwrap();
    let (mut p5, _) = v5(addr, "p5", None, true);
    p5.queue_publish_with("m/a", b"json", 1, false, &content_type("application/json"));
    p5.flush().unwrap();
    assert_eq!(s5.next_publish().unwrap(), qos1_msg("m/a", b"json", false));
    assert_eq!(codec::prop_value(&s5.last_props, 0x03), Some(&b"application/json"[..]), "properties forwarded");
    assert_eq!(s3.next_publish().unwrap(), qos1_msg("m/a", b"json", false));
    let mut p3 = client(addr, "p3");
    p3.publish("m/b", b"old", 0, false).unwrap();
    assert_eq!(s5.next_publish().unwrap(), publish_msg("m/b", b"old"));
    assert!(s5.last_props.is_empty());
    // The broker assigns a client id to an MQTT 5 client without one.
    let (anon, _) = v5(addr, "", None, true);
    assert!(anon.assigned_client_id().is_some_and(|id| id.starts_with("croisette-")), "{:?}", anon.connack_props);
    assert_eq!(codec::prop_value(&anon.connack_props, codec::prop::TOPIC_ALIAS_MAXIMUM), Some(&[0, 64][..]));
    // Topic aliases from the client.
    let mut alias = Vec::new();
    codec::prop_u16(&mut alias, codec::prop::TOPIC_ALIAS, 7);
    p5.queue_publish_with("m/aliased", b"1", 0, false, &alias);
    p5.queue_publish_with("", b"2", 0, false, &alias);
    p5.flush().unwrap();
    assert_eq!(s5.next_publish().unwrap(), publish_msg("m/aliased", b"1"));
    assert_eq!(s5.next_publish().unwrap(), publish_msg("m/aliased", b"2"));
    engine.shutdown();
}

#[test]
fn mqtt5_subscription_options() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut p = client(addr, "p");
    p.publish("keep", b"retained", 0, true).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let (mut c, _) = v5(addr, "c", None, true);
    // No Local: its own messages do not come back.
    c.subscribe_with(&[("nl", 0x04)]).unwrap();
    c.publish("nl", b"mine", 0, false).unwrap();
    p.publish("nl", b"theirs", 0, false).unwrap();
    assert_eq!(c.next_publish().unwrap(), publish_msg("nl", b"theirs"));
    // Retain Handling 2: no retained message; 1: only for a new subscription.
    c.subscribe_with(&[("keep", 0x20)]).unwrap();
    assert_nothing(&mut c);
    c.unsubscribe(&["keep"]).unwrap();
    c.subscribe_with(&[("keep", 0x10)]).unwrap();
    assert_eq!(
        c.next_publish().unwrap(),
        Incoming::Publish { topic: "keep".into(), payload: b"retained".to_vec(), qos: 0, retain: true }
    );
    c.subscribe_with(&[("keep", 0x10)]).unwrap();
    assert_nothing(&mut c);
    // Retain As Published: the retain flag of a live message is kept.
    let (mut rap, _) = v5(addr, "rap", None, true);
    rap.subscribe_with(&[("live", 0x08 | 0x20)]).unwrap();
    let (mut plain, _) = v5(addr, "plain", None, true);
    plain.subscribe_with(&[("live", 0x20)]).unwrap();
    p.publish("live", b"x", 0, true).unwrap();
    let live = |retain| Incoming::Publish { topic: "live".into(), payload: b"x".to_vec(), qos: 0, retain };
    assert_eq!(rap.next_publish().unwrap(), live(true));
    assert_eq!(plain.next_publish().unwrap(), live(false));
    engine.shutdown();
}

#[test]
fn mqtt5_expiry_and_flow_control() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let expiry = |secs: u32| {
        let mut p = Vec::new();
        codec::prop_u32(&mut p, codec::prop::MESSAGE_EXPIRY, secs);
        p
    };
    // Session expiry: an offline session with a 1 s expiry is gone after it.
    let (mut dev, _) = v5(addr, "dev", Some(1), true);
    dev.subscribe_qos(&["e/#"], 1).unwrap();
    dev.disconnect().unwrap();
    let (mut p, _) = v5(addr, "p", None, true);
    p.queue_publish_with("e/short", b"expires", 1, false, &expiry(1));
    p.queue_publish_with("e/long", b"stays", 1, false, &expiry(600));
    p.queue_publish_with("ret", b"old", 0, true, &expiry(1));
    p.flush().unwrap();
    let (mut keep, _) = v5(addr, "keep", Some(600), true);
    keep.subscribe_qos(&["e/#"], 1).unwrap();
    keep.disconnect().unwrap();
    p.queue_publish_with("e/short", b"expires", 1, false, &expiry(1));
    p.queue_publish_with("e/long", b"stays", 1, false, &expiry(600));
    p.flush().unwrap();
    std::thread::sleep(Duration::from_millis(3200));
    let (_dev, present) = v5(addr, "dev", Some(1), false);
    assert!(!present, "the session expired");
    // Message expiry: the queued message past its expiry is dropped, the other keeps the rest
    // of its interval.
    let (mut keep, present) = v5(addr, "keep", Some(600), false);
    assert!(present);
    assert_eq!(keep.next_publish().unwrap(), qos1_msg("e/long", b"stays", false));
    assert!(keep.last_expiry.is_some_and(|e| (590..600).contains(&e)), "{:?}", keep.last_expiry);
    assert_nothing(&mut keep);
    let mut late = client(addr, "late");
    late.subscribe(&["ret"]).unwrap();
    assert_nothing(&mut late); // the retained message expired
                               // Receive Maximum: at most 2 unacknowledged QoS 1 messages to this client.
    let c =
        Connect { level: 5, client_id: "slow", clean_session: true, receive_maximum: Some(2), ..Connect::default() };
    let (mut slow, _) = Client::connect_with(addr, &c).unwrap();
    slow.set_timeout(Some(Duration::from_secs(5))).unwrap();
    slow.auto_ack = false;
    slow.subscribe_qos(&["w"], 1).unwrap();
    for i in 0..5u8 {
        p.publish("w", &[i], 1, false).unwrap();
    }
    assert_eq!(slow.next_publish().unwrap(), qos1_msg("w", &[0], false));
    assert_eq!(slow.next_publish().unwrap(), qos1_msg("w", &[1], false));
    assert_nothing(&mut slow);
    // Maximum Packet Size: larger messages are not sent to this client.
    let c = Connect {
        level: 5,
        client_id: "small",
        clean_session: true,
        maximum_packet_size: Some(100),
        ..Connect::default()
    };
    let (mut small, _) = Client::connect_with(addr, &c).unwrap();
    small.set_timeout(Some(Duration::from_secs(5))).unwrap();
    small.subscribe(&["sz"]).unwrap();
    p.publish("sz", &[0u8; 200], 0, false).unwrap();
    p.publish("sz", b"fits", 0, false).unwrap();
    assert_eq!(small.next_publish().unwrap(), publish_msg("sz", b"fits"));
    engine.shutdown();
}

#[test]
fn mqtt5_disconnects_with_reason_codes() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    // Takeover: the first connection gets DISCONNECT Session taken over (0x8E).
    let (mut first, _) = v5(addr, "twin", Some(60), true);
    let (_second, present) = v5(addr, "twin", Some(60), false);
    assert!(present);
    match first.next_publish().unwrap() {
        Incoming::Other(p) => assert!(p.contains("Disconnect { reason: 142"), "{p}"),
        other => panic!("{other:?}"),
    }
    // A DISCONNECT that sets the session expiry to 0 ends the session.
    let (dev, _) = v5(addr, "dev", Some(600), true);
    let mut props = Vec::new();
    codec::prop_u32(&mut props, codec::prop::SESSION_EXPIRY, 0);
    dev.disconnect_with(0, &props).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let (_dev, present) = v5(addr, "dev", Some(600), false);
    assert!(!present);
    // Reason 0x04 (disconnect with will message) publishes the will.
    let mut watcher = client(addr, "watcher");
    watcher.subscribe(&["wills/#"]).unwrap();
    let c = Connect {
        level: 5,
        client_id: "w",
        clean_session: true,
        will: Some(Will { topic: "wills/w", payload: b"bye", ..Will::default() }),
        ..Connect::default()
    };
    let (w, _) = Client::connect_with(addr, &c).unwrap();
    w.disconnect_with(0x04, &[]).unwrap();
    assert_eq!(watcher.next_publish().unwrap(), publish_msg("wills/w", b"bye"));
    engine.shutdown();
}

#[test]
fn mqtt5_will_delay() {
    let _g = serial();
    let (engine, addr) = start(vec![1, 2]);
    let mut watcher = client(addr, "watcher");
    watcher.subscribe(&["wills/#"]).unwrap();
    let delayed = |id: &'static str, delay: u32| Connect {
        level: 5,
        client_id: id,
        clean_session: false,
        session_expiry: Some(600),
        will: Some(Will { topic: "wills/x", payload: id.as_bytes(), delay: Some(delay), ..Will::default() }),
        ..Connect::default()
    };
    // Published after its delay.
    let (late, _) = Client::connect_with(addr, &delayed("late", 2)).unwrap();
    let t = Instant::now();
    drop(late);
    assert_eq!(watcher.next_publish().unwrap(), publish_msg("wills/x", b"late"));
    assert!(t.elapsed() >= Duration::from_millis(1500), "{:?}", t.elapsed());
    // Not published if the client comes back within the delay.
    let (back, _) = Client::connect_with(addr, &delayed("back", 3)).unwrap();
    drop(back);
    std::thread::sleep(Duration::from_millis(300));
    let (_back, present) = Client::connect_with(addr, &delayed("back", 3)).unwrap();
    assert!(present);
    watcher.set_timeout(Some(Duration::from_millis(4500))).unwrap();
    assert!(watcher.next_publish().is_err(), "no will after a reconnection");
    engine.shutdown();
}
