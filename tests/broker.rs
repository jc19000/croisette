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
        will: Some(Will { topic, payload: b"gone", qos: 0, retain }),
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
    let bad_level = Connect { level: 5, client_id: "x", clean_session: true, ..Connect::default() };
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
    assert!(redelivered > 0, "reconnections happened with unacknowledged messages");
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
