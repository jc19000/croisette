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
    let port = free_port();
    let config =
        Config { ip: Ipv4Addr::LOCALHOST, port, cores, mode: CoreMode::Spin, connect_timeout: Duration::from_secs(1) };
    let engine = broker::start(&config).unwrap();
    (engine, SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

fn client(addr: SocketAddr, id: &str) -> Client {
    let c = Client::connect(addr, id).unwrap();
    c.set_timeout(Some(Duration::from_secs(5))).unwrap();
    c
}

fn publish_msg(topic: &str, payload: &[u8]) -> Incoming {
    Incoming::Publish { topic: topic.into(), payload: payload.into(), retain: false }
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
        Incoming::Publish { topic: "status/door".into(), payload: b"open".to_vec(), retain: true }
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
        Incoming::Publish { topic: "status/dev3".into(), payload: b"gone".to_vec(), retain: true }
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
