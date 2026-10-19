//! End-to-end tests of the broker on two cores, with real TCP clients.

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use cannes::CoreMode;
use cannes_mqtt::broker::{self, Config};
use cannes_mqtt::client::{Client, Incoming};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn start(cores: Vec<usize>) -> (cannes::Engine, SocketAddr) {
    let port = free_port();
    let config = Config { ip: Ipv4Addr::LOCALHOST, port, cores, mode: CoreMode::Spin };
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
