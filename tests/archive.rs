//! The archive tier end to end: messages published to the broker, read back from the Iceberg
//! table (SQLite catalog, local warehouse). Needs `--features iceberg`.
#![cfg(feature = "iceberg")]

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::time::Duration;

use cannes::CoreMode;
use croisette::archive::{self, ArchiveConfig, Archiver};
use croisette::broker::{self, Config};
use croisette::client::Client;

#[test]
fn published_messages_land_in_an_iceberg_table() {
    let dir = std::env::temp_dir().join(format!("croisette-archive-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let config = ArchiveConfig {
        filters: vec!["sensors/#".into()],
        catalog: format!("sqlite:{}/catalog.db", dir.display()),
        warehouse: format!("file://{}/warehouse", dir.display()),
        commit_interval: Duration::from_millis(500),
        ..ArchiveConfig::default()
    };
    let archiver = Archiver::start(&config).unwrap();
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let broker_config = Config {
        ip: Ipv4Addr::LOCALHOST,
        port,
        cores: vec![1, 2],
        mode: CoreMode::Spin,
        archive: Some(archiver.sender()),
        ..Config::default()
    };
    let engine = broker::start(&broker_config).unwrap();
    let mut p = Client::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port)), "p").unwrap();
    for i in 0..500u32 {
        p.publish(&format!("sensors/{}", i % 5), &i.to_be_bytes(), 0, false).unwrap();
        p.publish("other/x", b"not archived", 0, false).unwrap();
    }
    p.publish("sensors/r", b"retained", 1, true).unwrap();
    std::thread::sleep(Duration::from_millis(2500)); // the cores send every second
    assert!(archiver.stats().commits.load(std::sync::atomic::Ordering::Relaxed) >= 1, "periodic commit");
    for i in 500..600u32 {
        p.publish(&format!("sensors/{}", i % 5), &i.to_be_bytes(), 1, false).unwrap();
    }
    broker::stop(engine, &broker_config);
    let stats = (
        archiver.stats().committed.load(std::sync::atomic::Ordering::Relaxed),
        archiver.stats().dropped.load(std::sync::atomic::Ordering::Relaxed),
    );
    archiver.stop();
    assert_eq!(stats, (601, 0));

    let mut rows = archive::read(&config).unwrap();
    rows.sort_by_key(|r| r.ts_us); // files come in any order
    assert_eq!(rows.len(), 601);
    assert!(rows.iter().all(|r| r.topic.starts_with("sensors/") && &*r.client_id == "p"));
    let retained: Vec<_> = rows.iter().filter(|r| r.retain).collect();
    assert_eq!(retained.len(), 1);
    assert_eq!((&*retained[0].topic, &retained[0].payload[..], retained[0].qos), ("sensors/r", &b"retained"[..], 1));
    // Every message once; within a topic, in publish order by time.
    let mut values: Vec<u32> =
        rows.iter().filter(|r| !r.retain).map(|r| u32::from_be_bytes(r.payload[..].try_into().unwrap())).collect();
    for t in 0..5 {
        let topic = format!("sensors/{t}");
        let mine: Vec<u32> = rows
            .iter()
            .filter(|r| *r.topic == topic)
            .map(|r| u32::from_be_bytes(r.payload[..].try_into().unwrap()))
            .collect();
        assert!(mine.windows(2).all(|w| w[0] < w[1]), "{topic}: {mine:?}");
    }
    values.sort_unstable();
    assert_eq!(values, (0..600).collect::<Vec<_>>());
    std::fs::remove_dir_all(&dir).unwrap();
}
