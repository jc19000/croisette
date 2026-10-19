//! MQTT broker on cannes.
//!
//! usage: croisette [--port 1883] [--cores 1,2] [--mode MODE] [--max-queued BYTES]
//!        [--max-packet BYTES] [--max-inflight MESSAGES] [--connect-timeout SECONDS]
//!        [--sys-interval SECONDS] [--passwd FILE] [--acl FILE] [--allow-anonymous true|false]
//!        [--tls-cert PEM --tls-key PEM [--tls-port 8883]] [--persist-dir DIR [--autosave SECONDS]]
//!        croisette hash-password USER PASSWORD     (prints a password file line)
//!
//! MODE: `spin`, `adaptive` (2000 idle loops, then 50 µs sleeps), `adaptive:<loops>:<µs>`,
//! `sleep:<µs>` (wait in io_uring after each idle iteration, an I/O completion ends the wait).
//! Default: `sleep:100000` on one core, `adaptive` on several.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cannes::CoreMode;
use croisette::auth::Auth;
use croisette::broker::{self, Config};

fn main() {
    let mut config = Config::default();
    let all: Vec<String> = std::env::args().collect();
    if all.get(1).map(String::as_str) == Some("hash-password") {
        let (Some(user), Some(password)) = (all.get(2), all.get(3)) else {
            panic!("usage: croisette hash-password USER PASSWORD")
        };
        println!("{}", croisette::auth::hash_password(user, password));
        return;
    }
    let (mut passwd, mut acl, mut anonymous) = (None, None, true);
    let mut mode = None;
    let (mut tls_cert, mut tls_key) = (None, None);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--port" => config.port = value.parse().expect("port"),
            "--cores" => config.cores = value.split(',').map(|c| c.parse().expect("core")).collect(),
            "--mode" => {
                let us = |v: &str| Duration::from_micros(v.parse().expect("microseconds"));
                let parts: Vec<&str> = value.split(':').collect();
                mode = Some(match parts[..] {
                    ["spin"] => CoreMode::Spin,
                    ["adaptive"] => broker::adaptive(),
                    ["adaptive", loops, sleep] => {
                        CoreMode::Adaptive { idle_loops: loops.parse().expect("loops"), sleep: us(sleep) }
                    }
                    ["sleep", sleep] => CoreMode::Sleep(us(sleep)),
                    _ => panic!("unknown mode {value}"),
                })
            }
            "--max-queued" => config.max_queued = value.parse().expect("bytes"),
            "--max-packet" => config.max_packet = value.parse().expect("bytes"),
            "--connect-timeout" => config.connect_timeout = Duration::from_secs(value.parse().expect("seconds")),
            "--max-inflight" => config.max_inflight = value.parse().expect("messages"),
            "--sys-interval" => config.sys_interval = Duration::from_secs(value.parse().expect("seconds")),
            "--passwd" => passwd = Some(PathBuf::from(value)),
            "--tls-cert" => tls_cert = Some(PathBuf::from(value)),
            "--tls-key" => tls_key = Some(PathBuf::from(value)),
            "--tls-port" => config.tls_port = value.parse().expect("port"),
            "--persist-dir" => config.persist_dir = Some(PathBuf::from(value)),
            "--autosave" => config.autosave = Duration::from_secs(value.parse().expect("seconds")),
            "--acl" => acl = Some(PathBuf::from(value)),
            "--allow-anonymous" => anonymous = value.parse().expect("true or false"),
            other => panic!("unknown option {other}"),
        }
    }
    if passwd.is_some() || acl.is_some() || !anonymous {
        let auth = Auth::load(passwd.as_deref(), acl.as_deref(), anonymous).expect("password or ACL file");
        config.auth = Some(Arc::new(auth));
    }
    // Default: one core waits in io_uring (nothing else can wake it, so no busy wait); several
    // cores spin while busy and sleep briefly when idle, to see each other's events quickly.
    config.mode = mode.unwrap_or(if config.cores.len() == 1 {
        CoreMode::Sleep(Duration::from_millis(100))
    } else {
        broker::adaptive()
    });
    match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => {
            config.tls = Some(croisette::tls::server_config(&cert, &key).expect("TLS certificate"))
        }
        (None, None) => {}
        _ => panic!("--tls-cert and --tls-key go together"),
    }
    croisette::net::block_termination_signals();
    let engine = broker::start(&config).expect("broker start");
    let tls = config.tls.as_ref().map(|_| format!(", TLS on port {}", config.tls_port)).unwrap_or_default();
    eprintln!("croisette listening on port {}{tls} on cores {:?} ({:?})", config.port, config.cores, config.mode);
    let sig = croisette::net::wait_for_termination();
    eprintln!("croisette: signal {sig}, stopping");
    engine.shutdown(); // closes the connections and writes the snapshots
}
