//! MQTT broker on cannes.
//!
//! usage: croisette [--port 1883] [--cores 1,2] [--mode MODE] [--max-queued BYTES]
//!        [--max-packet BYTES] [--max-inflight MESSAGES] [--connect-timeout SECONDS]
//!        [--sys-interval SECONDS]
//!
//! MODE: `spin`, `adaptive` (2000 idle loops, then 50 µs sleeps), `adaptive:<loops>:<µs>`,
//! `sleep:<µs>` (wait in io_uring after each idle iteration, an I/O completion ends the wait).

use std::time::Duration;

use cannes::CoreMode;
use croisette::broker::{self, Config};

fn main() {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--port" => config.port = value.parse().expect("port"),
            "--cores" => config.cores = value.split(',').map(|c| c.parse().expect("core")).collect(),
            "--mode" => {
                let us = |v: &str| Duration::from_micros(v.parse().expect("microseconds"));
                let parts: Vec<&str> = value.split(':').collect();
                config.mode = match parts[..] {
                    ["spin"] => CoreMode::Spin,
                    ["adaptive"] => broker::adaptive(),
                    ["adaptive", loops, sleep] => {
                        CoreMode::Adaptive { idle_loops: loops.parse().expect("loops"), sleep: us(sleep) }
                    }
                    ["sleep", sleep] => CoreMode::Sleep(us(sleep)),
                    _ => panic!("unknown mode {value}"),
                }
            }
            "--max-queued" => config.max_queued = value.parse().expect("bytes"),
            "--max-packet" => config.max_packet = value.parse().expect("bytes"),
            "--connect-timeout" => config.connect_timeout = Duration::from_secs(value.parse().expect("seconds")),
            "--max-inflight" => config.max_inflight = value.parse().expect("messages"),
            "--sys-interval" => config.sys_interval = Duration::from_secs(value.parse().expect("seconds")),
            other => panic!("unknown option {other}"),
        }
    }
    let _engine = broker::start(&config).expect("broker start");
    eprintln!("croisette listening on port {} on cores {:?} ({:?})", config.port, config.cores, config.mode);
    loop {
        std::thread::park();
    }
}
