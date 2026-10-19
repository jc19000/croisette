//! MQTT broker on cannes.
//!
//! usage: cannes-mqtt [--port 1883] [--cores 1,2] [--mode spin|adaptive]

use cannes_mqtt::broker::{self, Config};

fn main() {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--port" => config.port = value.parse().expect("port"),
            "--cores" => config.cores = value.split(',').map(|c| c.parse().expect("core")).collect(),
            "--mode" => {
                config.mode = match value.as_str() {
                    "spin" => cannes::CoreMode::Spin,
                    "adaptive" => broker::adaptive(),
                    other => panic!("unknown mode {other}"),
                }
            }
            other => panic!("unknown option {other}"),
        }
    }
    let _engine = broker::start(&config).expect("broker start");
    eprintln!("cannes-mqtt listening on port {} on cores {:?} ({:?})", config.port, config.cores, config.mode);
    loop {
        std::thread::park();
    }
}
