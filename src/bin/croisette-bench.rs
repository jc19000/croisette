//! MQTT load generator for comparing brokers.
//!
//! Scenarios:
//! - `flow`: `--pubs` publisher connections send to `bench/<i>` at `--rate` msg/s in total
//!   (open loop, Poisson arrivals; 0 = as fast as possible), `--subs` subscriber connections
//!   subscribe to `--filter` (default `bench/#`, i.e. fan-in; `bench/0` with one publisher is a
//!   fan-out). Reports delivered msg/s, end-to-end latency, and the broker's CPU.
//! - `idle`: opens `--conns` connections that send nothing, reports the broker's memory per
//!   connection and its CPU while idle.
//!
//! The broker's CPU and memory come from /proc/<pid> (`--broker-pid`): CPU time from the
//! `schedstat` of all its threads, memory from `VmRSS`.
//!
//! usage: croisette-bench flow|idle --port P --broker-pid PID [--pubs N] [--subs N] [--rate R]
//!        [--secs S] [--payload B] [--filter F] [--threads T] [--qos Q] [--conns N] [--tls CA_FILE]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use croisette::client::Client;

static EPOCH: OnceLock<Instant> = OnceLock::new();

fn now_ns() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

// ---- Broker metrics from /proc -------------------------------------------------------------------

fn broker_cpu_ns(pid: u32) -> u64 {
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else { return 0 };
    tasks
        .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("schedstat")).ok())
        .filter_map(|s| s.split_whitespace().next()?.parse::<u64>().ok())
        .sum()
}

fn broker_rss_kb(pid: u32) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))?.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}

// ---- Latency histogram: 1 µs buckets up to 100 ms ----------------------------------------------

struct Histogram {
    buckets: Vec<u64>,
    count: u64,
}

impl Histogram {
    fn new() -> Self {
        Histogram { buckets: vec![0; 100_001], count: 0 }
    }

    fn record(&mut self, ns: u64) {
        let us = (ns / 1000).min(100_000) as usize;
        self.buckets[us] += 1;
        self.count += 1;
    }

    fn merge(&mut self, other: &Histogram) {
        for (a, b) in self.buckets.iter_mut().zip(&other.buckets) {
            *a += b;
        }
        self.count += other.count;
    }

    fn percentile(&self, p: f64) -> u64 {
        let target = ((p / 100.0) * self.count as f64).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (us, n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= target {
                return us as u64;
            }
        }
        100_000
    }
}

// ---- Arguments -----------------------------------------------------------------------------------

struct Args {
    scenario: String,
    opts: HashMap<String, String>,
}

impl Args {
    fn parse() -> Args {
        let mut it = std::env::args().skip(1);
        let scenario = it.next().expect("scenario: flow | idle");
        let mut opts = HashMap::new();
        while let Some(k) = it.next() {
            let v = it.next().unwrap_or_else(|| panic!("{k} needs a value"));
            opts.insert(k.trim_start_matches("--").to_string(), v);
        }
        Args { scenario, opts }
    }

    fn get<T: std::str::FromStr>(&self, key: &str, default: T) -> T {
        self.opts.get(key).map(|v| v.parse().unwrap_or_else(|_| panic!("bad --{key}"))).unwrap_or(default)
    }

    fn string(&self, key: &str, default: &str) -> String {
        self.opts.get(key).cloned().unwrap_or_else(|| default.to_string())
    }
}

/// `--tls CA_FILE`: connect over TLS, trusting that CA, to a certificate for `localhost`.
static TLS: OnceLock<Arc<croisette::client::ClientConfig>> = OnceLock::new();

fn connect_retry(addr: SocketAddr, id: &str) -> Client {
    for _ in 0..50 {
        let c = match TLS.get() {
            Some(config) => Client::connect_tls(addr, id, config.clone(), "localhost"),
            None => Client::connect(addr, id),
        };
        if let Ok(c) = c {
            return c;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("cannot connect {id} to {addr}");
}

// ---- flow ----------------------------------------------------------------------------------------

fn flow(args: &Args, addr: SocketAddr, pid: u32) {
    let pubs: usize = args.get("pubs", 10);
    let subs: usize = args.get("subs", 1);
    let rate: f64 = args.get("rate", 0.0);
    let secs: f64 = args.get("secs", 10.0);
    let warmup: f64 = args.get("warmup", 2.0);
    let payload_len: usize = args.get::<usize>("payload", 64).max(8);
    let filter = args.string("filter", "bench/#");
    let threads: usize = args.get("threads", 1);
    let qos: u8 = args.get("qos", 0);
    assert!(qos == 0 || TLS.get().is_none(), "--tls: QoS 0 only");

    // Subscribers first, each subscribed before anyone publishes.
    let sub_clients: Vec<Client> = (0..subs)
        .map(|i| {
            let mut c = connect_retry(addr, &format!("bench-sub-{i}"));
            c.subscribe_qos(&[&filter], qos).unwrap();
            c.stream.set_nonblocking(true).unwrap();
            c
        })
        .collect();
    thread::sleep(Duration::from_millis(200)); // subscriptions reach every router replica

    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU64::new(0));
    let (readers_n, writers_n) = (threads.min(subs).max(1), threads.min(pubs).max(1));
    let start = Arc::new(Barrier::new(readers_n + writers_n + 1));

    // Subscriber threads: spin over their non-blocking sockets.
    let mut groups: Vec<Vec<Client>> = (0..readers_n).map(|_| Vec::new()).collect();
    for (i, c) in sub_clients.into_iter().enumerate() {
        groups[i % readers_n].push(c);
    }
    let readers: Vec<_> = groups
        .into_iter()
        .map(|mine| {
            let (stop, measuring, start) = (stop.clone(), measuring.clone(), start.clone());
            thread::spawn(move || {
                let mut mine = mine;
                let mut hist = Histogram::new();
                let mut received = 0u64;
                start.wait();
                while !stop.load(Ordering::Relaxed) {
                    let on = measuring.load(Ordering::Relaxed);
                    for c in mine.iter_mut() {
                        let r = c.read_publishes(|p| {
                            if on {
                                let t0 = u64::from_le_bytes(p[..8].try_into().unwrap());
                                hist.record(now_ns().saturating_sub(t0));
                                received += 1;
                            }
                        });
                        if let Err(e) = r {
                            if e.kind() != std::io::ErrorKind::WouldBlock {
                                panic!("subscriber: {e}");
                            }
                        }
                    }
                }
                (hist, received)
            })
        })
        .collect();

    // Publisher threads: open loop at rate / threads each.
    let writers: Vec<_> = (0..writers_n)
        .map(|t| {
            let ids: Vec<usize> = (0..pubs).filter(|i| i % writers_n == t).collect();
            let (stop, measuring, start, sent) = (stop.clone(), measuring.clone(), start.clone(), sent.clone());
            thread::spawn(move || {
                let clients: Vec<(Client, String)> = ids
                    .iter()
                    .map(|i| (connect_retry(addr, &format!("bench-pub-{i}")), format!("bench/{i}")))
                    .collect();
                // PUBACKs of QoS 1 publishes are read and dropped by a helper thread.
                for (c, _) in clients.iter().filter(|_| qos > 0) {
                    let mut acks = c.stream.try_clone().unwrap();
                    thread::spawn(move || {
                        let mut b = [0u8; 4096];
                        while std::io::Read::read(&mut acks, &mut b).is_ok_and(|n| n > 0) {}
                    });
                }
                let mut clients = clients;
                let per_thread = rate / writers_n as f64;
                let mut payload = vec![0u8; payload_len];
                let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ t as u64;
                let mut next = 0usize;
                start.wait();
                let mut due = now_ns();
                while !stop.load(Ordering::Relaxed) {
                    let n = if per_thread > 0.0 {
                        // Every arrival due by now, timestamped with its scheduled time.
                        let now = now_ns();
                        let mut n = 0;
                        while due <= now && n < 256 {
                            let len = clients.len();
                            let (c, topic) = &mut clients[next % len];
                            payload[..8].copy_from_slice(&due.to_le_bytes());
                            c.queue_publish(topic, &payload, qos, false);
                            next += 1;
                            n += 1;
                            rng ^= rng << 13;
                            rng ^= rng >> 7;
                            rng ^= rng << 17;
                            let u = ((rng >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
                            due += (-u.ln() / per_thread * 1e9) as u64;
                        }
                        n
                    } else {
                        // As fast as possible: batches of 64 per connection.
                        let ts = now_ns();
                        payload[..8].copy_from_slice(&ts.to_le_bytes());
                        let len = clients.len();
                        let (c, topic) = &mut clients[next % len];
                        for _ in 0..64 {
                            c.queue_publish(topic, &payload, qos, false);
                        }
                        next += 1;
                        64
                    };
                    if n > 0 {
                        for (c, _) in clients.iter_mut() {
                            c.flush().unwrap();
                        }
                        if measuring.load(Ordering::Relaxed) {
                            sent.fetch_add(n as u64, Ordering::Relaxed);
                        }
                    } else {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    start.wait();
    thread::sleep(Duration::from_secs_f64(warmup));
    let (cpu0, t0) = (broker_cpu_ns(pid), Instant::now());
    measuring.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_secs_f64(secs));
    measuring.store(false, Ordering::Relaxed);
    let (cpu1, elapsed) = (broker_cpu_ns(pid), t0.elapsed());
    let rss = broker_rss_kb(pid);
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
    let mut hist = Histogram::new();
    let mut received = 0;
    for r in readers {
        let (h, n) = r.join().unwrap();
        hist.merge(&h);
        received += n;
    }
    let el = elapsed.as_secs_f64();
    let cpu_s = (cpu1 - cpu0) as f64 / 1e9;
    println!(
        "flow pubs={pubs} subs={subs} filter={filter} rate={rate} payload={payload_len} qos={qos} | sent/s={:.0} delivered/s={:.0} | broker cpu={:.2} cores, {:.2} cpu-s per million delivered, rss={} MB | latency us p50={} p99={} p99.9={} max={}",
        sent.load(Ordering::Relaxed) as f64 / el,
        received as f64 / el,
        cpu_s / el,
        if received > 0 { cpu_s / (received as f64 / 1e6) } else { 0.0 },
        rss / 1024,
        hist.percentile(50.0),
        hist.percentile(99.0),
        hist.percentile(99.9),
        hist.percentile(100.0),
    );
}

// ---- idle ----------------------------------------------------------------------------------------

fn idle(args: &Args, addr: SocketAddr, pid: u32) {
    let conns: usize = args.get("conns", 10_000);
    let secs: f64 = args.get("secs", 10.0);
    thread::sleep(Duration::from_millis(500));
    let rss0 = broker_rss_kb(pid);
    let t = Instant::now();
    let held: Vec<Client> = (0..conns).map(|i| connect_retry(addr, &format!("idle-{i}"))).collect();
    let connect_s = t.elapsed().as_secs_f64();
    thread::sleep(Duration::from_secs(1));
    let rss1 = broker_rss_kb(pid);
    let (cpu0, t0) = (broker_cpu_ns(pid), Instant::now());
    thread::sleep(Duration::from_secs_f64(secs));
    let cpu = (broker_cpu_ns(pid) - cpu0) as f64 / 1e9 / t0.elapsed().as_secs_f64();
    println!(
        "idle conns={conns} | connect {:.0} conn/s | broker rss {} MB -> {} MB, {:.2} KB per connection | broker cpu while idle {:.3} cores",
        conns as f64 / connect_s,
        rss0 / 1024,
        rss1 / 1024,
        (rss1 as f64 - rss0 as f64) / conns as f64,
        cpu
    );
    drop(held);
}

fn main() {
    let args = Args::parse();
    let port: u16 = args.get("port", 1883);
    let pid: u32 = args.get("broker-pid", 0);
    if let Some(ca) = args.opts.get("tls") {
        let pem = std::fs::read(ca).expect("--tls CA file");
        TLS.set(croisette::client::tls_config(&pem).expect("CA certificates")).ok();
    }
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    now_ns();
    match args.scenario.as_str() {
        "flow" => flow(&args, addr, pid),
        "idle" => idle(&args, addr, pid),
        other => panic!("unknown scenario {other}"),
    }
}
