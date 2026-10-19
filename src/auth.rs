//! Authentication and access control, in the formats Mosquitto uses.
//!
//! Password file: one `user:password` per line, the password in clear or as
//! `$sha256$<salt>$<hex of sha256(salt + password)>` (`croisette hash-password` prints one).
//!
//! ACL file: `topic [read|write|readwrite] <filter>` lines apply to the user of the last
//! `user <name>` line, or to anonymous clients before any; `pattern ...` lines apply to every
//! client, with `%c` replaced by the client id and `%u` by the user name. Without an ACL file
//! every client may publish and subscribe anywhere; with one, only what a rule allows.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use crate::codec;

enum Secret {
    Plain(Vec<u8>),
    Salted { salt: Vec<u8>, hash: [u8; 32] },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub read: bool,
    pub write: bool,
    pub filter: String,
}

#[derive(Default)]
struct Acl {
    anonymous: Vec<Rule>,
    users: HashMap<String, Vec<Rule>>,
    patterns: Vec<Rule>,
}

#[derive(Default)]
pub struct Auth {
    users: HashMap<String, Secret>,
    pub allow_anonymous: bool,
    acl: Option<Acl>,
}

/// CONNACK return codes.
pub const BAD_CREDENTIALS: u8 = 4;
pub const NOT_AUTHORIZED: u8 = 5;

impl Auth {
    pub fn load(passwd: Option<&Path>, acl: Option<&Path>, allow_anonymous: bool) -> io::Result<Auth> {
        let mut auth = Auth { allow_anonymous, ..Auth::default() };
        if let Some(path) = passwd {
            auth.users = parse_passwd(&std::fs::read_to_string(path)?)?;
        }
        if let Some(path) = acl {
            auth.acl = Some(parse_acl(&std::fs::read_to_string(path)?)?);
        }
        Ok(auth)
    }

    /// From file contents, for tests and embedding.
    pub fn from_strs(passwd: &str, acl: Option<&str>, allow_anonymous: bool) -> io::Result<Auth> {
        Ok(Auth { users: parse_passwd(passwd)?, allow_anonymous, acl: acl.map(parse_acl).transpose()? })
    }

    /// CONNACK code for these credentials: 0 accepted.
    pub fn check(&self, username: Option<&str>, password: Option<&[u8]>) -> u8 {
        let Some(user) = username else {
            return if self.allow_anonymous { 0 } else { NOT_AUTHORIZED };
        };
        let Some(secret) = self.users.get(user) else { return BAD_CREDENTIALS };
        let given = password.unwrap_or_default();
        let ok = match secret {
            Secret::Plain(p) => constant_time_eq(p, given),
            Secret::Salted { salt, hash } => constant_time_eq(hash, &sha256(&[salt, given].concat())),
        };
        if ok {
            0
        } else {
            BAD_CREDENTIALS
        }
    }

    /// The rules of a client, `None` when there is no ACL (everything allowed).
    pub fn rules(&self, username: Option<&str>, client_id: &str) -> Option<Vec<Rule>> {
        let acl = self.acl.as_ref()?;
        let mut rules = match username {
            Some(u) => acl.users.get(u).cloned().unwrap_or_default(),
            None => acl.anonymous.clone(),
        };
        for p in &acl.patterns {
            let filter = p.filter.replace("%c", client_id).replace("%u", username.unwrap_or(""));
            rules.push(Rule { filter, ..p.clone() });
        }
        Some(rules)
    }
}

/// Whether `rules` allow publishing to `topic`.
pub fn can_write(rules: &[Rule], topic: &str) -> bool {
    rules.iter().any(|r| r.write && codec::matches(&r.filter, topic))
}

/// Whether `rules` allow subscribing to `filter`: some read rule covers every topic it matches.
pub fn can_read(rules: &[Rule], filter: &str) -> bool {
    let filter = crate::index::shared(filter).map_or(filter, |(_, f)| f);
    rules.iter().any(|r| r.read && covers(&r.filter, filter))
}

/// Whether every topic matching `filter` matches `rule`.
fn covers(rule: &str, filter: &str) -> bool {
    let mut r = rule.split('/');
    let mut f = filter.split('/');
    loop {
        match (r.next(), f.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(level)) if level != "#" => {}
            (Some(a), Some(b)) if a == b && a != "+" && a != "#" => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

fn invalid(line: usize, why: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("line {}: {why}", line + 1))
}

fn parse_passwd(text: &str) -> io::Result<HashMap<String, Secret>> {
    let mut users = HashMap::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (user, password) = line.split_once(':').ok_or_else(|| invalid(n, "expected user:password"))?;
        let secret = match password.strip_prefix("$sha256$") {
            Some(rest) => {
                let (salt, hex) = rest.split_once('$').ok_or_else(|| invalid(n, "expected $sha256$salt$hash"))?;
                let hash = unhex(hex).ok_or_else(|| invalid(n, "bad hash"))?;
                Secret::Salted { salt: salt.as_bytes().to_vec(), hash }
            }
            None => Secret::Plain(password.as_bytes().to_vec()),
        };
        users.insert(user.to_string(), secret);
    }
    Ok(users)
}

fn parse_acl(text: &str) -> io::Result<Acl> {
    let mut acl = Acl::default();
    let mut user: Option<String> = None;
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (keyword, rest) = line.split_once(char::is_whitespace).ok_or_else(|| invalid(n, "expected a rule"))?;
        let rest = rest.trim();
        match keyword {
            "user" => {
                user = Some(rest.to_string());
                acl.users.entry(rest.to_string()).or_default();
            }
            "topic" | "pattern" => {
                let (read, write, filter) = match rest.split_once(char::is_whitespace) {
                    Some(("read", f)) => (true, false, f.trim()),
                    Some(("write", f)) => (false, true, f.trim()),
                    Some(("readwrite", f)) => (true, true, f.trim()),
                    _ => (true, true, rest),
                };
                if !codec::valid_filter(filter) {
                    return Err(invalid(n, "bad topic filter"));
                }
                let rule = Rule { read, write, filter: filter.to_string() };
                match (keyword, &user) {
                    ("pattern", _) => acl.patterns.push(rule),
                    (_, Some(u)) => acl.users.get_mut(u).unwrap().push(rule),
                    (_, None) => acl.anonymous.push(rule),
                }
            }
            _ => return Err(invalid(n, "expected user, topic or pattern")),
        }
    }
    Ok(acl)
}

/// A password file line for `user` with a random salt.
pub fn hash_password(user: &str, password: &str) -> String {
    let mut seed = [0u8; 12];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = io::Read::read_exact(&mut f, &mut seed);
    }
    let salt: String = seed.iter().map(|b| format!("{:02x}", b)).collect();
    let hash = sha256(&[salt.as_bytes(), password.as_bytes()].concat());
    format!("{user}:$sha256${salt}${}", hash.iter().map(|b| format!("{:02x}", b)).collect::<String>())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn unhex(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// SHA-256 (FIPS 180-4).
pub fn sha256(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98,
        0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
        0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8,
        0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
        0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
        0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] =
        [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19];
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&((data.len() as u64) * 8).to_be_bytes());
    for block in msg.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(block[4 * i..4 * i + 4].try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (x, y) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *x = x.wrapping_add(y);
        }
    }
    let mut out = [0u8; 32];
    for (i, x) in h.iter().enumerate() {
        out[4 * i..4 * i + 4].copy_from_slice(&x.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn sha256_known_values() {
        assert_eq!(hex(&sha256(b"")), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assert_eq!(hex(&sha256(b"abc")), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        let long = vec![b'a'; 1000];
        assert_eq!(hex(&sha256(&long)), "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3");
    }

    #[test]
    fn passwords() {
        let line = hash_password("bob", "s3cret");
        let auth = Auth::from_strs(&format!("alice:wonder\n{line}\n"), None, false).unwrap();
        assert_eq!(auth.check(Some("alice"), Some(b"wonder")), 0);
        assert_eq!(auth.check(Some("alice"), Some(b"wonde")), BAD_CREDENTIALS);
        assert_eq!(auth.check(Some("bob"), Some(b"s3cret")), 0);
        assert_eq!(auth.check(Some("bob"), Some(b"secret")), BAD_CREDENTIALS);
        assert_eq!(auth.check(Some("eve"), Some(b"x")), BAD_CREDENTIALS);
        assert_eq!(auth.check(None, None), NOT_AUTHORIZED);
    }

    #[test]
    fn acl_rules() {
        let acl = "topic read $SYS/#\nuser alice\ntopic readwrite sensors/#\ntopic write cmd/+\npattern readwrite devices/%c/#\n";
        let auth = Auth::from_strs("", Some(acl), true).unwrap();
        let alice = auth.rules(Some("alice"), "d1").unwrap();
        assert!(can_write(&alice, "sensors/a/b") && can_read(&alice, "sensors/+/b") && can_read(&alice, "sensors/#"));
        assert!(can_write(&alice, "cmd/x") && !can_read(&alice, "cmd/x"));
        assert!(can_write(&alice, "devices/d1/x") && !can_write(&alice, "devices/d2/x"));
        assert!(!can_read(&alice, "#") && !can_read(&alice, "devices/+/x"));
        assert!(can_read(&alice, "$share/g/sensors/x"));
        let anon = auth.rules(None, "d2").unwrap();
        assert!(can_read(&anon, "$SYS/broker/uptime") && !can_write(&anon, "$SYS/x"));
        assert!(!can_write(&anon, "sensors/a") && can_write(&anon, "devices/d2/a"));
        assert!(Auth::from_strs("", None, true).unwrap().rules(None, "x").is_none());
    }
}
