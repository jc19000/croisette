//! MQTT over WebSocket (RFC 6455), server side: the HTTP upgrade, then binary frames carrying the
//! MQTT byte stream (a packet may span frames, a frame may hold several packets).

const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_HANDSHAKE: usize = 8 << 10;

/// A WebSocket protocol violation (bad request, unmasked or oversized frame): the connection
/// must close.
#[derive(Debug, PartialEq, Eq)]
pub struct WsError;

#[derive(Default)]
pub struct Ws {
    upgraded: bool,
    /// Received bytes not decoded yet (handshake or frame header).
    raw: Vec<u8>,
    /// MQTT bytes from the frames, not parsed yet.
    pub mqtt: Vec<u8>,
    /// Frames to send before the next data (handshake response, pongs, close).
    control: Vec<u8>,
    /// The client sent a close frame, or the upgrade failed: close after sending `control`.
    pub closing: bool,
}

impl Ws {
    pub fn new() -> Box<Ws> {
        Box::default()
    }

    /// Takes received bytes: completes the upgrade, then decodes frames into `mqtt`. An error
    /// (bad request, protocol violation) ends the connection.
    pub fn receive(&mut self, data: &[u8]) -> Result<(), WsError> {
        self.raw.extend_from_slice(data);
        if !self.upgraded {
            let Some(end) = find(&self.raw, b"\r\n\r\n") else {
                return if self.raw.len() > MAX_HANDSHAKE { Err(WsError) } else { Ok(()) };
            };
            let request = String::from_utf8_lossy(&self.raw[..end]).into_owned();
            self.raw.drain(..end + 4);
            self.upgrade(&request)?;
        }
        self.decode_frames()
    }

    fn upgrade(&mut self, request: &str) -> Result<(), WsError> {
        let mut lines = request.split("\r\n");
        let first = lines.next().unwrap_or("");
        let header = |name: &str| {
            request.split("\r\n").skip(1).find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.trim().eq_ignore_ascii_case(name).then(|| v.trim().to_string())
            })
        };
        let upgrade_ok = header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
        let (Some(key), true, true) = (header("sec-websocket-key"), first.starts_with("GET "), upgrade_ok) else {
            self.control.extend_from_slice(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
            self.closing = true;
            return Ok(());
        };
        let accept = base64(&sha1(format!("{key}{GUID}").as_bytes()));
        let protocol = header("sec-websocket-protocol")
            .and_then(|p| p.split(',').map(str::trim).find(|p| *p == "mqtt" || *p == "mqttv3.1").map(str::to_string));
        let mut response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n"
        );
        if let Some(p) = protocol {
            response.push_str(&format!("Sec-WebSocket-Protocol: {p}\r\n"));
        }
        response.push_str("\r\n");
        self.control.extend_from_slice(response.as_bytes());
        self.upgraded = true;
        Ok(())
    }

    fn decode_frames(&mut self) -> Result<(), WsError> {
        loop {
            let b = &self.raw;
            if b.len() < 2 {
                return Ok(());
            }
            let opcode = b[0] & 0x0f;
            if b[1] & 0x80 == 0 {
                return Err(WsError); // client frames must be masked
            }
            let (len, mut at) = match b[1] & 0x7f {
                126 if b.len() >= 4 => (u16::from_be_bytes([b[2], b[3]]) as usize, 4),
                127 if b.len() >= 10 => (u64::from_be_bytes(b[2..10].try_into().unwrap()) as usize, 10),
                126 | 127 => return Ok(()),
                n => (n as usize, 2),
            };
            if len > crate::codec::MAX_PACKET * 2 {
                return Err(WsError);
            }
            if b.len() < at + 4 + len {
                return Ok(());
            }
            let mask = [b[at], b[at + 1], b[at + 2], b[at + 3]];
            at += 4;
            let payload: Vec<u8> = b[at..at + len].iter().enumerate().map(|(i, x)| x ^ mask[i % 4]).collect();
            self.raw.drain(..at + len);
            match opcode {
                0x0 | 0x2 => self.mqtt.extend_from_slice(&payload),
                0x8 => {
                    frame(&mut self.control, 0x8, &[]);
                    self.closing = true;
                    return Ok(());
                }
                0x9 => frame(&mut self.control, 0xA, &payload),
                0xA => {}
                _ => return Err(WsError), // text frames are not MQTT
            }
        }
    }

    /// Appends the pending control frames, then `mqtt` as one binary frame, to `out`.
    pub fn send(&mut self, mqtt: &[u8], out: &mut Vec<u8>) {
        out.append(&mut self.control);
        if !mqtt.is_empty() {
            frame(out, 0x2, mqtt);
        }
    }

    pub fn wants_write(&self) -> bool {
        !self.control.is_empty()
    }
}

/// An unmasked frame with the FIN bit (server to client).
fn frame(out: &mut Vec<u8>, opcode: u8, payload: &[u8]) {
    out.push(0x80 | opcode);
    match payload.len() {
        n if n < 126 => out.push(n as u8),
        n if n <= 0xffff => {
            out.push(126);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        n => {
            out.push(127);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(payload);
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

pub fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::new();
    for c in data.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= c.len() {
                s.push(T[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                s.push('=');
            }
        }
    }
    s
}

/// SHA-1 (FIPS 180-4), for the WebSocket handshake only.
pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&((data.len() as u64) * 8).to_be_bytes());
    for block in msg.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(block[4 * i..4 * i + 4].try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let t = a.rotate_left(5).wrapping_add(f).wrapping_add(e).wrapping_add(k).wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = t;
        }
        for (x, y) in h.iter_mut().zip([a, b, c, d, e]) {
            *x = x.wrapping_add(y);
        }
    }
    let mut out = [0u8; 20];
    for (i, x) in h.iter().enumerate() {
        out[4 * i..4 * i + 4].copy_from_slice(&x.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_accept_key() {
        // RFC 6455, section 1.3.
        assert_eq!(base64(&sha1(format!("dGhlIHNhbXBsZSBub25jZQ=={GUID}").as_bytes())), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(base64(b"abc"), "YWJj");
        assert_eq!(base64(b"a"), "YQ==");
    }

    fn client_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mask = [1u8, 2, 3, 4];
        let mut f = vec![0x80 | opcode];
        match payload.len() {
            n if n < 126 => f.push(0x80 | n as u8),
            n => {
                f.push(0x80 | 126);
                f.extend_from_slice(&(n as u16).to_be_bytes());
            }
        }
        f.extend_from_slice(&mask);
        f.extend(payload.iter().enumerate().map(|(i, x)| x ^ mask[i % 4]));
        f
    }

    #[test]
    fn upgrade_frames_and_control() {
        let mut ws = Ws::new();
        let req = "GET /mqtt HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n";
        let mut data = req.as_bytes().to_vec();
        data.extend(client_frame(0x2, b"hello "));
        let big = vec![7u8; 300];
        data.extend(client_frame(0x2, &big));
        data.extend(client_frame(0x9, b"p"));
        // Byte by byte: frames and the handshake may arrive in pieces.
        for b in &data {
            ws.receive(std::slice::from_ref(b)).unwrap();
        }
        assert_eq!(&ws.mqtt[..6], b"hello ");
        assert_eq!(&ws.mqtt[6..], &big[..]);
        let mut out = Vec::new();
        ws.send(b"xy", &mut out);
        let text = String::from_utf8_lossy(&out);
        assert!(text.starts_with("HTTP/1.1 101"), "{text}");
        assert!(text.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        assert!(text.contains("Sec-WebSocket-Protocol: mqtt"));
        assert!(out.ends_with(&[0x8A, 1, b'p', 0x82, 2, b'x', b'y']), "pong then data");
        assert!(Ws::new().receive(b"GET / HTTP/1.1\r\n\r\n").is_ok());
        let mut unmasked = Ws::new();
        unmasked.receive(req.as_bytes()).unwrap();
        assert!(unmasked.receive(&[0x82, 1, 0]).is_err());
    }
}
