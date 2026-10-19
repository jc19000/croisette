//! TLS for client connections, with rustls on top of the io_uring receives and sends: ciphertext
//! comes in and goes out through the same buffers as plain TCP, the connection tier works on the
//! plaintext.

use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;

pub use rustls::ServerConfig;

/// A server configuration from PEM files: the certificate chain and its private key.
pub fn server_config(cert: &Path, key: &Path) -> io::Result<Arc<ServerConfig>> {
    server_config_from_pem(&std::fs::read(cert)?, &std::fs::read(key)?)
}

pub fn server_config_from_pem(cert: &[u8], key: &[u8]) -> io::Result<Arc<ServerConfig>> {
    let certs = rustls_pemfile::certs(&mut &cert[..]).collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut &key[..])?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no private key"))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(io::Error::other)?;
    Ok(Arc::new(config))
}

/// The TLS state of a connection, and the plaintext received and not parsed yet.
pub struct Tls {
    conn: rustls::ServerConnection,
    pub plain: Vec<u8>,
}

impl Tls {
    pub fn new(config: &Arc<ServerConfig>) -> io::Result<Box<Tls>> {
        let mut conn = rustls::ServerConnection::new(config.clone()).map_err(io::Error::other)?;
        conn.set_buffer_limit(None); // the connection tier bounds what it queues
        Ok(Box::new(Tls { conn, plain: Vec::new() }))
    }

    /// Takes received ciphertext; the plaintext it carries is appended to `plain`. An error (bad
    /// record, failed handshake) ends the connection.
    pub fn receive(&mut self, mut cipher: &[u8]) -> io::Result<()> {
        while !cipher.is_empty() {
            self.conn.read_tls(&mut cipher)?;
            self.conn.process_new_packets().map_err(io::Error::other)?;
            match self.conn.reader().read_to_end(&mut self.plain) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Encrypts `plain` and appends the records to `out`, after any handshake records due.
    pub fn send(&mut self, plain: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        if !plain.is_empty() {
            self.conn.writer().write_all(plain)?;
        }
        while self.conn.wants_write() {
            self.conn.write_tls(out)?;
        }
        Ok(())
    }

    /// TLS records are due (handshake, alerts).
    pub fn wants_write(&self) -> bool {
        self.conn.wants_write()
    }
}
