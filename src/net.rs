//! The few system calls std does not offer: SO_REUSEPORT listeners, TCP_NODELAY and shutdown
//! on raw fds, timerfd, termination signals.

use std::io;
use std::net::Ipv4Addr;
use std::os::fd::RawFd;
use std::time::Duration;

#[repr(C)]
struct SockAddrIn {
    family: u16,
    port: u16,
    addr: u32,
    zero: [u8; 8],
}

#[repr(C)]
struct TimeSpec {
    sec: i64,
    nsec: i64,
}

#[repr(C)]
struct ITimerSpec {
    interval: TimeSpec,
    value: TimeSpec,
}

#[repr(C)]
struct SigSet([u64; 16]);

extern "C" {
    fn socket(domain: i32, ty: i32, protocol: i32) -> i32;
    fn setsockopt(fd: i32, level: i32, name: i32, value: *const i32, len: u32) -> i32;
    fn bind(fd: i32, addr: *const SockAddrIn, len: u32) -> i32;
    fn listen(fd: i32, backlog: i32) -> i32;
    fn close(fd: i32) -> i32;
    fn timerfd_create(clock: i32, flags: i32) -> i32;
    fn timerfd_settime(fd: i32, flags: i32, new: *const ITimerSpec, old: *mut ITimerSpec) -> i32;
    fn sigemptyset(set: *mut SigSet) -> i32;
    fn sigaddset(set: *mut SigSet, sig: i32) -> i32;
    fn pthread_sigmask(how: i32, set: *const SigSet, old: *mut SigSet) -> i32;
    fn sigwait(set: *const SigSet, sig: *mut i32) -> i32;
    #[link_name = "shutdown"]
    fn shutdown_fd(fd: i32, how: i32) -> i32;
}

const AF_INET: i32 = 2;
const SOCK_STREAM: i32 = 1;
const SOCK_CLOEXEC: i32 = 0o2000000;
const SOL_SOCKET: i32 = 1;
const SO_REUSEADDR: i32 = 2;
const SO_REUSEPORT: i32 = 15;
const IPPROTO_TCP: i32 = 6;
const TCP_NODELAY: i32 = 1;

fn check(ret: i32) -> io::Result<i32> {
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

fn set_int(fd: RawFd, level: i32, name: i32, value: i32) -> io::Result<()> {
    check(unsafe { setsockopt(fd, level, name, &value, 4) }).map(|_| ())
}

/// A listening TCP socket with SO_REUSEPORT: one per core on the same port, and the kernel
/// spreads incoming connections across them. TCP_NODELAY is set on the listener and inherited by
/// accepted sockets.
pub fn reuseport_listener(ip: Ipv4Addr, port: u16, backlog: i32) -> io::Result<RawFd> {
    let fd = check(unsafe { socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0) })?;
    let setup = (|| {
        set_int(fd, SOL_SOCKET, SO_REUSEADDR, 1)?;
        set_int(fd, SOL_SOCKET, SO_REUSEPORT, 1)?;
        set_int(fd, IPPROTO_TCP, TCP_NODELAY, 1)?;
        let addr = SockAddrIn { family: AF_INET as u16, port: port.to_be(), addr: u32::from(ip).to_be(), zero: [0; 8] };
        check(unsafe { bind(fd, &addr, std::mem::size_of::<SockAddrIn>() as u32) })?;
        check(unsafe { listen(fd, backlog) })?;
        Ok(())
    })();
    match setup {
        Ok(()) => Ok(fd),
        Err(e) => {
            unsafe { close(fd) };
            Err(e)
        }
    }
}

/// Shuts both directions of a socket: sends FIN and completes any recv still in flight on it.
/// Needed before closing a socket that io_uring has a pending operation on: the operation holds
/// a reference to the file, so a close alone would leave the connection open.
pub fn shutdown(fd: RawFd) {
    const SHUT_RDWR: i32 = 2;
    unsafe { shutdown_fd(fd, SHUT_RDWR) };
}

/// A timerfd firing every `period`: each read of 8 bytes completes at the next expiry, so a
/// read kept in flight on io_uring wakes the event loop periodically, even when it sleeps.
pub fn timerfd(period: Duration) -> io::Result<RawFd> {
    const CLOCK_MONOTONIC: i32 = 1;
    let fd = check(unsafe { timerfd_create(CLOCK_MONOTONIC, SOCK_CLOEXEC) })?;
    let t = || TimeSpec { sec: period.as_secs() as i64, nsec: period.subsec_nanos() as i64 };
    let spec = ITimerSpec { interval: t(), value: t() };
    if let Err(e) = check(unsafe { timerfd_settime(fd, 0, &spec, std::ptr::null_mut()) }) {
        unsafe { close(fd) };
        return Err(e);
    }
    Ok(fd)
}

fn termination_signals() -> SigSet {
    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    let mut set = SigSet([0; 16]);
    unsafe {
        sigemptyset(&mut set);
        sigaddset(&mut set, SIGINT);
        sigaddset(&mut set, SIGTERM);
    }
    set
}

/// Blocks SIGINT and SIGTERM in the calling thread and the threads it starts afterwards, so that
/// `wait_for_termination` receives them. Call before starting the engine.
pub fn block_termination_signals() {
    const SIG_BLOCK: i32 = 0;
    let set = termination_signals();
    unsafe { pthread_sigmask(SIG_BLOCK, &set, std::ptr::null_mut()) };
}

/// Waits for SIGINT or SIGTERM; returns its number.
pub fn wait_for_termination() -> i32 {
    let set = termination_signals();
    let mut sig = 0;
    unsafe { sigwait(&set, &mut sig) };
    sig
}
