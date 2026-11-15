//! Audited Linux network-device binding (`SO_BINDTODEVICE`) and synchronous ORS signal masking
//! (`pthread_sigmask` / `sigtimedwait`) helpers.

use crate::{Error, Result};
use std::fmt;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::os::fd::BorrowedFd;
use std::time::Duration;

/// Maximum permitted network interface name length in bytes (`libc::IFNAMSIZ - 1 == 15`).
pub const MAX_INTERFACE_NAME_LEN: usize = 15;

/// Returns the calling kernel thread's Linux TID (`gettid(2)`).
pub fn current_tid() -> Result<i32> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: `libc::gettid()` takes no arguments, touches no user memory, is async-signal-safe,
        // and always succeeds with the positive kernel thread ID of the caller.
        Ok(unsafe { libc::gettid() })
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(Error::UnsupportedPlatform)
    }
}

const IFNAMSIZ_BUF_LEN: usize = MAX_INTERFACE_NAME_LEN + 1;

#[cfg(target_os = "linux")]
const _: () = assert!(IFNAMSIZ_BUF_LEN == libc::IFNAMSIZ);

/// Validates a kernel network interface name (`1..=15` printable ASCII bytes, no NUL or whitespace)
/// and returns a NUL-terminated buffer plus the byte length including the trailing NUL.
fn validate_interface_name(interface: &str) -> Result<([u8; IFNAMSIZ_BUF_LEN], usize)> {
    let bytes = interface.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_INTERFACE_NAME_LEN
        || !bytes.iter().all(u8::is_ascii_graphic)
    {
        return Err(Error::InvalidResources);
    }
    let mut buf = [0u8; IFNAMSIZ_BUF_LEN];
    buf[..bytes.len()].copy_from_slice(bytes);
    Ok((buf, bytes.len() + 1))
}

/// Binds an open socket descriptor to a specific Linux network device via `SO_BINDTODEVICE`.
///
/// Validates that `interface` is non-empty printable ASCII (`1..=15` bytes, `< libc::IFNAMSIZ`,
/// no interior NUL or whitespace) before invoking `setsockopt(SOL_SOCKET, SO_BINDTODEVICE)`.
pub fn bind_socket_device(fd: BorrowedFd<'_>, interface: &str) -> Result<()> {
    let (buf, len) = validate_interface_name(interface)?;
    #[cfg(target_os = "linux")]
    {
        use std::io;
        use std::os::fd::AsRawFd;

        // SAFETY: `fd` is a valid open descriptor borrowed for this call, and `buf` is an
        // initialized NUL-terminated stack buffer of `len <= libc::IFNAMSIZ` bytes.
        let rc = unsafe {
            libc::setsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_BINDTODEVICE,
                buf.as_ptr().cast::<libc::c_void>(),
                len as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(Error::System {
                operation: "setsockopt SO_BINDTODEVICE",
                source: io::Error::last_os_error(),
            });
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (fd, buf, len);
        Err(Error::UnsupportedPlatform)
    }
}

/// Opens a `SOCK_NONBLOCK | SOCK_CLOEXEC` `TCP_NODELAY` socket, optionally binding it to
/// `interface` (`SO_BINDTODEVICE`, taking precedence when both `interface` and `local_ip` are
/// supplied) or `local_ip` (`bind(ip:0)`), and initiates a nonblocking `connect(peer)`.
pub fn open_bound_nonblocking_tcp_stream(
    peer: SocketAddr,
    local_ip: Option<IpAddr>,
    interface: Option<&str>,
) -> Result<TcpStream> {
    if let Some(iface) = interface {
        let _ = validate_interface_name(iface)?;
    } else if let Some(lip) = local_ip
        && lip.is_ipv4() != peer.is_ipv4()
    {
        return Err(Error::InvalidResources);
    }

    #[cfg(target_os = "linux")]
    {
        linux::open_bound_nonblocking_tcp_stream(peer, local_ip, interface)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (peer, local_ip, interface);
        Err(Error::UnsupportedPlatform)
    }
}

/// Connects a blocking `TCP_NODELAY` `SOCK_CLOEXEC` socket within `timeout`, optionally binding
/// `interface` (`SO_BINDTODEVICE`, taking precedence over `local_ip`) or `local_ip` (`bind(ip:0)`).
pub fn connect_bound_tcp_blocking(
    peer: SocketAddr,
    local_ip: Option<IpAddr>,
    interface: Option<&str>,
    timeout: Duration,
) -> Result<TcpStream> {
    if let Some(iface) = interface {
        let _ = validate_interface_name(iface)?;
    } else if let Some(lip) = local_ip
        && lip.is_ipv4() != peer.is_ipv4()
    {
        return Err(Error::InvalidResources);
    }
    if timeout.is_zero() {
        return Err(Error::Timeout);
    }

    #[cfg(target_os = "linux")]
    {
        linux::connect_bound_tcp_blocking(peer, local_ip, interface, timeout)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (peer, local_ip, interface, timeout);
        Err(Error::UnsupportedPlatform)
    }
}

/// Process-control signals handled synchronously by the ORS `main` housekeeping loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OrsSignal {
    /// `SIGINT` — initiate graceful ORS shutdown.
    SigInt,
    /// `SIGTERM` — initiate graceful ORS shutdown.
    SigTerm,
    /// `SIGUSR1` — user-defined operational hook (e.g., runtime diagnostics / state dump).
    SigUsr1,
    /// `SIGUSR2` — user-defined operational hook (e.g., latency-tracer flush).
    SigUsr2,
}

impl OrsSignal {
    /// Canonical POSIX signal name.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SigInt => "SIGINT",
            Self::SigTerm => "SIGTERM",
            Self::SigUsr1 => "SIGUSR1",
            Self::SigUsr2 => "SIGUSR2",
        }
    }

    /// Returns `true` when the signal requests graceful process termination (`SIGINT` / `SIGTERM`).
    #[inline]
    pub const fn is_shutdown(self) -> bool {
        matches!(self, Self::SigInt | Self::SigTerm)
    }
}

impl fmt::Display for OrsSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Masks `SIGINT`, `SIGTERM`, `SIGUSR1`, and `SIGUSR2` on the calling thread via
/// `pthread_sigmask(SIG_BLOCK, ...)`.
///
/// Call this on ORS `main` *before* spawning `REQ`, `RESP`, `Dialer`, `AsyncRestQuerier`, or
/// telemetry threads so every spawned child inherits the blocked signal mask and never suffers
/// asynchronous signal preemption on hot cores.
pub fn block_ors_signals_current_thread() -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::block_ors_signals_current_thread()
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(Error::UnsupportedPlatform)
    }
}

/// Synchronous `sigtimedwait` poller for `SIGINT`, `SIGTERM`, `SIGUSR1`, and `SIGUSR2`.
#[derive(Clone)]
pub struct SignalWaiter {
    #[cfg(target_os = "linux")]
    set: libc::sigset_t,
    #[cfg(not(target_os = "linux"))]
    _private: (),
}

impl fmt::Debug for SignalWaiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignalWaiter").finish_non_exhaustive()
    }
}

impl SignalWaiter {
    /// Blocks ORS signals on the calling thread (`block_ors_signals_current_thread`) and returns a
    /// [`SignalWaiter`] ready to dequeue them synchronously via [`Self::wait_timeout`].
    pub fn new() -> Result<Self> {
        block_ors_signals_current_thread()?;
        Self::without_blocking()
    }

    /// Constructs a [`SignalWaiter`] when the calling thread has already masked ORS signals via
    /// [`block_ors_signals_current_thread`].
    pub fn without_blocking() -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Ok(Self {
                set: linux::ors_sigset()?,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(Error::UnsupportedPlatform)
        }
    }

    /// Waits up to `timeout` for a pending [`OrsSignal`] via `sigtimedwait`.
    ///
    /// Returns `Ok(None)` when `timeout` expires (`EAGAIN`) or when `sigtimedwait` is interrupted
    /// by an unmasked signal (`EINTR`).
    pub fn wait_timeout(&self, timeout: Duration) -> Result<Option<OrsSignal>> {
        #[cfg(target_os = "linux")]
        {
            linux::wait_ors_signal(&self.set, timeout)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = timeout;
            Err(Error::UnsupportedPlatform)
        }
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{OrsSignal, bind_socket_device};
    use crate::{Error, Result};
    use std::io;
    use std::mem::{MaybeUninit, size_of};
    use std::net::{IpAddr, SocketAddr, TcpStream};
    use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
    use std::time::{Duration, Instant};

    fn system(operation: &'static str) -> Error {
        Error::System {
            operation,
            source: io::Error::last_os_error(),
        }
    }

    fn raw_sockaddr(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
        // SAFETY: `sockaddr_storage` is plain old data; zero-initializing avoids uninitialized padding.
        let mut storage: libc::sockaddr_storage = unsafe { core::mem::zeroed() };
        match addr {
            SocketAddr::V4(v4) => {
                let sin = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(v4.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                // SAFETY: `sockaddr_storage` is at least as large and aligned as `sockaddr_in`.
                unsafe {
                    core::ptr::write((&mut storage as *mut libc::sockaddr_storage).cast(), sin);
                }
                (storage, size_of::<libc::sockaddr_in>() as libc::socklen_t)
            }
            SocketAddr::V6(v6) => {
                let sin6 = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: v6.port().to_be(),
                    sin6_flowinfo: v6.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: v6.ip().octets(),
                    },
                    sin6_scope_id: v6.scope_id(),
                };
                // SAFETY: `sockaddr_storage` is at least as large and aligned as `sockaddr_in6`.
                unsafe {
                    core::ptr::write((&mut storage as *mut libc::sockaddr_storage).cast(), sin6);
                }
                (storage, size_of::<libc::sockaddr_in6>() as libc::socklen_t)
            }
        }
    }

    fn open_and_start_connect(
        peer: SocketAddr,
        local_ip: Option<IpAddr>,
        interface: Option<&str>,
    ) -> Result<(OwnedFd, bool)> {
        let domain = if peer.is_ipv4() {
            libc::AF_INET
        } else {
            libc::AF_INET6
        };
        // SAFETY: `socket` with `SOCK_CLOEXEC` creates a fresh file descriptor or returns `-1`.
        let raw_fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if raw_fd < 0 {
            return Err(system("socket tcp"));
        }
        // SAFETY: `raw_fd >= 0` is a newly created owned socket descriptor.
        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };

        if let Some(iface) = interface {
            bind_socket_device(fd.as_fd(), iface)?;
        } else if let Some(lip) = local_ip {
            let (bind_storage, bind_len) = raw_sockaddr(SocketAddr::new(lip, 0));
            // SAFETY: `fd` is valid and `bind_storage` holds `bind_len` initialized address bytes.
            if unsafe {
                libc::bind(
                    fd.as_raw_fd(),
                    (&bind_storage as *const libc::sockaddr_storage).cast(),
                    bind_len,
                )
            } != 0
            {
                return Err(system("bind tcp local_ip"));
            }
        }

        let one: libc::c_int = 1;
        // SAFETY: `fd` is a valid TCP socket and `one` is an initialized `c_int`.
        if unsafe {
            libc::setsockopt(
                fd.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                (&one as *const libc::c_int).cast(),
                size_of::<libc::c_int>() as libc::socklen_t,
            )
        } != 0
        {
            return Err(system("setsockopt TCP_NODELAY"));
        }

        let (peer_storage, peer_len) = raw_sockaddr(peer);
        // SAFETY: `fd` is valid and `peer_storage` holds `peer_len` initialized address bytes.
        let rc = unsafe {
            libc::connect(
                fd.as_raw_fd(),
                (&peer_storage as *const libc::sockaddr_storage).cast(),
                peer_len,
            )
        };
        if rc == 0 {
            return Ok((fd, true));
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINPROGRESS) {
            return Ok((fd, false));
        }
        Err(Error::System {
            operation: "connect tcp",
            source: err,
        })
    }

    pub(super) fn open_bound_nonblocking_tcp_stream(
        peer: SocketAddr,
        local_ip: Option<IpAddr>,
        interface: Option<&str>,
    ) -> Result<TcpStream> {
        let (fd, _connected_immediately) = open_and_start_connect(peer, local_ip, interface)?;
        Ok(TcpStream::from(fd))
    }

    pub(super) fn connect_bound_tcp_blocking(
        peer: SocketAddr,
        local_ip: Option<IpAddr>,
        interface: Option<&str>,
        timeout: Duration,
    ) -> Result<TcpStream> {
        let deadline = Instant::now() + timeout;
        let (fd, connected_immediately) = open_and_start_connect(peer, local_ip, interface)?;
        if !connected_immediately {
            loop {
                let Some(rem) = deadline.checked_duration_since(Instant::now()) else {
                    return Err(Error::Timeout);
                };
                if rem.is_zero() {
                    return Err(Error::Timeout);
                }
                let wait_ms = i32::try_from(rem.as_millis().max(1)).unwrap_or(i32::MAX);
                let mut pfd = libc::pollfd {
                    fd: fd.as_raw_fd(),
                    events: libc::POLLOUT,
                    revents: 0,
                };
                // SAFETY: `pfd` points to 1 valid `pollfd` on the stack.
                let n = unsafe { libc::poll(&mut pfd, 1, wait_ms) };
                if n < 0 {
                    if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(system("poll tcp connect"));
                }
                if n == 0 {
                    return Err(Error::Timeout);
                }
                let mut so_err: libc::c_int = 0;
                let mut len = size_of::<libc::c_int>() as libc::socklen_t;
                // SAFETY: `fd` is valid and `so_err`/`len` are valid stack pointers for `SO_ERROR`.
                if unsafe {
                    libc::getsockopt(
                        fd.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_ERROR,
                        (&mut so_err as *mut libc::c_int).cast(),
                        &mut len,
                    )
                } != 0
                {
                    return Err(system("getsockopt SO_ERROR"));
                }
                if so_err != 0 {
                    return Err(Error::System {
                        operation: "connect tcp",
                        source: io::Error::from_raw_os_error(so_err),
                    });
                }
                break;
            }
        }

        // Clear `O_NONBLOCK` before handing out a blocking `std::net::TcpStream`.
        // SAFETY: `F_GETFL` takes no extra arguments on valid descriptor `fd`.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(system("fcntl F_GETFL"));
        }
        // SAFETY: `F_SETFL` updates status flags on valid descriptor `fd`.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags & !libc::O_NONBLOCK) } != 0 {
            return Err(system("fcntl F_SETFL"));
        }

        Ok(TcpStream::from(fd))
    }

    pub(super) fn ors_sigset() -> Result<libc::sigset_t> {
        let mut set = MaybeUninit::<libc::sigset_t>::zeroed();
        // SAFETY: `set.as_mut_ptr()` points to a valid `sigset_t` allocation initialized by `sigemptyset`.
        if unsafe { libc::sigemptyset(set.as_mut_ptr()) } != 0 {
            return Err(system("sigemptyset"));
        }
        for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGUSR1, libc::SIGUSR2] {
            // SAFETY: `set` was initialized by `sigemptyset`, and `sig` is a valid standard signal.
            if unsafe { libc::sigaddset(set.as_mut_ptr(), sig) } != 0 {
                return Err(system("sigaddset"));
            }
        }
        // SAFETY: `sigemptyset` and `sigaddset` succeeded on `set`.
        Ok(unsafe { set.assume_init() })
    }

    pub(super) fn block_ors_signals_current_thread() -> Result<()> {
        let set = ors_sigset()?;
        // SAFETY: `set` is an initialized `sigset_t`, and `oldset` may be null per POSIX.
        let rc = unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &set, core::ptr::null_mut()) };
        if rc != 0 {
            return Err(Error::System {
                operation: "pthread_sigmask",
                source: io::Error::from_raw_os_error(rc),
            });
        }
        Ok(())
    }

    pub(super) fn wait_ors_signal(
        set: &libc::sigset_t,
        timeout: Duration,
    ) -> Result<Option<OrsSignal>> {
        let ts = libc::timespec {
            tv_sec: libc::time_t::try_from(timeout.as_secs()).unwrap_or(libc::time_t::MAX),
            tv_nsec: libc::c_long::from(timeout.subsec_nanos()),
        };
        let mut info = MaybeUninit::<libc::siginfo_t>::zeroed();
        // SAFETY: `set`, `info`, and `ts` are valid stack pointers for the duration of `sigtimedwait`.
        let signo = unsafe { libc::sigtimedwait(set, info.as_mut_ptr(), &ts) };
        if signo < 0 {
            let err = io::Error::last_os_error();
            if matches!(err.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) {
                return Ok(None);
            }
            return Err(Error::System {
                operation: "sigtimedwait",
                source: err,
            });
        }
        match signo {
            libc::SIGINT => Ok(Some(OrsSignal::SigInt)),
            libc::SIGTERM => Ok(Some(OrsSignal::SigTerm)),
            libc::SIGUSR1 => Ok(Some(OrsSignal::SigUsr1)),
            libc::SIGUSR2 => Ok(Some(OrsSignal::SigUsr2)),
            other => Err(Error::System {
                operation: "sigtimedwait unexpected signal",
                source: io::Error::other(format!("unexpected signal {other}")),
            }),
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn thread_directed_ors_signals_roundtrip_via_signal_waiter() {
        std::thread::spawn(|| {
            let waiter = SignalWaiter::new().unwrap();
            assert_eq!(waiter.wait_timeout(Duration::from_millis(5)).unwrap(), None);

            for (raw, expected, shutdown) in [
                (libc::SIGUSR1, OrsSignal::SigUsr1, false),
                (libc::SIGUSR2, OrsSignal::SigUsr2, false),
                (libc::SIGTERM, OrsSignal::SigTerm, true),
                (libc::SIGINT, OrsSignal::SigInt, true),
            ] {
                // SAFETY: `pthread_self()` returns the calling thread's valid `pthread_t`, and
                // `SignalWaiter::new()` already masked `raw` on this thread.
                let rc = unsafe { libc::pthread_kill(libc::pthread_self(), raw) };
                assert_eq!(rc, 0);
                let got = waiter.wait_timeout(Duration::from_secs(1)).unwrap();
                assert_eq!(got, Some(expected));
                assert_eq!(expected.is_shutdown(), shutdown);
            }
        })
        .join()
        .unwrap();
    }
}
