//! Audited Linux OS boundary for `AF_UNIX` `SOCK_SEQPACKET` control sockets, `0o700` pinned
//! directories, sealed `memfd` ELF execution, thread-level CPU affinity (`CpuSet`,
//! `pin_current_thread`, `current_thread_affinity`, `spawn_pinned_thread`,
//! `validate_cpu_isolation`, `current_tid`), network-interface socket binding (`bind_socket_device`,
//! `open_bound_nonblocking_tcp_stream`, `connect_bound_tcp_blocking`), synchronous ORS signal
//! masking (`OrsSignal`, `block_ors_signals_current_thread`, `SignalWaiter`), and `/proc` + cgroup
//! v2 process resource inspection.
//!
//! Higher-level business, transport, and supervision crates retain `#![forbid(unsafe_code)]` and
//! delegate all raw descriptor, `memfd`, cgroup, `SO_BINDTODEVICE`, `pthread_sigmask` /
//! `sigtimedwait`, and `sched_setaffinity` / `sched_getaffinity` syscalls through this crate
//! (`#![deny(unsafe_op_in_unsafe_fn)]`).
#![deny(unsafe_op_in_unsafe_fn)]

use std::io;
use std::os::fd::OwnedFd;

pub const MAX_FRAME_BYTES: usize = 65_536;
pub const MAX_FDS: usize = 16;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    UnsupportedPlatform,
    Timeout,
    Closed,
    InvalidPath,
    InvalidExecutable,
    InvalidResources,
    ResourceConflict,
    InsecureDirectory,
    Truncated,
    Ancillary,
    FdCount {
        expected: usize,
        actual: usize,
    },
    Protocol(tickwire_ipc::Error),
    System {
        operation: &'static str,
        source: io::Error,
    },
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerCredentials {
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
}
pub struct ReceivedFrame {
    bytes: Vec<u8>,
    fds: Vec<OwnedFd>,
}
impl ReceivedFrame {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn fds(&self) -> &[OwnedFd] {
        &self.fds
    }
    pub fn into_parts(self) -> (Vec<u8>, Vec<OwnedFd>) {
        (self.bytes, self.fds)
    }
}
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{Connection, ControlListener, PendingConnection, PrivateDirectory, monotonic_ns};
#[cfg(not(target_os = "linux"))]
mod unsupported;
#[cfg(not(target_os = "linux"))]
pub use unsupported::{
    Connection, ControlListener, PendingConnection, PrivateDirectory, monotonic_ns,
};

#[cfg(target_os = "linux")]
mod executable;
#[cfg(target_os = "linux")]
pub use executable::ExecutableImage;
#[cfg(not(target_os = "linux"))]
pub use unsupported::ExecutableImage;

mod affinity;
pub use affinity::{
    CpuSet, MAX_CPUS, current_thread_affinity, pin_current_thread, spawn_pinned_thread,
    validate_cpu_isolation,
};

mod ors_os;
pub use ors_os::{
    MAX_INTERFACE_NAME_LEN, OrsSignal, SignalWaiter, bind_socket_device,
    block_ors_signals_current_thread, connect_bound_tcp_blocking, current_tid,
    open_bound_nonblocking_tcp_stream,
};

mod process_resources;
pub use process_resources::{ResourceDomain, ResourceLimits, ResourceObservation};
