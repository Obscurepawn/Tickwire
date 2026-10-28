//! Bounded Linux control packets with owned, close-on-exec descriptor receipt.
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
