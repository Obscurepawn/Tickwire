use super::*;
use std::os::fd::BorrowedFd;
use std::path::{Path, PathBuf};
use std::time::Instant;
pub struct PrivateDirectory {
    path: PathBuf,
}
impl PrivateDirectory {
    pub fn create_in(_parent: &Path, _prefix: &str) -> Result<Self> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}
pub struct ControlListener {
    _private: (),
}
impl ControlListener {
    pub fn bind(path: &Path) -> Result<Self> {
        Self::bind_with_mode(path, 0o600)
    }
    pub fn bind_with_mode(_path: &Path, _mode: u32) -> Result<Self> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn accept(&self, _deadline: Instant) -> Result<Connection> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn try_accept(&self) -> Result<Option<Connection>> {
        Err(Error::UnsupportedPlatform)
    }
}
pub struct Connection {
    _private: (),
}
pub struct PendingConnection {
    _private: (),
}
impl PendingConnection {
    pub fn new(_path: &Path) -> Result<Self> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn poll(&mut self) -> Result<Option<Connection>> {
        Err(Error::UnsupportedPlatform)
    }
}
impl Connection {
    pub fn connect(_path: &Path, _deadline: Instant) -> Result<Self> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn peer_credentials(&self) -> PeerCredentials {
        unreachable!()
    }
    pub fn send(
        &mut self,
        _bytes: &[u8],
        _fds: &[BorrowedFd<'_>],
        _deadline: Instant,
    ) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn try_send(&mut self, _bytes: &[u8], _fds: &[BorrowedFd<'_>]) -> Result<bool> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn recv(&mut self, _deadline: Instant) -> Result<ReceivedFrame> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn try_recv(&mut self) -> Result<Option<ReceivedFrame>> {
        Err(Error::UnsupportedPlatform)
    }
}
pub fn monotonic_ns() -> Result<u64> {
    Err(Error::UnsupportedPlatform)
}
