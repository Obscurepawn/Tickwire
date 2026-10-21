//! HTTPS and peer I/O share one durable business/consensus owner.
use crate::{
    runner,
    service::{PeerBudget, PollStatus as PeerStatus, ServiceError},
};
use std::{net::SocketAddr, time::Instant};
use tickwire_config_service::{AppliedResult, ReplicaControl, auth::ServerTls, https};
use tickwire_control_raft::{driver::DriverStatus, generation::GenerationStore};

#[derive(Debug)]
pub enum Error {
    Assembly(runner::Error),
    Peer(ServiceError<tickwire_config_service::Error>),
    Api(tickwire_config_service::Error),
    Stopped,
}
pub type Result<T> = core::result::Result<T, Error>;
pub struct PollStatus {
    pub peer: PeerStatus,
    pub http: https::Status,
}
pub struct ApiOwner {
    peer: runner::Server,
    http: Option<https::Server>,
}
impl ApiOwner {
    pub fn from_parts(peer: runner::Server, http: https::Server) -> Self {
        Self {
            peer,
            http: Some(http),
        }
    }
    pub fn assemble(
        config: &runner::Configuration,
        address: SocketAddr,
        trust: ServerTls,
        limits: https::Limits,
    ) -> Result<Self> {
        let peer = runner::prepare(config).map_err(Error::Assembly)?;
        Self::assemble_prepared(peer, address, trust, limits)
    }
    pub fn assemble_prepared(
        prepared: runner::Prepared,
        address: SocketAddr,
        trust: ServerTls,
        limits: https::Limits,
    ) -> Result<Self> {
        let http = https::Server::bind(address, trust, limits).map_err(Error::Api)?;
        let peer = prepared.assemble().map_err(Error::Assembly)?;
        Ok(Self::from_parts(peer, http))
    }
    pub fn http_addr(&self) -> Result<SocketAddr> {
        self.http
            .as_ref()
            .ok_or(Error::Stopped)?
            .local_addr()
            .map_err(Error::Api)
    }
    pub fn status(&self) -> DriverStatus {
        self.peer.status()
    }
    pub fn poll(
        &mut self,
        now: Instant,
        peer_budget: PeerBudget,
        http_budget: usize,
    ) -> Result<PollStatus> {
        if self.http.is_none() {
            return Err(Error::Stopped);
        }
        if !(1..=64).contains(&http_budget) {
            return Err(Error::Api(tickwire_config_service::Error::Bounds));
        }
        let result = self.poll_inner(now, peer_budget, http_budget);
        if result.is_err() {
            self.peer.stop();
            self.http.take();
        }
        result
    }
    fn poll_inner(
        &mut self,
        now: Instant,
        peer_budget: PeerBudget,
        http_budget: usize,
    ) -> Result<PollStatus> {
        let peer = self.peer.poll(now, peer_budget).map_err(Error::Peer)?;
        let http = self.http.as_mut().ok_or(Error::Stopped)?;
        let http = self
            .peer
            .with_control(|mut control| http.poll(&mut control, http_budget))
            .map_err(Error::Peer)?
            .map_err(Error::Api)?;
        Ok(PollStatus { peer, http })
    }
    pub fn take_applied(&mut self, items: usize, bytes: usize) -> Vec<AppliedResult> {
        self.peer.take_applied(items, bytes)
    }
    pub fn with_control<R>(
        &mut self,
        f: impl FnOnce(ReplicaControl<'_, GenerationStore>) -> R,
    ) -> Result<R> {
        self.peer.with_control(f).map_err(Error::Peer)
    }
    pub fn replace_http_trust(&mut self, trust: ServerTls) -> Result<()> {
        self.http
            .as_mut()
            .ok_or(Error::Stopped)?
            .replace_trust(trust)
            .map_err(Error::Api)
    }
    pub fn replace_peer_trust(&mut self, trust: crate::tls::PeerTls) -> Result<usize> {
        self.peer.replace_trust(trust).map_err(Error::Peer)
    }
    pub fn shutdown(&mut self) -> Result<()> {
        let result = if let Some(mut http) = self.http.take() {
            self.peer
                .with_control(|mut control| http.shutdown(&mut control))
                .map_err(Error::Peer)
                .and_then(|r| r.map_err(Error::Api))
        } else {
            Ok(())
        };
        self.peer.stop();
        result
    }
}
