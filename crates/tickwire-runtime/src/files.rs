//! Concrete file-backed assembly of the Binance trading owners.
use crate::{
    authority::{GateProvider, LocalAuthority, RecoveryQueue},
    node::{self, Bindings, CoreNode, Observer, Strategy},
};
use std::path::PathBuf;
use tickwire_account_gateway::{AccountGateway, ConnectionConfig};
use tickwire_account_journal::FileBackend;
use tickwire_binance::request::Credentials;
use tickwire_binance_account::{
    AccountIngress,
    rest::{RestConfig, RestOwner},
};
use tickwire_binance_clock::{self as time, BinanceTimeClient, SystemClock};
use tickwire_binance_execution::{
    self as execution, GateAuthority, RestEncoder, SharedSessionState,
};
use tickwire_binance_private as private;
use tickwire_binance_public as public;
use tickwire_binance_quota::Endpoint;
use tickwire_journal::{PhysicalFrontier, store::FileStore};
use tickwire_recovery::FileBlobStore;

#[derive(Clone, Copy)]
pub struct FixedDeliveryBudget(pub u64);
impl execution::DeliveryBudget for FixedDeliveryBudget {
    fn remaining_ns(
        &mut self,
        _: &tickwire_venue::CanonicalCommand,
        _: &tickwire_clock::SignedTime,
        _: &time::LocalTime,
    ) -> execution::Result<u64> {
        if self.0 == 0 {
            return Err(execution::Error::Invalid);
        }
        Ok(self.0)
    }
}
pub struct Directories {
    pub account: PathBuf,
    pub input: PathBuf,
    pub recovery: PathBuf,
}
pub enum OpenMode {
    Create,
    RecoverFrozen {
        account_required: Option<PhysicalFrontier>,
        input_required: Option<PhysicalFrontier>,
    },
}
pub struct Configuration {
    pub directories: Directories,
    pub mode: OpenMode,
    pub account: tickwire_account_gateway::Config,
    pub input: tickwire_binance_account::Config,
    pub rest: RestConfig,
    pub time: time::Config,
    pub market: public::Config,
    pub private: private::SessionConfig,
    pub private_first_connection_epoch: u64,
    pub command: execution::Config,
    pub command_binding: tickwire_binance_quota::CommandBinding,
    pub command_delivery_budget_ns: u64,
    pub runtime: node::Config,
    pub bindings: Bindings,
    pub maximum_staged_commands: usize,
    pub maximum_staged_bytes: usize,
}
pub struct Networks {
    pub rest: ConnectionConfig,
    pub time: time::NetworkConfig,
    pub public: public::NetworkConfig,
    pub private: private::NetworkConfig,
}
type Time = BinanceTimeClient<SystemClock>;
pub type FileNode<'a, P, G, S, O, const L: usize, const U: usize, const B: usize> = CoreNode<
    'a,
    P,
    SystemClock,
    SystemClock,
    FixedDeliveryBudget,
    FileStore,
    FileBackend,
    GateAuthority<LocalAuthority<FileBlobStore, G>, Time, FixedDeliveryBudget>,
    RestEncoder<'a, Time, FixedDeliveryBudget>,
    S,
    O,
    L,
    U,
    B,
>;

/// Constructs real TLS clients and local WAL owners. Credentials and authority
/// policy are explicit caller inputs; this function does not grant trading rights.
pub fn open<'a, P, G, S, O, const L: usize, const U: usize, const B: usize>(
    config: Configuration,
    networks: Networks,
    credentials: Credentials<'a>,
    authentication: private::Authentication<'a>,
    quota: Endpoint,
    policy: G,
    strategy: S,
    output: O,
    observer: Box<dyn Observer + 'a>,
) -> node::Result<FileNode<'a, P, G, S, O, L, U, B>>
where
    P: public::DepthPolicy,
    G: GateProvider,
    S: Strategy,
    O: public::PublicSink,
{
    if config.directories.account == config.directories.input
        || config.directories.account == config.directories.recovery
        || config.directories.input == config.directories.recovery
        || config.time.domain != config.market.domain
        || config.time.source != config.market.source
        || config.command_delivery_budget_ns == 0
    {
        return Err(node::Error::Config);
    }
    for path in [
        &config.directories.account,
        &config.directories.input,
        &config.directories.recovery,
    ] {
        if !path.is_absolute() {
            return Err(node::Error::Config);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(node::Error::Io)?;
        }
    }
    let clock = SystemClock::new(config.time.domain).map_err(|_| node::Error::Config)?;
    let time =
        BinanceTimeClient::new(config.time, networks.time, clock).map_err(node::Error::Clock)?;
    let shared = SharedSessionState::new(
        time,
        quota,
        config.command_binding,
        FixedDeliveryBudget(config.command_delivery_budget_ns),
        config.command,
    )
    .map_err(node::Error::Shared)?;
    let queue = RecoveryQueue::new(config.maximum_staged_commands, config.maximum_staged_bytes)
        .map_err(node::Error::Recovery)?;
    let blobs = FileBlobStore::open(&config.directories.recovery).map_err(|e| {
        node::Error::Recovery(tickwire_account_gateway::AuthorityError::Recovery(e))
    })?;
    let authority = shared.authority(LocalAuthority::new(blobs, queue.clone(), policy));
    let encoder = shared.encoder(credentials);
    let backend = FileBackend::open(&config.directories.account).map_err(node::Error::Io)?;
    let input = FileStore::open(&config.directories.input).map_err(node::Error::Io)?;
    let (gateway, ingress) = match config.mode {
        OpenMode::Create => {
            let gateway = AccountGateway::create(
                backend,
                config.account,
                config.input.baseline.clone(),
                authority,
                encoder,
                networks.rest,
            )
            .map_err(|e| node::Error::Account(tickwire_binance_account::Error::Gateway(e)))?;
            let ingress =
                AccountIngress::create(input, config.input).map_err(node::Error::Account)?;
            (gateway, ingress)
        }
        OpenMode::RecoverFrozen {
            account_required,
            input_required,
        } => {
            let ingress = AccountIngress::recover(input, config.input, input_required.as_ref())
                .map_err(node::Error::Account)?;
            let mut verifier=ingress.activation_verifier();
            let gateway = AccountGateway::recover_with_reactivation(
                backend,
                config.account,
                authority,
                encoder,
                account_required.as_ref(),
                &mut verifier,
            )
            .map_err(|e| node::Error::Account(tickwire_binance_account::Error::Gateway(e)))?;
            (gateway, ingress)
        }
    };
    let rest =
        RestOwner::from_parts(ingress, gateway, config.rest).map_err(node::Error::Account)?;
    let clock = SystemClock::new(config.market.domain).map_err(|_| node::Error::Config)?;
    let public = public::PublicDepthClient::new(config.market, networks.public, clock)
        .map_err(node::Error::Market)?;
    let private = private::PrivateClient::new(
        config.private,
        authentication,
        networks.private,
        config.private_first_connection_epoch,
    )
    .map_err(node::Error::Private)?;
    CoreNode::new(
        public,
        private,
        rest,
        shared,
        queue,
        strategy,
        output,
        observer,
        config.runtime,
        config.bindings,
    )
}
