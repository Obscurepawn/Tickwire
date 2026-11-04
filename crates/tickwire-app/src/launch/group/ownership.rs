//! Static claims on the resources that two hosted members could otherwise mutate together.
use crate::{Result, config::read_file, hosted::HostConfig, invalid};
use std::{
    fs,
    io::ErrorKind,
    path::{Component, Path, PathBuf},
};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::ConnectConfig;
use tickwire_shm::BankKey;
use tickwire_strategy_sdk::bootstrap::Bootstrap;

#[derive(Default)]
pub(super) struct Ownership {
    directories: Vec<(PathBuf, [u8; 16])>,
    markets: Vec<MarketClaim>,
}

struct MarketClaim {
    member: [u8; 16],
    server_pid: u32,
    bank: BankKey,
    cursor: [u8; 16],
    request: tickwire_control::RequestKey,
}

impl Ownership {
    pub(super) fn claim(&mut self, host: &HostConfig, bootstrap: &Bootstrap) -> Result<()> {
        let member = bootstrap.market.local.instance;
        for path in [
            &bootstrap.command_directory,
            &bootstrap.report_directory,
            &host.recovery_directory,
        ] {
            self.directory(path, member)?;
        }
        if let Some(external) = &bootstrap.external_market {
            self.market(external, host, member)?;
        } else {
            self.directory(&host.control_directory, member)?;
        }
        if let Some(capture) = &host.capture {
            let input: tickwire_strategy_sdk::capture::Bootstrap =
                serde_json::from_slice(&read_file(&capture.bootstrap, 2 * 1024 * 1024)?)?;
            self.directory(&capture.publication_directory, member)?;
            self.directory(&input.report_directory, member)?;
            self.directory(&input.output_directory, member)?;
            self.market(&input.market, host, member)?;
        }
        Ok(())
    }

    fn directory(&mut self, path: &Path, member: [u8; 16]) -> Result<()> {
        let path = output_root(path)?;
        if self.directories.iter().any(|(other, owner)| {
            *owner != member && (other.starts_with(&path) || path.starts_with(other))
        }) {
            return Err(invalid(&format!(
                "multi strategy member output directory overlaps another member: {}",
                path.display()
            )));
        }
        self.directories.push((path, member));
        Ok(())
    }

    fn market(
        &mut self,
        external: &ConnectConfig,
        host: &HostConfig,
        member: [u8; 16],
    ) -> Result<()> {
        if external.cursor_id == [0; 16] || external.request_id == [0; 16] {
            return Err(invalid(
                "shared market cursor and request IDs must be nonzero",
            ));
        }
        let h = ResourceHeader::decode(&external.bank_header)
            .map_err(|_| invalid("external market resource"))?;
        let bank = BankKey {
            resource: u128::from_le_bytes(h.resource_id),
            channel: u128::from_le_bytes(h.channel_id),
            channel_epoch: h.channel_epoch,
            ring_epoch: h.ring_epoch,
            feed_generation: h.feed_generation,
        };
        // The same MD process owns the request ledger across its topics. It registers
        // each subscriber under the authenticated supervisor's control session.
        let request = tickwire_control::RequestKey {
            subject: host.supervisor.instance,
            epoch: external.endpoint.control_epoch,
            request_id: external.request_id,
        };
        let server_pid = external.endpoint.peer_pid;
        for other in self
            .markets
            .iter()
            .filter(|x| x.member != member && x.server_pid == server_pid)
        {
            if other.bank == bank && other.cursor == external.cursor_id {
                return Err(invalid(
                    "shared market bank requires distinct member cursors",
                ));
            }
            if other.request == request {
                return Err(invalid(
                    "shared market server requires distinct supervisor requests",
                ));
            }
        }
        self.markets.push(MarketClaim {
            member,
            server_pid,
            bank,
            cursor: external.cursor_id,
            request,
        });
        Ok(())
    }
}

// Output directories can be created during launch. Resolve existing parents (including
// symlinks) without creating any part of the path during inspection.
fn output_root(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid("multi strategy output directory must be absolute"));
    }
    let mut resolved = PathBuf::new();
    for component in path.components() {
        if component == Component::ParentDir {
            resolved.pop();
            continue;
        }
        resolved.push(component);
        match fs::symlink_metadata(&resolved) {
            Ok(_) => resolved = fs::canonicalize(&resolved)?,
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(resolved)
}
