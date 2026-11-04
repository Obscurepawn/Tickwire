//! Cold-start ownership checks shared by deployment inspection and launch.
mod ownership;

use crate::{
    Result,
    config::read_file,
    hosted::HostConfig,
    invalid,
    strategy::{AppStrategy, Config, FileStrategy, MemberConfig},
};
use serde::Serialize;
use std::collections::BTreeSet;
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_runtime::files;
use tickwire_runtime_policy::ControlPlane;
use tickwire_strategy_sdk::bootstrap::Bootstrap;

#[derive(Serialize)]
pub struct Slot {
    pub logical_strategy_id: u64,
    pub mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<[u8; 16]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
}

enum Source<'a> {
    Hosted(&'a HostConfig, Box<Bootstrap>, Option<Box<ResourceHeader>>),
    Local(FileStrategy),
}

struct Prepared<'a> {
    slot: Slot,
    source: Source<'a>,
}

fn prepare(members: &[MemberConfig], risk_domain: [u8; 16]) -> Result<Vec<Prepared<'_>>> {
    if members.is_empty() || members.len() > 32 {
        return Err(invalid("multi strategy requires 1 through 32 members"));
    }
    let mut slots = BTreeSet::new();
    let mut instances = BTreeSet::new();
    let mut topics = BTreeSet::new();
    let mut ownership = ownership::Ownership::default();
    let mut prepared = Vec::with_capacity(members.len());
    for member in members {
        let id = member.logical_strategy_id;
        if id == 0 || !slots.insert(id) {
            return Err(invalid(
                "each strategy slot requires a distinct nonzero logical ID",
            ));
        }
        let mut slot = Slot {
            logical_strategy_id: id,
            mode: "observe",
            instance: None,
            generation: None,
        };
        let source = match member.strategy.as_ref() {
            Config::Multi { .. } => return Err(invalid("multi strategy groups cannot nest")),
            Config::Hosted { config } => {
                let bootstrap: Bootstrap =
                    serde_json::from_slice(&read_file(&config.bootstrap, 1024 * 1024)?)?;
                let identity = &bootstrap.market.local;
                if identity.logical_strategy != id || bootstrap.risk_domain != risk_domain {
                    return Err(invalid(
                        "strategy member does not match its logical strategy or risk domain",
                    ));
                }
                if identity.instance == [0; 16]
                    || identity.generation == 0
                    || !instances.insert(identity.instance)
                {
                    return Err(invalid(
                        "multi strategy requires distinct, nonzero hosted instances and nonzero generations",
                    ));
                }
                ownership.claim(config, &bootstrap)?;
                let local_header = if let Some(external) = &bootstrap.external_market {
                    if external.endpoint.local.identity() != identity.identity() {
                        return Err(invalid("external market strategy identity mismatch"));
                    }
                    None
                } else {
                    let header = ResourceHeader::decode(&read_file(&config.market_resource, 4096)?)
                        .map_err(|_| invalid("market resource header"))?;
                    if !topics.insert(header.topic_id) {
                        return Err(invalid(
                            "independent local publishers require distinct topic IDs",
                        ));
                    }
                    Some(Box::new(header))
                };
                slot.mode = "hosted";
                slot.instance = Some(identity.instance);
                slot.generation = Some(identity.generation);
                Source::Hosted(config, Box::new(bootstrap), local_header)
            }
            local => {
                if matches!(local, Config::FileQueue { .. }) {
                    slot.mode = "file_queue";
                }
                Source::Local(FileStrategy::new(local)?)
            }
        };
        prepared.push(Prepared { slot, source });
    }
    Ok(prepared)
}

pub fn inspect(members: &[MemberConfig], risk_domain: [u8; 16]) -> Result<Vec<Slot>> {
    Ok(prepare(members, risk_domain)?
        .into_iter()
        .map(|member| member.slot)
        .collect())
}

pub(super) fn build(
    members: &[MemberConfig],
    runtime: &files::Configuration,
    plane: ControlPlane,
) -> Result<Box<dyn AppStrategy>> {
    let prepared = prepare(members, runtime.command_binding.risk_domain_id)?;
    let mut children = Vec::with_capacity(prepared.len());
    for member in prepared {
        let owner: Box<dyn AppStrategy> = match member.source {
            Source::Hosted(config, bootstrap, header) => crate::hosted::build_prepared(
                config,
                runtime,
                plane.clone(),
                *bootstrap,
                header.map(|h| *h),
            )?,
            Source::Local(strategy) => Box::new(strategy),
        };
        children.push((member.slot.logical_strategy_id, owner));
    }
    Ok(Box::new(crate::strategy::MultiStrategy::new(
        children, plane,
    )?))
}
