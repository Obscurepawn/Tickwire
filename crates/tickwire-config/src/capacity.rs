//! Resource arithmetic over pinned inventory and retained allocations.
//! Assessments carry content identities, never deployment authorization.
use crate::{Candidate, Error, Format, Hash, Limits, Result, artifacts::ArtifactCatalog};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Allocation {
    resource_id: String,
    host_id: String,
    shared_bytes: u64,
    private_bytes: u64,
    cpus: Vec<u32>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedDocument {
    schema: u32,
    cluster_id: String,
    namespace_id: String,
    revision: String,
    allocations: Vec<Allocation>,
}
pub struct RetainedResources {
    document: RetainedDocument,
    hash: Hash,
    revision: u64,
}
impl RetainedResources {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let parsed = strict(bytes)?;
        let document: RetainedDocument =
            serde_json::from_value(parsed.value().clone()).map_err(|_| Error::Schema)?;
        require(document.schema == 1, "retained schema")?;
        identity(&document.cluster_id, &document.namespace_id)?;
        let revision = document
            .revision
            .parse::<u64>()
            .map_err(|_| Error::Revision)?;
        if revision == 0 || revision >= 1 << 63 || revision.to_string() != document.revision {
            return Err(Error::Revision);
        }
        if document.allocations.len() > 4096 {
            return Err(Error::Capacity);
        }
        let mut ids = BTreeSet::new();
        for allocation in &document.allocations {
            require(
                valid_id(&allocation.resource_id)
                    && valid_id(&allocation.host_id)
                    && ids.insert(&allocation.resource_id),
                "retained identity",
            )?;
            require(
                allocation.cpus.len() <= 4096
                    && allocation.cpus.iter().all(|cpu| *cpu <= 4095)
                    && allocation.cpus.iter().collect::<BTreeSet<_>>().len()
                        == allocation.cpus.len(),
                "retained cpu",
            )?;
        }
        Ok(Self {
            document,
            hash: parsed.hash(),
            revision,
        })
    }
    pub fn hash(&self) -> Hash {
        self.hash
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
}
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cpu {
    cpu_id: u32,
    package_id: u32,
    core_id: u32,
    numa_node: u32,
}
impl Cpu {
    fn physical(&self) -> (u32, u32) {
        (self.package_id, self.core_id)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Host {
    id: String,
    failure_domain: String,
    memory_bytes: u64,
    system_reserved_bytes: u64,
    shm_capacity_bytes: u64,
    cpus: Vec<Cpu>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    schema: u32,
    cluster_id: String,
    namespace_id: String,
    hosts: Vec<Host>,
}
/// Strict inventory content; capacity and source authorization are separate.
pub struct HostInventory {
    document: Inventory,
}
impl HostInventory {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let document: Inventory =
            serde_json::from_value(strict(bytes)?.value().clone()).map_err(|_| Error::Schema)?;
        require(document.schema == 1, "inventory schema")?;
        identity(&document.cluster_id, &document.namespace_id)?;
        if document.hosts.is_empty() || document.hosts.len() > 256 {
            return Err(Error::Capacity);
        }
        let mut ids = BTreeSet::new();
        for host in &document.hosts {
            require(
                valid_id(&host.id) && valid_id(&host.failure_domain) && ids.insert(&host.id),
                "inventory hosts",
            )?;
        }
        Ok(Self { document })
    }
    pub fn cluster_id(&self) -> &str {
        &self.document.cluster_id
    }
    pub fn namespace_id(&self) -> &str {
        &self.document.namespace_id
    }
    pub fn host_ids(&self) -> impl Iterator<Item = &str> {
        self.document.hosts.iter().map(|host| host.id.as_str())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostCapacity {
    pub shared_bytes: u64,
    pub private_bytes: u64,
    pub memory_commitment_bytes: u64,
}
pub struct CapacityAssessment {
    candidate_hash: Hash,
    inventory_hash: Hash,
    retained_hash: Hash,
    hosts: BTreeMap<String, HostCapacity>,
}
impl CapacityAssessment {
    pub fn check(
        candidate: &Candidate,
        catalog: &ArtifactCatalog,
        retained: &RetainedResources,
    ) -> Result<Self> {
        let resources = candidate.validate_static()?;
        let value = candidate.value();
        let reference = value["qualification"]["host_inventory_ref"]
            .as_str()
            .ok_or(Error::Schema)?;
        let bytes = catalog
            .get(reference)
            .ok_or(Error::Semantic("missing host inventory"))?;
        let inventory = HostInventory::decode(bytes)?.document;
        let expected = &value["identity"];
        require(
            expected["cluster_id"] == inventory.cluster_id
                && expected["namespace_id"] == inventory.namespace_id
                && retained.document.cluster_id == inventory.cluster_id
                && retained.document.namespace_id == inventory.namespace_id,
            "inventory identity",
        )?;
        if inventory.hosts.is_empty() || inventory.hosts.len() > 256 {
            return Err(Error::Capacity);
        }
        let mut by_host = BTreeMap::new();
        for host in &inventory.hosts {
            require(
                valid_id(&host.id)
                    && valid_id(&host.failure_domain)
                    && by_host.insert(host.id.as_str(), host).is_none(),
                "inventory hosts",
            )?;
        }
        let configured_hosts = array(value, "hosts")?;
        require(
            configured_hosts.len() == by_host.len()
                && retained
                    .document
                    .allocations
                    .iter()
                    .all(|a| by_host.contains_key(a.host_id.as_str())),
            "inventory host set",
        )?;
        let mut hosts = BTreeMap::new();
        for configured in configured_hosts {
            let id = string(configured, "id")?;
            let host = by_host
                .get(id)
                .ok_or(Error::Semantic("inventory host set"))?;
            let shared = if configured["role"] == "trading" {
                resources.total_shared_bytes
            } else {
                0
            };
            hosts.insert(
                id.to_owned(),
                host_capacity(
                    configured,
                    host,
                    array(value, "processes")?,
                    &retained.document.allocations,
                    shared,
                )?,
            );
        }
        Ok(Self {
            candidate_hash: candidate.hash(),
            inventory_hash: Sha256::digest(bytes).into(),
            retained_hash: retained.hash(),
            hosts,
        })
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn inventory_hash(&self) -> Hash {
        self.inventory_hash
    }
    pub fn retained_hash(&self) -> Hash {
        self.retained_hash
    }
    pub fn hosts(&self) -> &BTreeMap<String, HostCapacity> {
        &self.hosts
    }
}

fn host_capacity(
    configured: &Value,
    host: &Host,
    processes: &[Value],
    retained: &[Allocation],
    mut shared_bytes: u64,
) -> Result<HostCapacity> {
    require(
        configured["failure_domain"] == host.failure_domain
            && host.system_reserved_bytes < host.memory_bytes,
        "inventory host",
    )?;
    let usable = host.memory_bytes - host.system_reserved_bytes;
    let memory_budget = number(configured, "memory_bytes")?;
    let shm_budget = number(configured, "shm_budget_bytes")?;
    require(
        memory_budget <= usable
            && shm_budget <= host.shm_capacity_bytes
            && host.shm_capacity_bytes <= usable,
        "inventory memory",
    )?;
    if host.cpus.is_empty() || host.cpus.len() > 4096 {
        return Err(Error::Capacity);
    }
    let mut cpus = BTreeMap::new();
    let mut physical = BTreeMap::new();
    let mut nodes = BTreeSet::new();
    for cpu in &host.cpus {
        require(
            cpu.cpu_id <= 4095
                && cpu.package_id <= 65535
                && cpu.core_id <= 65535
                && cpu.numa_node <= 31
                && cpus.insert(cpu.cpu_id, cpu).is_none(),
            "inventory cpu",
        )?;
        if let Some(node) = physical.insert(cpu.physical(), cpu.numa_node) {
            require(node == cpu.numa_node, "inventory physical numa")?;
        }
        nodes.insert(cpu.numa_node);
    }
    require(
        physical.len() as u64 >= number(configured, "physical_cores")?
            && nodes.len() as u64 >= number(configured, "numa_nodes")?,
        "inventory topology",
    )?;
    let mut housekeeping = BTreeSet::new();
    let mut claimed = BTreeSet::new();
    for cpu in array(configured, "housekeeping_cpus")? {
        let cpu = cpu.as_u64().ok_or(Error::Schema)? as u32;
        let mapped = cpus
            .get(&cpu)
            .ok_or(Error::Semantic("inventory missing cpu"))?;
        housekeeping.insert(cpu);
        claimed.insert(mapped.physical());
    }
    let mut private_bytes = 0;
    for process in processes.iter().filter(|p| p["host_id"] == host.id) {
        private_bytes = add(private_bytes, number(process, "memory_limit_bytes")?)?;
        for cpu in array(process, "cpus")? {
            let cpu = cpu.as_u64().ok_or(Error::Schema)? as u32;
            let mapped = cpus
                .get(&cpu)
                .ok_or(Error::Semantic("inventory missing cpu"))?;
            require(
                u64::from(mapped.numa_node) == number(process, "numa_node")?,
                "inventory process numa",
            )?;
            require(
                if process["role"] == "supervisor" {
                    housekeeping.contains(&cpu)
                } else {
                    claimed.insert(mapped.physical())
                },
                "inventory physical core sharing",
            )?;
        }
    }
    for allocation in retained.iter().filter(|a| a.host_id == host.id) {
        shared_bytes = add(shared_bytes, allocation.shared_bytes)?;
        private_bytes = add(private_bytes, allocation.private_bytes)?;
        for cpu in &allocation.cpus {
            let mapped = cpus
                .get(cpu)
                .ok_or(Error::Semantic("retained missing cpu"))?;
            require(
                claimed.insert(mapped.physical()),
                "retained physical core sharing",
            )?;
        }
    }
    require(
        shared_bytes <= shm_budget && shared_bytes <= host.shm_capacity_bytes,
        "inventory shared capacity",
    )?;
    // Reserve the complete configured SHM budget, including its unused slack.
    // Extra shared allocations consume that budget and are not added twice.
    let memory_commitment_bytes = add(private_bytes, shm_budget)?;
    require(
        memory_commitment_bytes <= memory_budget && memory_commitment_bytes <= usable,
        "inventory memory capacity",
    )?;
    Ok(HostCapacity {
        shared_bytes,
        private_bytes,
        memory_commitment_bytes,
    })
}
fn strict(bytes: &[u8]) -> Result<Candidate> {
    Candidate::parse(
        Format::Json,
        bytes,
        Limits {
            max_bytes: 4 * 1024 * 1024,
            max_depth: 16,
        },
    )
}
fn require(condition: bool, check: &'static str) -> Result<()> {
    condition.then_some(()).ok_or(Error::Semantic(check))
}
fn valid_id(s: &str) -> bool {
    (1..=63).contains(&s.len())
        && s.as_bytes()[0].is_ascii_lowercase()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
fn identity(cluster: &str, namespace: &str) -> Result<()> {
    require(
        valid_id(cluster)
            && namespace.len() == 36
            && namespace.bytes().enumerate().all(|(i, b)| {
                if [8, 13, 18, 23].contains(&i) {
                    b == b'-'
                } else {
                    b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
                }
            }),
        "inventory identity",
    )
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Error::Capacity)
}
fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key].as_str().ok_or(Error::Schema)
}
fn number(v: &Value, key: &str) -> Result<u64> {
    v[key].as_u64().ok_or(Error::Schema)
}
fn array<'a>(v: &'a Value, key: &str) -> Result<&'a [Value]> {
    v[key].as_array().map(Vec::as_slice).ok_or(Error::Schema)
}
