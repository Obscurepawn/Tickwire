//! Original capture dependency graph. Runtime session/market qualification stays
//! with SDK/Host; historical recovery must still retain every committed edge.
use super::*;
use tickwire_ipc::{ResourceHeader, Wire};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Captured {
    schema: u16,
    attempt: Object,
    identity: window::Identity,
    contract: Object,
    seed: Object,
    inputs: Object,
    seal: Object,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    schema: u16,
    identity: window::Identity,
    contract: Object,
    seed: Object,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema: u16,
    bootstrap: Object,
    request: Object,
    completion: Captured,
    pid: u32,
    report_ring_sequence: u64,
    market_applied: u64,
    observed_mono_ns: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SealRequest {
    schema: u16,
    capture: [u8; 16],
    bootstrap: Object,
    contract: Object,
    account_position: u64,
    deadline_mono_ns: u64,
}
// Content-bearing fields only. SDK owns the remaining OS-specific endpoint,
// path and polling fields; this view neither opens paths nor attests sessions.
#[derive(Deserialize)]
struct Bootstrap {
    schema: u16,
    identity: window::Identity,
    contract: Contract,
    config: replay::VersionedBytes,
    models: Vec<replay::Model>,
    report_header: Vec<u8>,
    initial_account_position: u64,
    deadline_mono_ns: u64,
}
fn decode<T: serde::de::DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T> {
    let value: T = super::super::super::decode(bytes)?;
    if replay::canonical(&value)? != bytes {
        return Err(Error::Encoding);
    }
    Ok(value)
}
pub(super) fn verify(
    installation: &Installation,
    request: &Request,
    contract: &Contract,
    inputs: &Inputs,
    seal: &window::Seal,
    get: &mut dyn FnMut(Object) -> Result<Vec<u8>>,
) -> Result<()> {
    let completion: Captured = decode(&get(request.capture_completion)?)?;
    let receipt: Receipt = decode(&get(request.capture_receipt)?)?;
    let attempt: Attempt = decode(&get(completion.attempt)?)?;
    receipt.bootstrap.validate(2 * 1024 * 1024)?;
    receipt.request.validate(2 * 1024 * 1024)?;
    let sealed: SealRequest = decode(&get(receipt.request)?)?;
    let bootstrap: Bootstrap =
        serde_json::from_slice(&get(receipt.bootstrap)?).map_err(|_| Error::Encoding)?;
    let mut seed = inputs.clone();
    seed.warm.clear();
    if get(completion.seed)? != seed.bytes()?
        || completion.schema != 1
        || completion.identity != seal.identity
        || completion.contract != seal.contract
        || completion.seed != seal.seed
        || completion.inputs != seal.inputs
        || completion.seal != request.capture_seal
        || attempt.schema != 1
        || attempt.identity != completion.identity
        || attempt.contract != completion.contract
        || attempt.seed != completion.seed
        || receipt.schema != 1
        || receipt.completion != completion
        || receipt.pid != installation.pid
        || receipt.bootstrap != sealed.bootstrap
        || receipt.market_applied != seal.end.markets[0].sequence
        || receipt.observed_mono_ns < seal.end.now_ns
        || receipt.observed_mono_ns > installation.completed_ns
        || receipt.observed_mono_ns >= sealed.deadline_mono_ns
        || sealed.schema != 1
        || sealed.capture != seal.identity.capture
        || sealed.contract != seal.contract
        || sealed.account_position != seal.end.accounts[0].sequence
        || sealed.deadline_mono_ns > bootstrap.deadline_mono_ns
        || request.deadline_mono_ns > bootstrap.deadline_mono_ns
        || bootstrap.schema != 1
        || bootstrap.identity != seal.identity
        || bootstrap.contract != *contract
        || bootstrap.config != seed.config
        || bootstrap.models != seed.models
        || bootstrap.initial_account_position != seal.start.accounts[0].sequence
    {
        return Err(Error::Identity);
    }
    let header = ResourceHeader::decode(&bootstrap.report_header).map_err(|_| Error::Identity)?;
    if receipt.report_ring_sequence
        != header
            .first_seq
            .checked_add(sealed.account_position)
            .ok_or(Error::Bounds)?
    {
        return Err(Error::Identity);
    }
    let mut chain = [0; 32];
    for (i, row) in inputs.warm.iter().enumerate() {
        chain = window::next_chain(chain, i as u64 + 1, row)?;
    }
    if chain != seal.input_chain {
        return Err(Error::Identity);
    }
    Ok(())
}
