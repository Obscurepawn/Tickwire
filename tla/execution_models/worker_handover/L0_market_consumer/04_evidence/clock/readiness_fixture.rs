
#[cfg(test)]
pub(crate) fn market_clock_fixture(identity: Identity) -> ReadyProof {
    ReadyProof {
        fresh: Default::default(), deferred: None, resources: None, context: None,
        cache: Default::default(), directory: None, identity: identity.clone(),
        last: None, account_input: None, receipt: PathBuf::new(), exec_identity: identity,
        confirmed: None, seal_sequence: 0,
    }
}
