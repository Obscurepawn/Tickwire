//! An isolated replay account. It consumes canonical execution records and has
//! no reference to the real account owner or its WAL writer.
use super::{Error, Result};
use serde::{Deserialize, Serialize};
use tickwire_account::{Account, RecoveryRecord};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord, InputSeal};
use tickwire_release::replay::{MAX_BYTES, StreamKey, canonical};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    schema: u16,
    baseline: Vec<u8>,
    records: Vec<Vec<u8>>,
}
pub struct AccountReplica {
    saved: Saved,
    baseline: CanonicalBaseline,
    records: Vec<RecoveryRecord>,
    seal: InputSeal,
    account: Account,
    bytes: usize,
}
impl AccountReplica {
    pub fn stream(baseline: &CanonicalBaseline) -> StreamKey {
        StreamKey {
            identity: baseline.digest(),
            generation: baseline.snapshot().account_epoch,
        }
    }
    pub fn from_baseline(stream: &StreamKey, bytes: &[u8]) -> Result<Self> {
        let baseline = CanonicalBaseline::decode(bytes).map_err(|_| Error::Identity)?;
        if *stream != Self::stream(&baseline) {
            return Err(Error::Identity);
        }
        let account =
            Account::recover(baseline.snapshot().clone(), baseline.catalog().clone(), &[])
                .map_err(|_| Error::Callback)?;
        let seal = InputSeal::new(baseline.digest()).map_err(|_| Error::Identity)?;
        Ok(Self {
            saved: Saved {
                schema: 1,
                baseline: bytes.to_vec(),
                records: vec![],
            },
            baseline,
            records: vec![],
            seal,
            account,
            bytes: bytes.len(),
        })
    }
    pub fn restore(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        let saved: Saved = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
        if saved.schema != 1
            || saved.records.len() > tickwire_release::replay::MAX_ROWS
            || canonical(&saved)? != bytes
        {
            return Err(Error::Identity);
        }
        let baseline = CanonicalBaseline::decode(&saved.baseline).map_err(|_| Error::Identity)?;
        let mut replica = Self::from_baseline(&Self::stream(&baseline), &saved.baseline)?;
        for raw in &saved.records {
            let r = CanonicalRecord::decode(raw).map_err(|_| Error::Identity)?;
            replica.apply(&Self::stream(&baseline), r.sequence(), raw)?;
        }
        Ok(replica)
    }
    pub fn apply(
        &mut self,
        stream: &StreamKey,
        position: u64,
        bytes: &[u8],
    ) -> Result<crate::reports::AccountReport> {
        let r = CanonicalRecord::decode(bytes).map_err(|_| Error::Identity)?;
        if *stream != Self::stream(&self.baseline)
            || position != r.sequence()
            || self.seal.position().checked_add(1) != Some(position)
            || r.baseline() != self.baseline.digest()
        {
            return Err(Error::Sequence);
        }
        let next = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_BYTES)
            .ok_or(Error::Capacity)?;
        if self.saved.records.len() == tickwire_release::replay::MAX_ROWS {
            return Err(Error::Capacity);
        }
        let mut records = self.records.clone();
        records.push(r.record().clone());
        let account = Account::recover(
            self.baseline.snapshot().clone(),
            self.baseline.catalog().clone(),
            &records,
        )
        .map_err(|_| Error::Callback)?;
        let mut seal = self.seal.clone();
        seal.apply(&r).map_err(|_| Error::Identity)?;
        let mut saved = self.saved.clone();
        saved.records.push(bytes.to_vec());
        canonical(&saved)?;
        self.saved = saved;
        self.bytes = next;
        self.records = records;
        self.account = account;
        self.seal = seal;
        Ok(crate::reports::AccountReport::Record {
            position,
            record: r,
        })
    }
    pub fn checkpoint(&self) -> Result<Vec<u8>> {
        Ok(canonical(&self.saved)?)
    }
    pub fn position(&self) -> u64 {
        self.seal.position()
    }
    pub fn seal(&self) -> &InputSeal {
        &self.seal
    }
    pub fn account(&self) -> &Account {
        &self.account
    }
    pub fn baseline(&self) -> &CanonicalBaseline {
        &self.baseline
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_canonical_baseline_restores_without_mutating_execution_account() {
        let (snapshot, catalog, _) = crate::account_fixture::fixture();
        let actual = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
        let base = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let replica =
            AccountReplica::from_baseline(&AccountReplica::stream(&base), base.bytes()).unwrap();
        assert_eq!(
            replica.account().ledger_sequence(),
            actual.ledger_sequence()
        );
        assert_eq!(
            replica.account().strategy_orders_digest(8),
            actual.strategy_orders_digest(8)
        );
        assert_eq!(replica.position(), 0);
        assert_eq!(
            AccountReplica::restore(&replica.checkpoint().unwrap())
                .unwrap()
                .seal()
                .digest(),
            replica.seal().digest()
        );
    }
    #[test]
    fn real_admission_and_fill_replay_once_and_reject_a_duplicate_callback_position() {
        use crate::account_fixture as f;
        let (snapshot, catalog, command) = f::fixture();
        let base = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
        let mut actual = Account::from_snapshot(snapshot, catalog).unwrap();
        let c = f::canonical(command.clone());
        let a = actual.admit(&c, 50).unwrap();
        let admit = CanonicalRecord::new(
            base.digest(),
            1,
            RecoveryRecord::Admit {
                command: Box::new(c),
                evaluated_at: 50,
                expected: a.image().clone(),
            },
        )
        .unwrap();
        let event = f::event(f::fill(&command, "trade1", "0.0005", "60000", "30"));
        actual.apply_event(&event).unwrap();
        let fill = CanonicalRecord::new(
            base.digest(),
            2,
            RecoveryRecord::VenueEvent(Box::new(event)),
        )
        .unwrap();
        let stream = AccountReplica::stream(&base);
        let mut replica = AccountReplica::from_baseline(&stream, base.bytes()).unwrap();
        replica.apply(&stream, 1, admit.bytes()).unwrap();
        replica.apply(&stream, 2, fill.bytes()).unwrap();
        assert_eq!(
            replica.account().ledger_sequence(),
            actual.ledger_sequence()
        );
        assert_eq!(
            replica.account().strategy_orders_digest(8),
            actual.strategy_orders_digest(8)
        );
        for key in [f::key(1), f::key(2)] {
            assert_eq!(
                replica.account().asset(key).unwrap(),
                actual.asset(key).unwrap()
            );
        }
        let before = replica.checkpoint().unwrap();
        assert!(replica.apply(&stream, 2, fill.bytes()).is_err());
        assert_eq!(replica.checkpoint().unwrap(), before);
        let restored = AccountReplica::restore(&before).unwrap();
        assert_eq!(restored.seal().digest(), replica.seal().digest());
        assert_eq!(
            restored.account().strategy_orders_digest(8),
            actual.strategy_orders_digest(8)
        );
    }
}
