mod common;
use common::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_account::{Account, RecoveryRecord};
use tickwire_account_codec::*;
use tickwire_journal::{Config, Journal, store::FileStore};
use tickwire_recovery::{BlobStore, FileBlobStore, StoreKey};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "tickwire-account-codec-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
#[test]
fn real_blob_and_wal_reopen_restore_spot_and_usdm_resources() {
    for (snapshot, catalog, cmd) in [fixture(), futures_fixture()] {
        let temp = Temp::new();
        let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let mut live =
            Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone())
                .unwrap();
        let command = canonical(cmd.clone());
        let admission = live.admit(&command, 60).unwrap();
        let fill = if cmd.context.request.product == tickwire_venue::Product::Spot {
            event(fill(&cmd, "stored-trade", "0.0004", "50000", "20"))
        } else {
            event(fill(&cmd, "stored-trade", "0.4", "100", "40"))
        };
        live.apply_event(&fill).unwrap();
        let inputs = [
            RecoveryRecord::Admit {
                command: Box::new(command),
                evaluated_at: 60,
                expected: admission.image(),
            },
            RecoveryRecord::VenueEvent(Box::new(fill)),
        ];
        let mut blobs = FileBlobStore::open(temp.0.join("objects")).unwrap();
        blobs
            .put_sync(StoreKey::Object(baseline.digest()), baseline.bytes())
            .unwrap();
        let mut journal = Journal::create(
            FileStore::open(temp.0.join("wal")).unwrap(),
            Config::new([2; 16], [1; 16]),
        )
        .unwrap();
        let mut required = None;
        for (index, input) in inputs.into_iter().enumerate() {
            let encoded = CanonicalRecord::new(baseline.digest(), index as u64 + 1, input).unwrap();
            let receipt = journal.append_batch(5, &[encoded.bytes()]).unwrap();
            required = Some(receipt.frontier().clone());
        }
        drop(journal);
        drop(blobs);
        let blobs = FileBlobStore::open(temp.0.join("objects")).unwrap();
        let loaded = CanonicalBaseline::decode(
            &blobs
                .get(StoreKey::Object(baseline.digest()), MAX_BASELINE_BYTES)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(loaded.digest(), baseline.digest());
        let (_journal, recovery) = Journal::recover(
            FileStore::open(temp.0.join("wal")).unwrap(),
            required.as_ref(),
        )
        .unwrap();
        let mut decoded = Vec::new();
        for record in recovery.records {
            let encoded = CanonicalRecord::decode(&record.payload).unwrap();
            assert_eq!(encoded.sequence(), record.seq);
            assert_eq!(encoded.baseline(), loaded.digest());
            decoded.push(encoded.record().clone());
        }
        let restored = Account::recover(
            loaded.snapshot().clone(),
            loaded.catalog().clone(),
            &decoded,
        )
        .unwrap();
        assert!(restored.frozen());
        assert_eq!(restored.asset(key(1)).unwrap(), live.asset(key(1)).unwrap());
        assert_eq!(restored.asset(key(2)).unwrap(), live.asset(key(2)).unwrap());
        assert_eq!(
            restored.admission(admission.intent()).unwrap().image(),
            admission.image()
        );
        assert!(restored.order(&admission.orders()[0]).unwrap().unknown);
        if cmd.context.request.product != tickwire_venue::Product::Spot {
            assert_eq!(
                restored.venue_shadow(1).unwrap(),
                live.venue_shadow(1).unwrap()
            );
            assert_eq!(
                restored.position(12, tickwire_venue::PositionSide::Both),
                live.position(12, tickwire_venue::PositionSide::Both)
            );
        }
    }
}
