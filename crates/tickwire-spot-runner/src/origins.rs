//! Immutable, account-scoped order aliases. A crash can leave a reserved alias; it cannot
//! make an unsent command appear admitted. Consumers always check the authoritative account WAL.
use super::{Error, Result};
use tickwire_account::{IntentKey, Scope};
use tickwire_recovery::{BlobStore, StoreKey, digest};
pub(super) struct Origins<S>(pub S);
pub(super) struct Origin {
    pub intent: IntentKey,
    pub order: [u8; 16],
    pub digest: [u8; 32],
}
fn key(scope: &Scope, kind: &[u8], id: &[u8]) -> StoreKey {
    let mut bytes = b"tickwire.spot-order-origin.v1".to_vec();
    bytes.extend(scope.risk_domain_id);
    bytes.extend(scope.venue.to_be_bytes());
    bytes.extend(scope.account.to_be_bytes());
    bytes.push(
        if scope.environment == tickwire_venue::Environment::Production {
            1
        } else {
            2
        },
    );
    bytes.extend(kind);
    bytes.push(0);
    bytes.extend(id);
    StoreKey::Binding(digest(&bytes))
}
impl<S: BlobStore> Origins<S> {
    pub fn register(
        &mut self,
        scope: &Scope,
        intent: IntentKey,
        client: &str,
        order: [u8; 16],
        command: [u8; 32],
    ) -> Result<()> {
        let mut data = [0; 64];
        data[..8].copy_from_slice(&intent.epoch.to_be_bytes());
        data[8..16].copy_from_slice(&intent.intent_id.to_be_bytes());
        data[16..32].copy_from_slice(&order);
        data[32..].copy_from_slice(&command);
        let hash = digest(&data);
        self.0
            .put_sync(StoreKey::Object(hash), &data)
            .map_err(Error::Storage)?;
        for alias in [
            key(scope, b"intent", &data[..16]),
            key(scope, b"order", &order),
            key(scope, b"client", client.as_bytes()),
        ] {
            self.0.put_sync(alias, &hash).map_err(Error::Storage)?;
        }
        Ok(())
    }
    pub fn lookup(&self, scope: &Scope, kind: &[u8], id: &[u8]) -> Result<Origin> {
        let hash: [u8; 32] = self
            .0
            .get(key(scope, kind, id), 32)
            .map_err(Error::Storage)?
            .try_into()
            .map_err(|_| Error::Origin("invalid order locator"))?;
        let data = self
            .0
            .get(StoreKey::Object(hash), 64)
            .map_err(Error::Storage)?;
        if data.len() != 64 || digest(&data) != hash {
            return Err(Error::Origin("corrupt order origin"));
        }
        Ok(Origin {
            intent: IntentKey {
                epoch: u64::from_be_bytes(data[..8].try_into().unwrap()),
                intent_id: u64::from_be_bytes(data[8..16].try_into().unwrap()),
            },
            order: data[16..32].try_into().unwrap(),
            digest: data[32..64].try_into().unwrap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_retries_preserve_order_bindings_and_venue_namespaces() {
        let mut index = Origins(tickwire_recovery::MockBlobStore::default());
        let mut scope = Scope {
            risk_domain_id: [4; 16],
            venue: 2,
            account: 1,
            environment: tickwire_venue::Environment::Test,
        };
        let intent = IntentKey {
            epoch: 5,
            intent_id: 10,
        };
        for _ in 0..2 {
            index
                .register(&scope, intent, "workerA", [1; 16], [7; 32])
                .unwrap();
        }
        let other = IntentKey {
            epoch: 5,
            intent_id: 11,
        };
        assert!(
            index
                .register(&scope, other, "workerA", [2; 16], [8; 32])
                .is_err()
        );
        assert_eq!(
            index.lookup(&scope, b"client", b"workerA").unwrap().intent,
            intent
        );
        assert!(
            index
                .register(&scope, intent, "workerB", [1; 16], [9; 32])
                .is_err()
        );
        assert_eq!(
            index.lookup(&scope, b"order", &[1; 16]).unwrap().digest,
            [7; 32]
        );
        scope.venue = 3;
        assert!(index.lookup(&scope, b"client", b"workerA").is_err());
        index
            .register(&scope, other, "workerA", [2; 16], [8; 32])
            .unwrap();
    }
}
