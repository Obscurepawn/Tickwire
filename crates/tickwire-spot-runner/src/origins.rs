//! Immutable, account-scoped order aliases. A crash can leave a reserved alias; it cannot
//! make an unsent command appear admitted. Consumers always check the authoritative account WAL.
use super::{Error, Result};
use std::collections::BTreeMap;
use tickwire_account::{IntentKey, Scope};
use tickwire_recovery::{BlobStore, StoreKey, digest};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Origin {
    pub intent: IntentKey,
    pub order: [u8; 16],
    pub digest: [u8; 32],
}

pub(super) struct Origins<S> {
    store: S,
    cache: BTreeMap<[u8; 32], Origin>,
}

fn locator(scope: &Scope, kind: &[u8], id: &[u8]) -> [u8; 32] {
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
    digest(&bytes)
}

impl<S: BlobStore> Origins<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            cache: BTreeMap::new(),
        }
    }

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

        let entry = Origin {
            intent,
            order,
            digest: command,
        };
        let locators = [
            locator(scope, b"intent", &data[..16]),
            locator(scope, b"order", &order),
            locator(scope, b"client", client.as_bytes()),
        ];
        for loc in &locators {
            if let Some(existing) = self.cache.get(loc)
                && *existing != entry
            {
                return Err(Error::Origin("conflicting cached order origin"));
            }
        }
        if locators.iter().all(|loc| self.cache.contains_key(loc)) {
            return Ok(());
        }

        let hash = digest(&data);
        self.store
            .put_sync(StoreKey::Object(hash), &data)
            .map_err(Error::Storage)?;
        for loc in locators {
            self.store
                .put_sync(StoreKey::Binding(loc), &hash)
                .map_err(Error::Storage)?;
            self.cache.insert(loc, entry);
        }
        Ok(())
    }

    pub fn lookup(&self, scope: &Scope, kind: &[u8], id: &[u8]) -> Result<Origin> {
        let loc = locator(scope, kind, id);
        if let Some(origin) = self.cache.get(&loc) {
            return Ok(*origin);
        }
        let hash: [u8; 32] = self
            .store
            .get(StoreKey::Binding(loc), 32)
            .map_err(Error::Storage)?
            .try_into()
            .map_err(|_| Error::Origin("invalid order locator"))?;
        let data = self
            .store
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
        let mut index = Origins::new(tickwire_recovery::MockBlobStore::default());
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
