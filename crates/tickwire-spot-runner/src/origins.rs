//! Immutable, account-scoped order aliases. A crash can leave a reserved alias; it cannot
//! make an unsent command appear admitted. Consumers always check the authoritative account WAL.
use super::{Error, Result};
use tickwire_account::{
    Account, DEFAULT_RETENTION_WINDOW_NS, IntentKey, MAX_ORDERS, Scope, pool::WindowedTable,
};
use tickwire_recovery::{BlobStore, StoreKey, digest};

pub const DEFAULT_ORIGIN_ORDERS: usize = MAX_ORDERS;
pub const DEFAULT_ORIGIN_SLOTS: usize = DEFAULT_ORIGIN_ORDERS * 3;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Origin {
    pub intent: IntentKey,
    pub order: [u8; 16],
    pub digest: [u8; 32],
}

pub(super) struct Origins<S> {
    store: S,
    cache: WindowedTable<[u8; 32], Origin>,
    by_intent: WindowedTable<IntentKey, [[u8; 32]; 3]>,
    retention_window_ns: Option<u64>,
}

fn locator(scope: &Scope, kind: &[u8], id: &[u8]) -> [u8; 32] {
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    ctx.update(b"tickwire.spot-order-origin.v1");
    ctx.update(&scope.risk_domain_id);
    ctx.update(&scope.venue.to_be_bytes());
    ctx.update(&scope.account.to_be_bytes());
    ctx.update(&[
        if scope.environment == tickwire_venue::Environment::Production {
            1
        } else {
            2
        },
    ]);
    ctx.update(kind);
    ctx.update(&[0]);
    ctx.update(id);
    ctx.finish()
        .as_ref()
        .try_into()
        .expect("SHA-256 is 32 bytes")
}

impl<S: BlobStore> Origins<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            cache: WindowedTable::with_capacity(DEFAULT_ORIGIN_SLOTS),
            by_intent: WindowedTable::with_capacity(DEFAULT_ORIGIN_ORDERS),
            retention_window_ns: Some(DEFAULT_RETENTION_WINDOW_NS),
        }
    }

    #[cfg(test)]
    pub fn with_order_capacity(store: S, max_orders: usize) -> Self {
        let orders = max_orders.max(1);
        let slots = orders.saturating_mul(3);
        Self {
            store,
            cache: WindowedTable::with_capacity(slots),
            by_intent: WindowedTable::with_capacity(orders),
            retention_window_ns: Some(DEFAULT_RETENTION_WINDOW_NS),
        }
    }

    pub fn set_retention_window_ns(&mut self, window_ns: Option<u64>) {
        self.retention_window_ns = window_ns;
    }

    pub fn retention_window_ns(&self) -> Option<u64> {
        self.retention_window_ns
    }

    pub fn is_at_capacity(&self) -> bool {
        self.by_intent.len() >= self.by_intent.capacity()
            || self.cache.len().saturating_add(3) > self.cache.capacity()
    }

    pub fn retire_intent(&mut self, intent: IntentKey, retired_at_mono_ns: u64) -> bool {
        let Some(locators) = self.by_intent.get(&intent).copied() else {
            return false;
        };
        self.by_intent.retire(&intent, retired_at_mono_ns);
        for loc in &locators {
            self.cache.retire(loc, retired_at_mono_ns);
        }
        true
    }

    pub fn evict_expired(&mut self, now_mono_ns: u64) -> usize {
        let win = self.retention_window_ns;
        self.by_intent.evict_expired(now_mono_ns, win);
        self.cache.evict_expired(now_mono_ns, win)
    }

    pub fn sync_account(&mut self, account: &Account, now_mono_ns: u64) {
        let mut to_retire = Vec::new();
        for (intent, _, retired_at) in self.by_intent.iter() {
            if retired_at.is_none() && !account.is_intent_active(*intent) {
                let stamp = account
                    .intent_retired_mono_ns(*intent)
                    .unwrap_or(now_mono_ns);
                to_retire.push((*intent, stamp));
            }
        }
        for (intent, stamp) in to_retire {
            self.retire_intent(intent, stamp);
        }
        self.evict_expired(now_mono_ns);
    }

    pub fn register(
        &mut self,
        scope: &Scope,
        intent: IntentKey,
        client: &str,
        order: [u8; 16],
        command: [u8; 32],
        now_mono_ns: u64,
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
        let win = self.retention_window_ns;
        for loc in &locators {
            if let Some(existing) = self.cache.get_live(loc, now_mono_ns, win)
                && *existing != entry
            {
                return Err(Error::Origin("conflicting cached order origin"));
            }
        }
        if locators
            .iter()
            .all(|loc| self.cache.get_live(loc, now_mono_ns, win) == Some(&entry))
        {
            return Ok(());
        }

        if self.is_at_capacity() {
            self.evict_expired(now_mono_ns);
            if self.is_at_capacity() {
                return Err(Error::Origin("order origin pool capacity exhausted"));
            }
        }

        let hash = digest(&data);
        self.store
            .put_sync(StoreKey::Object(hash), &data)
            .map_err(Error::Storage)?;
        for loc in locators {
            self.store
                .put_sync(StoreKey::Binding(loc), &hash)
                .map_err(Error::Storage)?;
        }
        self.by_intent
            .insert_or_evict(intent, locators, None, now_mono_ns, win)
            .map_err(|_| Error::Origin("order origin pool capacity exhausted"))?;
        for loc in locators {
            self.cache
                .insert_or_evict(loc, entry, None, now_mono_ns, win)
                .map_err(|_| Error::Origin("order origin pool capacity exhausted"))?;
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

    fn reference_vec_locator(scope: &Scope, kind: &[u8], id: &[u8]) -> [u8; 32] {
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
                .register(&scope, intent, "workerA", [1; 16], [7; 32], 1_000)
                .unwrap();
        }
        let other = IntentKey {
            epoch: 5,
            intent_id: 11,
        };
        assert!(
            index
                .register(&scope, other, "workerA", [2; 16], [8; 32], 1_000)
                .is_err()
        );
        assert_eq!(
            index.lookup(&scope, b"client", b"workerA").unwrap().intent,
            intent
        );
        assert!(
            index
                .register(&scope, intent, "workerB", [1; 16], [9; 32], 1_000)
                .is_err()
        );
        assert_eq!(
            index.lookup(&scope, b"order", &[1; 16]).unwrap().digest,
            [7; 32]
        );
        scope.venue = 3;
        assert!(index.lookup(&scope, b"client", b"workerA").is_err());
        index
            .register(&scope, other, "workerA", [2; 16], [8; 32], 1_000)
            .unwrap();
    }

    #[test]
    fn zero_alloc_stack_locator_is_byte_identical_to_reference_encoding() {
        for env in [
            tickwire_venue::Environment::Production,
            tickwire_venue::Environment::Test,
        ] {
            let scope = Scope {
                risk_domain_id: [0x5a; 16],
                venue: 7,
                account: 42,
                environment: env,
            };
            for (kind, id) in [
                (&b"intent"[..], &[1u8, 2, 3, 4, 5, 6, 7, 8][..]),
                (&b"order"[..], &[0xabu8; 16][..]),
                (&b"client"[..], &b"t1selfdescribingclientid000001"[..]),
            ] {
                assert_eq!(
                    locator(&scope, kind, id),
                    reference_vec_locator(&scope, kind, id)
                );
            }
        }
    }

    #[test]
    fn windowed_pool_evicts_expired_retired_origins_past_capacity_without_reallocation() {
        let window_ns = 10_000_000_000u64;
        let mut index =
            Origins::with_order_capacity(tickwire_recovery::MockBlobStore::default(), 4);
        index.set_retention_window_ns(Some(window_ns));
        let slots_ptr = index.cache.slots_addr();
        let buckets_ptr = index.cache.buckets_addr();

        let scope = Scope {
            risk_domain_id: [9; 16],
            venue: 2,
            account: 3,
            environment: tickwire_venue::Environment::Test,
        };

        // 1. Register an active resting order (#1) that never retires.
        let active_intent = IntentKey {
            epoch: 1,
            intent_id: 1,
        };
        index
            .register(&scope, active_intent, "resting-1", [1; 16], [0x11; 32], 100)
            .unwrap();

        // 2. Fill the remaining 3 order slots inside the same window and retire them at t = 200.
        for id in 2..=4u64 {
            let intent = IntentKey {
                epoch: 1,
                intent_id: id,
            };
            let mut oid = [0u8; 16];
            oid[..8].copy_from_slice(&id.to_be_bytes());
            let mut dig = [0u8; 32];
            dig[..8].copy_from_slice(&id.to_le_bytes());
            index
                .register(&scope, intent, &format!("ord-{id}"), oid, dig, 150)
                .unwrap();
            assert!(index.retire_intent(intent, 200));
        }

        // Before `200 + window_ns`, a 5th order inside the same window hits pool capacity!
        assert!(
            index
                .register(
                    &scope,
                    IntentKey {
                        epoch: 1,
                        intent_id: 5
                    },
                    "overflow-in-window",
                    [5; 16],
                    [5; 32],
                    200 + window_ns - 1,
                )
                .is_err()
        );

        // 3. Advance across 24 sequential windows (> 6 * max_orders) without reallocating arena!
        for step in 0..24u64 {
            let id = 10 + step;
            let now_ns = (step + 2) * (window_ns + 1_000);
            let intent = IntentKey {
                epoch: 1,
                intent_id: id,
            };
            let mut oid = [0u8; 16];
            oid[..8].copy_from_slice(&id.to_be_bytes());
            let mut dig = [0u8; 32];
            dig[..8].copy_from_slice(&id.to_le_bytes());
            index
                .register(&scope, intent, &format!("seq-{id}"), oid, dig, now_ns)
                .unwrap();
            assert!(index.retire_intent(intent, now_ns + 10));
        }

        // Arena pointers never reallocated, and active resting order (#1) is still protected!
        assert_eq!(index.cache.slots_addr(), slots_ptr);
        assert_eq!(index.cache.buckets_addr(), buckets_ptr);
        assert_eq!(
            index
                .lookup(&scope, b"client", b"resting-1")
                .unwrap()
                .intent,
            active_intent
        );
        // Conflicting reuse of active resting order's client_id is still rejected even far in the future!
        assert!(
            index
                .register(
                    &scope,
                    IntentKey {
                        epoch: 1,
                        intent_id: 999
                    },
                    "resting-1",
                    [99; 16],
                    [99; 32],
                    100 * window_ns,
                )
                .is_err()
        );
    }
}
