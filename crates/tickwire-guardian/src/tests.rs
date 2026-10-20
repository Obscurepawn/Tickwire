use super::*;
use ring::signature::{Ed25519KeyPair, KeyPair};
use std::{cell::RefCell, io, rc::Rc};
use tickwire_fencing::{EmergencyClaims, Validity};
use tickwire_venue::{BoundedText, OrderTarget};

fn config() -> Config {
    Config {
        guardian_id: [7; 16],
        scope: Scope {
            risk_domain: [1; 16],
            venue: 1,
            account: 2,
            product: 1,
            environment: 2,
        },
        old_identity: ExecutionIdentity {
            host_id: [1; 16],
            boot_id: [2; 16],
            exec_instance_id: [3; 16],
            exec_epoch: 4,
            network_identity: [4; 32],
            credential_access_identity: [5; 32],
        },
        audit: tickwire_journal::Config::new([8; 16], [1; 16]),
        writer_epoch: 1,
        allowed_instruments: vec![12],
        max_requests: 8,
        recv_window_micros: 5_000_000,
        delivery_budget_ns: 1_000_000,
        request_timeout_ns: 1_000_000_000,
        max_response_bytes: 65536,
        audit_degraded: true,
    }
}
fn cancel() -> CanonicalCommand {
    let mut c = fixture::command();
    c.body = CommandBody::Cancel(OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(BoundedText::new("test-1").unwrap()),
        venue_order_id: None,
    });
    CanonicalCommand::new(c).unwrap()
}
fn authorization() -> (Verifier, Authorization) {
    let fence = Ed25519KeyPair::from_seed_unchecked(&[1; 32]).unwrap();
    let emergency = Ed25519KeyPair::from_seed_unchecked(&[2; 32]).unwrap();
    let verifier = Verifier::new(
        [3; 16],
        fence.public_key().as_ref().try_into().unwrap(),
        emergency.public_key().as_ref().try_into().unwrap(),
    )
    .unwrap();
    let claims = EmergencyClaims {
        schema: 1,
        scope: config().scope,
        guardian_id: [7; 16],
        controller_id: [3; 16],
        grant_id: [4; 16],
        control_commit: 1,
        precommitted_ns: 1,
        manual_activation_allowed: true,
        instruments: vec![12],
        actions: vec![Action::Cancel],
        command_digests: vec![cancel().digest()],
        max_requests: 1,
        validity: Validity {
            clock_boot_id: [1; 16],
            clock_generation: 1,
            issued_ns: 2,
            expires_ns: 90,
        },
    };
    let payload = serde_json::to_vec(&claims).unwrap();
    let signature = emergency.sign(&payload).as_ref().to_vec();
    (
        verifier,
        Authorization::Emergency(SignedDocument { payload, signature }),
    )
}
#[test]
fn scope_and_action_whitelist_do_not_admit_new_or_cross_account() {
    let mut c = fixture::command();
    assert_eq!(action(&c.body), Err(Error::Denied));
    let replacement = match c.body.clone() {
        CommandBody::New(o) => o,
        _ => unreachable!(),
    };
    let target = match cancel().command().body.clone() {
        CommandBody::Cancel(t) => t,
        _ => unreachable!(),
    };
    c.body = CommandBody::CancelReplace {
        target,
        replacement,
        mode: tickwire_venue::CancelReplaceMode::StopOnFailure,
    };
    assert_eq!(action(&c.body), Err(Error::Denied));
    assert_eq!(config().command(&cancel()).unwrap(), Action::Cancel);
    let mut c = cancel().command().clone();
    c.context.request.account = 3;
    assert_eq!(
        config().command(&CanonicalCommand::new(c).unwrap()),
        Err(Error::Identity)
    );
}
#[test]
fn emergency_permission_has_exact_command_scope_expiry_and_persistent_count() {
    let (verifier, authorization) = authorization();
    let now = |n| MonoTime::new(ClockDomain::new([1; 16], 1).unwrap(), n);
    assert!(permission(&config(), &verifier, &cancel(), &authorization, now(70), 0).is_ok());
    assert_eq!(
        permission(&config(), &verifier, &cancel(), &authorization, now(70), 1),
        Err(Error::Denied)
    );
    assert_eq!(
        permission(&config(), &verifier, &cancel(), &authorization, now(90), 0),
        Err(Error::Denied)
    );
    let mut changed = cancel().command().clone();
    changed.context.request.intent_id += 1;
    assert_eq!(
        permission(
            &config(),
            &verifier,
            &CanonicalCommand::new(changed).unwrap(),
            &authorization,
            now(70),
            0
        ),
        Err(Error::Denied)
    );
}

#[derive(Default)]
struct Memory {
    manifest: Option<Vec<u8>>,
    segments: BTreeMap<u64, Vec<u8>>,
    fail_sync: bool,
}
#[derive(Clone, Default)]
struct Mem(Rc<RefCell<Memory>>);
impl Store for Mem {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.0.borrow().manifest.clone())
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.0.borrow_mut().manifest = Some(b.to_vec());
        Ok(())
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        Ok(self.0.borrow().segments.keys().copied().collect())
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        let b = self
            .0
            .borrow()
            .segments
            .get(&id)
            .cloned()
            .ok_or_else(|| io::Error::other("missing"))?;
        if b.len() as u64 > max {
            return Err(io::Error::other("capacity"));
        }
        Ok(b)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        if self.0.borrow_mut().segments.insert(id, vec![]).is_some() {
            return Err(io::Error::other("exists"));
        }
        Ok(())
    }
    fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> io::Result<()> {
        let mut state = self.0.borrow_mut();
        let bytes = state
            .segments
            .get_mut(&id)
            .ok_or_else(|| io::Error::other("missing"))?;
        if bytes.len() as u64 != offset {
            return Err(io::Error::other("offset"));
        }
        bytes.extend(b);
        Ok(())
    }
    fn sync_segment(&mut self, _: u64) -> io::Result<()> {
        let mut s = self.0.borrow_mut();
        if s.fail_sync {
            s.fail_sync = false;
            Err(io::Error::other("sync"))
        } else {
            Ok(())
        }
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.0
            .borrow_mut()
            .segments
            .get_mut(&id)
            .ok_or_else(|| io::Error::other("missing"))?
            .truncate(len as usize);
        Ok(())
    }
}
#[test]
fn audit_chunks_complete_raw_response_and_rejects_quota_allocation_reuse() {
    let store = Mem::default();
    let mut audit = audit::Audit::create(store.clone(), &config()).unwrap();
    audit.start_quota_run([1; 16], [2; 16], 1).unwrap();
    let raw = vec![255; 50000];
    let outcome = Outcome {
        kind: ResultKind::Unknown,
        status: 0,
        accepted_bytes: 100,
        request_bytes: 100,
        raw_digest: hash(&raw),
        raw_response: raw.clone(),
        received_ns: 80,
        audit_degraded: true,
    };
    audit
        .append(audit::Event::Complete {
            command_digest: [3; 32],
            outcome,
        })
        .unwrap();
    let position = audit.position();
    drop(audit);
    let mut recovered = audit::Audit::recover(store, &config(), None).unwrap();
    assert_eq!(recovered.position(), position);
    assert!(
        matches!(recovered.events().last(),Some(audit::Event::Complete{outcome,..})if outcome.raw_response==raw)
    );
    assert_eq!(
        recovered.start_quota_run([1; 16], [2; 16], 1),
        Err(Error::AlreadyUsed)
    );
}
#[test]
fn failed_audit_sync_poison_prevents_publishing_logical_record() {
    let store = Mem::default();
    let mut audit = audit::Audit::create(store.clone(), &config()).unwrap();
    store.0.borrow_mut().fail_sync = true;
    assert_eq!(
        audit.start_quota_run([1; 16], [2; 16], 1),
        Err(Error::Audit)
    );
    assert_eq!(audit.events().len(), 1);
    assert_eq!(
        audit.start_quota_run([1; 16], [2; 16], 2),
        Err(Error::Audit)
    );
    // Complete physical chunk without its logical commit does not publish a grant.
    let mut reopened = audit::Audit::recover(store, &config(), None).unwrap();
    assert_eq!(reopened.events().len(), 1);
    reopened.start_quota_run([1; 16], [2; 16], 1).unwrap();
}
#[test]
fn local_clock_allows_equal_ticks_but_rejects_domain_or_time_regression() {
    let domain = ClockDomain::new([1; 16], 1).unwrap();
    let instant = Instant::now();
    let now = tickwire_binance_clock::LocalTime {
        instant,
        monotonic: MonoTime::new(domain, 20),
    };
    let mut last = None;
    ordered(&mut last, now).unwrap();
    ordered(&mut last, now).unwrap();
    assert_eq!(
        ordered(
            &mut last,
            tickwire_binance_clock::LocalTime {
                monotonic: MonoTime::new(domain, 19),
                ..now
            }
        ),
        Err(Error::Clock)
    );
    assert_eq!(last.unwrap().monotonic.nanos(), 20);
}
#[test]
fn guardian_connection_retries_share_one_charge_but_new_epochs_pay_again() {
    use tickwire_binance_quota::{Action as A, Policy, Rule};
    use tickwire_quota as q;
    let domain = ClockDomain::new([1; 16], 1).unwrap();
    let now = MonoTime::new(domain, 1);
    let key = q::WindowKey {
        bucket: q::BucketId {
            namespace: [2; 16],
            scope: q::ScopeKind::Account,
            subject: [3; 16],
            pool: 1,
        },
        window: 1,
    };
    let mut allocator = q::Coordinator::open(q::MockQuotaStore::default(), 1, now).unwrap();
    allocator
        .add_window(
            q::WindowConfig {
                key,
                revision: 1,
                starts_ns: 0,
                ends_ns: 1000,
                limit: 20,
                recovery_reserve: 10,
            },
            now,
        )
        .unwrap();
    let mut consumer = allocator
        .register_consumer(
            q::ConsumerId {
                instance: [4; 16],
                generation: 1,
            },
            900,
            now,
        )
        .unwrap();
    let cost = q::Cost {
        window: key,
        units: 1,
    };
    let grant = allocator
        .issue(
            1,
            &consumer,
            q::Class::Recovery,
            &[q::Cost {
                window: key,
                units: 10,
            }],
            800,
            now,
        )
        .unwrap();
    consumer.install(grant, now).unwrap();
    let mut endpoint = Endpoint::new(
        consumer,
        [5; 16],
        domain,
        Policy {
            generation: 1,
            starts_ns: 0,
            ends_ns: 700,
            rules: vec![Rule {
                source: Source::SpotTest,
                action: A::GuardianConnect,
                class: q::Class::Recovery,
                api_weight: 0,
                websocket_messages: 0,
                costs: vec![cost],
            }],
        },
    )
    .unwrap();
    let binding = CommandBinding {
        owner_id: [6; 16],
        risk_domain_id: [1; 16],
        account: 2,
        product: Product::Spot,
        environment: Environment::Test,
        domain,
    };
    assert_eq!(
        endpoint.consume_guardian_connect(&binding, 1, now).unwrap(),
        q::Consumed::New
    );
    assert_eq!(
        endpoint.consume_guardian_connect(&binding, 1, now).unwrap(),
        q::Consumed::Duplicate
    );
    assert_eq!(endpoint.remaining(key, q::Class::Recovery), 9);
    assert_eq!(
        endpoint.consume_guardian_connect(&binding, 2, now).unwrap(),
        q::Consumed::New
    );
    assert_eq!(endpoint.remaining(key, q::Class::Recovery), 8);
}
