use super::*;
use serde::{Deserialize, Serialize};
mod capture_tests;
mod probe_tests;
struct Fixture {
    op: Operation,
    manifest: ReleaseManifest,
    plans: Plans,
    contract: Contract,
    inputs: Inputs,
}
impl Fixture {
    fn new() -> Self {
        use tickwire_release::{
            Artifact, Binding, Capabilities, Method, OrderKind, PositionMode, Product, Role, Target,
        };
        let (snapshot, catalog, command) = crate::account_fixture::fixture();
        let base = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
        let mut account = tickwire_account::Account::from_snapshot(snapshot, catalog).unwrap();
        let command = CanonicalCommand::new(command).unwrap();
        let admitted = account.admit(&command, 50).unwrap();
        let record = CanonicalRecord::new(
            base.digest(),
            1,
            RecoveryRecord::Admit {
                command: Box::new(command.clone()),
                evaluated_at: 50,
                expected: admitted.image().clone(),
            },
        )
        .unwrap();
        let (resource, replica, bytes) = application::tests::market_fixture();
        let stream = application::market_stream(&resource).unwrap();
        let member = Member {
            risk_domain: [1; 16],
            exec_instance: [2; 16],
            exec_epoch: 4,
            old_instance: [2; 16],
            old_generation: 5,
            new_instance: [6; 16],
            new_generation: 7,
            clock_boot_id: [8; 16],
            clock_generation: 1,
            valid_until_ns: 10_000_000_000,
        };
        let mut op = Operation {
            schema: 3,
            operation_id: [10; 16],
            operation_revision: 1,
            logical_strategy_id: 8,
            members: vec![member.clone()],
            artifacts_digest: [11; 32],
            checkpoint_digest: [12; 32],
            rollback_digest: [13; 32],
            clock_boot_id: [8; 16],
            clock_generation: 1,
            valid_until_ns: 10_000_000_000,
        };
        let binding = Binding {
            operation_id: op.operation_id,
            revision: op.operation_revision,
            logical_strategy: 8,
            members_digest: op.members_digest().unwrap(),
        };
        let contract = Contract {
            schema: 1,
            factory: "rebuild_counter".into(),
            binary: [14; 32],
            state_schema: 15,
            config: Object::of(b"parameters"),
            models: vec![],
            ordering: Ordering::ObservedRebuildWindow,
            markets: vec![MarketRequirement {
                stream: stream.clone(),
                instrument: 1001,
                category: 1,
                minimum_observations: 1,
                known_bid_prices: vec![],
                known_ask_prices: vec![],
            }],
            actions: vec![ActionBinding {
                economic_intent: command.command().context.strategy.economic_intent_id,
                admitted_command: Object::of(command.bytes()),
            }],
            minimum_span_ns: 100_000_000,
            maximum_span_ns: 500_000_000,
            maximum_age_ns: 1_000_000_000,
            maximum_rows: 16,
            maximum_bytes: MAX_BYTES as u64,
            rng: Rng::Disabled,
        };
        let mut raw_resource = vec![0; ResourceHeader::SIZE];
        resource.encode(&mut raw_resource).unwrap();
        let inputs = Inputs {
            schema: 1,
            clock: ClockDomain {
                boot_id: [8; 16],
                generation: 1,
            },
            start_ns: 1_500_000_000,
            config: VersionedBytes {
                revision: 1,
                bytes: b"parameters".to_vec(),
            },
            models: vec![],
            markets: vec![MarketSeed {
                resource: raw_resource,
                checkpoint: replica.checkpoint().unwrap(),
                maximum_levels: 4,
            }],
            accounts: vec![AccountPrefix {
                baseline: base.bytes().to_vec(),
                records: vec![record.bytes().to_vec()],
            }],
            warm: vec![
                TimedInput {
                    now_ns: 1_600_000_000,
                    input: Input::MarketApplied {
                        stream: stream.clone(),
                        sequence: 1001,
                        received_ns: 1_600_000_000,
                        record: bytes.clone(),
                        payload: vec![],
                    },
                },
                TimedInput {
                    now_ns: 1_600_000_000,
                    input: Input::MarketCallback {
                        stream: stream.clone(),
                        instrument: 1001,
                        invoked: true,
                        maximum_age_ns: 1_000_000_000,
                    },
                },
            ],
        };
        let contract_object = Object::of(&contract.bytes().unwrap());
        let manifest = ReleaseManifest {
            schema: 1,
            binding: binding.clone(),
            targets: vec![Target {
                member: member.clone(),
                version: [17; 32],
                files: vec![
                    Artifact {
                        role: Role::Binary,
                        object: Object {
                            digest: contract.binary,
                            bytes: 10,
                        },
                    },
                    Artifact {
                        role: Role::HostConfig,
                        object: Object::of(b"host"),
                    },
                    Artifact {
                        role: Role::Bootstrap,
                        object: Object::of(b"bootstrap"),
                    },
                    Artifact {
                        role: Role::StrategyConfig,
                        object: contract.config,
                    },
                ],
                capabilities: Capabilities {
                    state_schema: 15,
                    input_schema: 2,
                    output_schema: 1,
                    products: vec![Product::Spot],
                    position_modes: vec![PositionMode::Spot],
                    orders: vec![OrderKind::Limit],
                    methods: vec![Method::Rebuild {
                        target_schema: 15,
                        contract: contract_object.digest,
                    }],
                },
            }],
        };
        let plan = Plan {
            schema: 1,
            binding: binding.clone(),
            member,
            target: Source {
                logical_strategy: 8,
                instance: [6; 16],
                generation: 7,
                binary_digest: contract.binary,
                state_schema: 15,
                input_stream: [18; 16],
                clock: inputs.clock.clone(),
            },
            contract: contract_object,
            inputs: Object::of(&inputs.bytes().unwrap()),
            end: Frontier {
                now_ns: 1_600_000_000,
                rows: 2,
                markets: vec![Cursor {
                    stream,
                    sequence: 1001,
                    content_digest: tickwire_recovery::digest(
                        &canonical(&inputs.warm[0].input).unwrap(),
                    ),
                }],
                accounts: vec![Cursor {
                    stream: account::AccountReplica::stream(&base),
                    sequence: 1,
                    content_digest: record.digest(),
                }],
                external: vec![],
            },
        };
        let plans = Plans {
            schema: 1,
            binding,
            members: vec![plan],
        };
        op.artifacts_digest = Object::of(&serde_json::to_vec(&manifest).unwrap()).digest;
        op.checkpoint_digest = Object::of(&plans.bytes().unwrap()).digest;
        Self {
            op,
            manifest,
            plans,
            contract,
            inputs,
        }
    }
    fn input(&self) -> Result<VerifiedInput> {
        VerifiedInput::verify(
            &self.op,
            &self.op.members[0],
            &self.plans,
            &self.contract,
            &self.inputs,
            &self.manifest,
        )
    }
    fn rebind(&mut self) {
        let object = Object::of(&self.contract.bytes().unwrap());
        self.plans.members[0].contract = object;
        self.plans.members[0].inputs = Object::of(&self.inputs.bytes().unwrap());
        self.manifest.targets[0].capabilities.methods = vec![tickwire_release::Method::Rebuild {
            target_schema: self.contract.state_schema,
            contract: object.digest,
        }];
        self.op.artifacts_digest = Object::of(&serde_json::to_vec(&self.manifest).unwrap()).digest;
        self.op.checkpoint_digest = Object::of(&self.plans.bytes().unwrap()).digest;
    }
}
#[derive(Serialize, Deserialize)]
struct State {
    observations: u64,
    consumed: Vec<[u8; 16]>,
    emit: bool,
}
impl crate::Strategy for State {
    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    fn on_market(
        &mut self,
        _v: crate::MarketView<'_>,
        _now: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.observations += 1;
        Ok(self
            .emit
            .then(|| CanonicalCommand::new(crate::test_command::command()).unwrap()))
    }
}
struct TestFactory {
    calls: std::cell::Cell<usize>,
    forget: bool,
    emit: bool,
}
impl TestFactory {
    fn new() -> Self {
        Self {
            calls: std::cell::Cell::new(0),
            forget: false,
            emit: false,
        }
    }
}
impl Factory for TestFactory {
    type Running = State;
    fn binding(&self) -> CompiledFactory {
        CompiledFactory {
            id: "rebuild_counter".into(),
            binary: [14; 32],
            state_schema: 15,
        }
    }
    fn begin(&self, input: &FactoryInput) -> Result<State> {
        self.calls.set(self.calls.get() + 1);
        Ok(State {
            observations: 0,
            consumed: if self.forget {
                vec![]
            } else {
                input.actions().iter().map(|a| a.economic_intent).collect()
            },
            emit: self.emit,
        })
    }
    fn consumed_actions(&self, bytes: &[u8]) -> Result<Vec<[u8; 16]>> {
        Ok(serde_json::from_slice::<State>(bytes)
            .map_err(|_| Error::Identity)?
            .consumed)
    }
    fn validate_checkpoint(&self, bytes: &[u8]) -> Result<()> {
        serde_json::from_slice::<State>(bytes).map_err(|_| Error::Identity)?;
        Ok(())
    }
}
#[test]
fn actual_rebuild_and_warm_callbacks_preserve_the_accepted_economic_action() {
    let fixture = Fixture::new();
    let factory = TestFactory::new();
    let result = execute(&factory, fixture.input().unwrap()).unwrap();
    assert_eq!(factory.calls.get(), 1);
    let state: State = serde_json::from_slice(
        &application::business_bytes(&result.checkpoint().business).unwrap(),
    )
    .unwrap();
    assert_eq!(state.observations, 1);
    assert_eq!(state.consumed, vec![[3; 16]]);
    assert_eq!(result.completion().observations[0].count, 1);
    assert_eq!(result.checkpoint().runtime.effects.count, 0);
    assert_eq!(result.completion().account_position, 1);
}

#[test]
fn declaration_or_unknown_price_cannot_replace_actual_warm_observations() {
    let mut f = Fixture::new();
    f.contract.markets[0].minimum_observations = 2;
    f.inputs.warm.push(f.inputs.warm[1].clone());
    f.plans.members[0].end.rows = 3;
    f.rebind();
    assert!(matches!(
        execute(&TestFactory::new(), f.input().unwrap()),
        Err(Error::StateMismatch)
    ));
    let mut f = Fixture::new();
    f.contract.markets[0].known_bid_prices = vec![1];
    f.rebind();
    assert!(matches!(
        execute(&TestFactory::new(), f.input().unwrap()),
        Err(Error::StateMismatch)
    ));
    let mut f = Fixture::new();
    if let Input::MarketApplied { sequence, .. } = &mut f.inputs.warm[0].input {
        *sequence += 1;
    }
    f.rebind();
    assert!(execute(&TestFactory::new(), f.input().unwrap()).is_err());
}
#[test]
fn accepted_action_cannot_be_forgotten_or_reissued_during_warmup() {
    let f = Fixture::new();
    let mut factory = TestFactory::new();
    factory.forget = true;
    assert!(matches!(
        execute(&factory, f.input().unwrap()),
        Err(Error::StateMismatch)
    ));
    factory.forget = false;
    factory.emit = true;
    assert!(matches!(
        execute(&factory, f.input().unwrap()),
        Err(Error::Unsupported)
    ));
    let mut missing = Fixture::new();
    missing.inputs.accounts[0].records.clear();
    missing.rebind();
    assert!(matches!(missing.input(), Err(Error::Identity)));
}
#[test]
fn rejected_binding_does_not_invoke_the_factory_and_end_cut_is_checked() {
    let f = Fixture::new();
    let factory = TestFactory::new();
    let mut input = f.input().unwrap();
    input.work.contract.binary = [99; 32];
    assert!(matches!(execute(&factory, input), Err(Error::Identity)));
    assert_eq!(factory.calls.get(), 0);
    let mut f = Fixture::new();
    f.plans.members[0].end.markets[0].sequence += 1;
    f.rebind();
    assert!(matches!(
        execute(&factory, f.input().unwrap()),
        Err(Error::StateMismatch)
    ));
}
struct FailingFiles {
    store: tickwire_recovery::FileBlobStore,
    puts: usize,
    fail: usize,
}
impl tickwire_recovery::BlobStore for FailingFiles {
    fn get(
        &self,
        key: tickwire_recovery::StoreKey,
        maximum: usize,
    ) -> tickwire_recovery::Result<Vec<u8>> {
        self.store.get(key, maximum)
    }
    fn put_sync(
        &mut self,
        key: tickwire_recovery::StoreKey,
        bytes: &[u8],
    ) -> tickwire_recovery::Result<()> {
        self.store.put_sync(key, bytes)?;
        self.puts += 1;
        if self.puts == self.fail {
            return Err(
                std::io::Error::other("injected return failure after real object sync").into(),
            );
        }
        Ok(())
    }
}
#[test]
fn each_actual_file_completion_precedes_the_persisted_rebuild_result() {
    use tickwire_recovery::{BlobStore, StoreKey};
    for fail in 1..=10 {
        let path = std::env::temp_dir().join(format!(
            "tickwire-rebuild-{}-{}-{fail}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut files = FailingFiles {
            store: tickwire_recovery::FileBlobStore::open(&path).unwrap(),
            puts: 0,
            fail,
        };
        let f = Fixture::new();
        let candidate = execute(&TestFactory::new(), f.input().unwrap()).unwrap();
        let completion = canonical(candidate.completion()).unwrap();
        let object = Object::of(&completion);
        let result = candidate.persist(&mut files);
        if fail <= 9 {
            assert!(matches!(result, Err(Error::Storage)), "completion {fail}");
        } else {
            let persisted = result.unwrap();
            assert_eq!(files.puts, 9);
            assert_eq!(persisted.object(), object);
            assert_eq!(
                files
                    .get(StoreKey::Object(object.digest), MAX_BYTES)
                    .unwrap(),
                completion
            );
            assert_eq!(persisted.completion().consumed_actions, vec![[3; 16]]);
        }
        drop(files);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn every_accepted_action_of_the_logical_strategy_requires_explicit_contract_coverage() {
    let mut f = Fixture::new();
    f.contract.actions.clear();
    f.rebind();
    assert!(
        matches!(f.input(), Err(Error::Identity)),
        "an unlisted accepted action remains an execution responsibility"
    );
}

#[test]
fn a_new_unlisted_admit_in_the_warm_window_rejects_the_candidate() {
    let mut f = Fixture::new();
    let (snapshot, catalog, mut command) = crate::account_fixture::fixture();
    let mut account = tickwire_account::Account::from_snapshot(snapshot, catalog).unwrap();
    let original = CanonicalCommand::new(command.clone()).unwrap();
    account.admit(&original, 50).unwrap();
    command.context.request.intent_id += 1;
    command.context.strategy.economic_intent_id = [99; 16];
    let tickwire_venue::CommandBody::New(ref mut order) = command.body else {
        unreachable!()
    };
    order.client_id = tickwire_venue::BoundedText::new("second-rebuild-action").unwrap();
    let command = CanonicalCommand::new(command).unwrap();
    let admitted = account.admit(&command, 51).unwrap();
    let previous = CanonicalRecord::decode(&f.inputs.accounts[0].records[0]).unwrap();
    let record = CanonicalRecord::new(
        previous.digest(),
        2,
        RecoveryRecord::Admit {
            command: Box::new(command),
            evaluated_at: 51,
            expected: admitted.image().clone(),
        },
    )
    .unwrap();
    let account_cursor = &mut f.plans.members[0].end.accounts[0];
    f.inputs.warm.push(TimedInput {
        now_ns: 1_600_000_000,
        input: Input::AccountCallback {
            stream: account_cursor.stream.clone(),
            position: 2,
            canonical: record.bytes().to_vec(),
        },
    });
    account_cursor.sequence = 2;
    account_cursor.content_digest =
        tickwire_recovery::digest(&canonical(&f.inputs.warm[2].input).unwrap());
    f.plans.members[0].end.rows = 3;
    f.rebind();
    assert!(
        matches!(
            execute(&TestFactory::new(), f.input().unwrap()),
            Err(Error::Identity)
        ),
        "a warm-up Admit cannot silently create a responsibility outside the fixed contract"
    );
}

#[test]
fn approved_old_rebuild_runs_actual_callbacks_at_the_fixed_cut_without_outputs() {
    use tickwire_release::rebuild::rollback as policy;
    let mut fixture = Fixture::new();
    let mut old = fixture.contract.clone();
    old.config = Object::of(b"fixed old parameters");
    let mut old_inputs = fixture.inputs.clone();
    old_inputs.config.bytes = b"fixed old parameters".to_vec();
    let mut files = fixture.manifest.targets[0].files.clone();
    files[3].object = old.config;
    let mut capabilities = fixture.manifest.targets[0].capabilities.clone();
    capabilities.methods = vec![tickwire_release::Method::Rebuild {
        target_schema: old.state_schema,
        contract: Object::of(&old.bytes().unwrap()).digest,
    }];
    let plan = policy::Plans {
        schema: 1,
        binding: fixture.plans.binding.clone(),
        members: vec![policy::MemberPlan {
            member: fixture.op.members[0].clone(),
            version: [31; 32],
            factory: old.factory.clone(),
            files,
            capabilities,
            probe: policy::Probe {
                instance: [32; 16],
                generation: 8,
                input_stream: [33; 16],
            },
            recovery: policy::Recovery::ApprovedOldRebuild {
                contract: Object::of(&old.bytes().unwrap()),
                inputs: Object::of(&old_inputs.bytes().unwrap()),
                end: fixture.plans.members[0].end.clone(),
            },
        }],
    };
    fixture.op.rollback_digest = Object::of(&plan.bytes().unwrap()).digest;
    let objects = [old.bytes().unwrap(), old_inputs.bytes().unwrap()]
        .into_iter()
        .map(|b| (Object::of(&b).digest, b))
        .collect::<BTreeMap<_, _>>();
    let verified = plan
        .verify(
            &fixture.op,
            &fixture.op.members[0],
            policy::Forward {
                plans: &fixture.plans,
                contract: &fixture.contract,
                inputs: &fixture.inputs,
                manifest: &fixture.manifest,
            },
            |h| {
                objects
                    .get(&h)
                    .cloned()
                    .ok_or(tickwire_release::Error::Missing)
            },
        )
        .unwrap();
    let mut factory = TestFactory::new();
    let candidate = rollback::prepare_declared(&factory, &verified);
    assert!(
        candidate.is_ok(),
        "actual approved old rebuild: {:?}",
        candidate.as_ref().err()
    );
    let candidate = candidate.unwrap();
    assert_eq!(factory.calls.get(), 1);
    assert_eq!(candidate.checkpoint().source.instance, [32; 16]);
    assert_eq!(candidate.checkpoint().source.generation, 8);
    assert_eq!(
        candidate.checkpoint().runtime.config.bytes,
        b"fixed old parameters"
    );
    assert_eq!(candidate.checkpoint().cut.sequence, 2);
    assert_eq!(candidate.checkpoint().runtime.effects.count, 0);
    assert_eq!(candidate.account_position(), 1);
    assert_ne!(candidate.account_seal(), [0; 32]);
    assert_eq!(candidate.observations()[0].count, 1);
    assert_eq!(candidate.consumed_actions(), &[[3; 16]]);
    let state: State = serde_json::from_slice(
        &application::business_bytes(&candidate.checkpoint().business).unwrap(),
    )
    .unwrap();
    assert_eq!(state.observations, 1);
    assert_eq!(state.consumed, vec![[3; 16]]);
    factory.forget = true;
    assert!(matches!(
        rollback::prepare_declared(&factory, &verified),
        Err(Error::StateMismatch)
    ));
    factory.forget = false;
    factory.emit = true;
    assert!(matches!(
        rollback::prepare_declared(&factory, &verified),
        Err(Error::Unsupported)
    ));
}
