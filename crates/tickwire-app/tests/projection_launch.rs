//! Production App entrypoint coverage. A lazy local owner is not a successful
//! resource launch; the real delegated success path requires cgroup privileges.
#![cfg(target_os = "linux")]
#![recursion_limit = "256"]
use serde_json::json;
use tickwire_app::observation::Target;

#[path = "../../tickwire-account/tests/common/mod.rs"]
mod account_fixture;
#[path = "projection_launch/channels.rs"]
mod channels;
#[path = "projection_launch/fixture.rs"]
mod fixture;
#[path = "projection_launch/runtime.rs"]
mod runtime;
use fixture::Fixture;

fn rejected(f: &Fixture) -> String {
    let error = match f.build() {
        Ok(_) => panic!("invalid projection reached hosted owner construction"),
        Err(e) => e.to_string(),
    };
    f.assert_no_launch_files();
    error
}

#[test]
fn local_entrypoint_preserves_legacy_and_does_not_claim_unstarted_resources() {
    let mut f = Fixture::new();
    f.change_host(|h| {
        h.as_object_mut().unwrap().remove("projection_launch");
    });
    let owner = f
        .build()
        .expect("the unrelated runtime/market fixture must be valid");
    let target = Target {
        logical_strategy_id: 1,
        instance: [2; 16],
        generation: 1,
    };
    assert!(owner.projection_launch(target).is_none());
    drop(owner);
    f.assert_no_launch_files();
    for shadow in [false, true] {
        let f = Fixture::with_shadow(shadow);
        let owner = f
            .build()
            .expect("a fully bound profile can construct a lazy local owner");
        assert!(
            owner.projection_launch(target).is_none(),
            "no Child exists before actual live market input"
        );
        f.assert_no_launch_files();
    }
}

#[test]
fn hosted_build_rejects_unknown_os_mapping_before_launch_side_effects() {
    let mut f = Fixture::new();
    f.change_host(|h| h["projection_launch"]["os_identities"][0]["name"] = "different-user".into());
    let error = rejected(&f);
    assert!(
        error.contains("projection"),
        "unexpected rejection: {error}"
    );
}

#[test]
fn hosted_build_rejects_restricted_modes_before_reading_their_inputs() {
    for mode in ["capture", "rotation", "preflight"] {
        let mut f = Fixture::new();
        let unavailable = f.root.join("never-read-this-input");
        let publication = f.root.join("publication");
        match mode {
            "capture" => f.change_host(|h| {
                h["capture"] = json!({"bootstrap":unavailable,"publication_directory":publication})
            }),
            "rotation" => f.change_host(|h| h["rotation_directory"] = json!(unavailable)),
            _ => {
                f.bootstrap.preflight_job = Some(unavailable);
                f.save_bootstrap();
            }
        }
        let error = rejected(&f);
        assert!(
            error.contains("projection"),
            "{mode} failed outside the profile gate: {error}"
        );
    }
}

#[test]
fn hosted_build_rejects_identity_resources_and_object_conflicts_before_launch() {
    for case in [
        "uid",
        "duplicate-os",
        "scope",
        "no-resources",
        "cpu",
        "memory",
        "object",
        "channel",
        "unregistered",
    ] {
        let mut f = Fixture::new();
        match case {
            "uid" => f.change_host(|h| {
                let uid = h["uid"].as_u64().unwrap();
                h["projection_launch"]["os_identities"][0]["uid"] = (uid + 1).into();
            }),
            "duplicate-os" => f.change_host(|h| {
                let duplicate = h["projection_launch"]["os_identities"][0].clone();
                h["projection_launch"]["os_identities"]
                    .as_array_mut()
                    .unwrap()
                    .push(duplicate);
            }),
            "scope" => {
                f.change_host(|h| h["projection_launch"]["scope"]["host_id"] = "other-host".into())
            }
            "no-resources" => f.change_host(|h| {
                h.as_object_mut().unwrap().remove("resources");
            }),
            "cpu" => f.change_host(|h| h["resources"]["worker"]["cpus"] = json!([4095])),
            "memory" => f.change_host(|h| {
                let n = h["resources"]["worker"]["memory_max"].as_u64().unwrap();
                h["resources"]["worker"]["memory_max"] = (n + 1).into();
            }),
            "object" => std::fs::write(f.root.join("projection.json"), b"{}").unwrap(),
            "channel" => {
                f.bootstrap.report.local.instance = [99; 16];
                f.save_bootstrap();
            }
            _ => {
                f.bootstrap.factory = None;
                f.save_bootstrap();
            }
        }
        let error = rejected(&f);
        assert!(
            error.contains("projection"),
            "{case} failed outside profile binding: {error}"
        );
    }
}

#[test]
fn remote_entrypoint_reaches_real_resource_rejection_without_legacy_fallback() {
    let mut f = Fixture::new();
    f.remote();
    f.create_launch_parents();
    let error = match f.build() {
        Ok(_) => panic!("ordinary filesystem cgroup cannot yield a hosted strategy"),
        Err(e) => e,
    };
    let host = error
        .downcast_ref::<tickwire_strategy_host::Error>()
        .unwrap_or_else(|| panic!("must reach the actual Host resource boundary: {error}"));
    assert!(
        matches!(
            host,
            tickwire_strategy_host::Error::Supervisor(tickwire_supervisor::Error::Transport(
                tickwire_control_sys::Error::InvalidResources
            ))
        ),
        "{host:?}"
    );
    assert!(f.root.join("commands").is_dir());
    assert!(f.root.join("reports").is_dir());
    f.assert_failed_launch_artifacts();
}

#[test]
fn remote_entrypoint_does_not_substitute_a_different_actual_elf() {
    let mut f = Fixture::new();
    f.remote();
    f.create_launch_parents();
    let other = std::fs::canonicalize("/bin/false").unwrap();
    assert_ne!(
        tickwire_recovery::digest(&std::fs::read(&other).unwrap()),
        f.bootstrap.market.local.build_hash,
        "the negative must use distinct actual executable bytes"
    );
    f.host.executable = other;
    let error = match f.build() {
        Ok(_) => panic!("a different actual ELF cannot satisfy the approved artifact"),
        Err(e) => e,
    };
    assert!(
        matches!(
            error.downcast_ref::<tickwire_strategy_host::Error>(),
            Some(tickwire_strategy_host::Error::Supervisor(
                tickwire_supervisor::Error::Identity
            ))
        ),
        "must reject the sealed image at Registry, got {error}"
    );
    f.assert_failed_launch_artifacts();
}

#[test]
fn local_live_market_cannot_issue_an_intent_or_proof_after_resource_rejection() {
    use tickwire_book::{BinanceSpot, Book, DepthEvent, Level, PriceTicks, QtyLots, Snapshot};
    let f = Fixture::new();
    f.create_launch_parents();
    let mut owner = f.build().unwrap();
    let mut clock = tickwire_binance_clock::SystemClock::new(f.runtime.time.domain).unwrap();
    let sampled = clock.sample().unwrap();
    let now = sampled.monotonic.nanos();
    let key = f.runtime.market.depth.key;
    let mut book = Book::<BinanceSpot, 8, 8>::new(key, 1);
    book.install_snapshot(Snapshot {
        key,
        generation: 1,
        last_update_id: 1,
        bids: &[Level {
            price: PriceTicks(100),
            quantity: QtyLots(1),
        }],
        asks: &[Level {
            price: PriceTicks(101),
            quantity: QtyLots(1),
        }],
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    book.apply(DepthEvent {
        key,
        generation: 1,
        first_update_id: 2,
        last_update_id: 2,
        previous_update_id: None,
        received_at_ns: now,
        bids: &[],
        asks: &[],
    })
    .unwrap();
    assert!(owner.on_market(&tickwire_binance_public::Notice::Sync {
        connection_epoch: 1,
        book_generation: 1,
        received_mono_ns: Some(now),
        raw_delta: None,
        encoding: tickwire_binance_public::DepthEncoding::Json,
        outcome: tickwire_binance_public::SyncOutcome::Applied(
            tickwire_book::ApplyOutcome::Applied {
                last_update_id: 2,
                became_live: true,
            }
        ),
        view: Some(book.view(now, 10_000_000_000).unwrap()),
    }));
    assert!(
        !owner.failed(),
        "actual market publication must succeed before startup"
    );
    let baseline = &f.runtime.input.baseline;
    let account = tickwire_account::Account::from_snapshot(
        baseline.snapshot().clone(),
        baseline.catalog().clone(),
    )
    .unwrap();
    let health = tickwire_runtime::node::Health::default();
    let local_time = tickwire_binance_clock::LocalTime {
        instant: sampled.instant,
        monotonic: sampled.monotonic,
    };
    assert!(owner.next(&account, health, local_time).is_none());
    assert!(
        owner.failed(),
        "failure must be latched by actual lazy launch"
    );
    assert!(
        owner
            .projection_launch(Target {
                logical_strategy_id: 1,
                instance: [2; 16],
                generation: 1
            })
            .is_none()
    );
    assert!(
        owner.next(&account, health, local_time).is_none(),
        "a failed launch must not be retried or release intent"
    );
    f.assert_failed_launch_artifacts();
}

#[test]
fn inspect_validates_all_strategy_slots_before_worker_startup() {
    let mut fixture = Fixture::new();
    let (snapshot, catalog, _) = account_fixture::fixture();
    let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let mut deployment =
        runtime::deployment_json(baseline.catalog().entries[0].instrument.instrument_id);
    let baseline_path = fixture.root.join("baseline.cbor");
    std::fs::write(&baseline_path, baseline.bytes()).unwrap();
    deployment["baseline"] = json!(baseline_path);
    deployment["strategy"] = json!({"mode":"multi","members":[
        {"logical_strategy_id":1,"strategy":{"mode":"hosted","config":fixture.host}},
        {"logical_strategy_id":1,"strategy":{"mode":"observe"}}
    ]});
    let path = fixture.root.join("deployment.json");
    let inspect = |d: &serde_json::Value| {
        std::fs::write(&path, serde_json::to_vec(d).unwrap()).unwrap();
        std::process::Command::new(env!("CARGO_BIN_EXE_tickwire-app"))
            .arg("inspect")
            .arg("--config")
            .arg(&path)
            .output()
            .unwrap()
    };
    let rejected = inspect(&deployment);
    assert!(
        !rejected.status.success(),
        "inspect accepted two active owners for one strategy"
    );
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("slot"));
    deployment["strategy"]["members"][1]["logical_strategy_id"] = 2.into();
    let accepted = inspect(&deployment);
    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&accepted.stdout).unwrap();
    assert_eq!(result["strategy_members"][0]["generation"], 1);
    assert_eq!(result["strategy_members"][1]["logical_strategy_id"], 2);
    assert_eq!(
        result["risk_domain_id"],
        json!(fixture.bootstrap.risk_domain)
    );
    fixture.bootstrap.risk_domain = [99; 16];
    fixture.save_bootstrap();
    let wrong_scope = inspect(&deployment);
    assert!(!wrong_scope.status.success());
    assert!(String::from_utf8_lossy(&wrong_scope.stderr).contains("risk domain"));
    fixture.assert_no_launch_files();
}

#[test]
fn shared_market_preflight_rejects_cursor_request_and_storage_aliases_before_launch() {
    use tickwire_app::strategy::Config;

    let mut first = Fixture::new();
    let mut second = Fixture::new();
    first.remote();
    second.bootstrap.market.local.instance = [22; 16];
    second.bootstrap.market.local.logical_strategy = 2;
    second.remote();
    let first_external = first.bootstrap.external_market.as_ref().unwrap();
    let second_external = second.bootstrap.external_market.as_mut().unwrap();
    second_external.endpoint.path = first_external.endpoint.path.clone();
    second_external.cursor_id = [42; 16];
    second_external.request_id = [43; 16];
    second.save_bootstrap();
    let members: Config = serde_json::from_value(json!({"mode":"multi","members":[
        {"logical_strategy_id":1,"strategy":{"mode":"hosted","config":first.host}},
        {"logical_strategy_id":2,"strategy":{"mode":"hosted","config":second.host}}
    ]}))
    .unwrap();
    let Config::Multi { members } = members else {
        unreachable!()
    };
    let scope = first.bootstrap.risk_domain;
    let inspect = || tickwire_app::launch::group::inspect(&members, scope);
    let original = second.bootstrap.clone();
    assert_eq!(inspect().unwrap().len(), 2);

    let alias = second.root.join("alias");
    std::os::unix::fs::symlink(&first.root, &alias).unwrap();
    for expected in ["cursor", "request", "directory", "checkpoint"] {
        second.bootstrap = original.clone();
        match expected {
            "cursor" => {
                second.bootstrap.external_market.as_mut().unwrap().cursor_id =
                    first_external.cursor_id
            }
            "request" => {
                second
                    .bootstrap
                    .external_market
                    .as_mut()
                    .unwrap()
                    .request_id = first_external.request_id
            }
            "directory" => {
                second.bootstrap.report_directory = first.bootstrap.command_directory.clone()
            }
            "checkpoint" => {
                second.bootstrap.command_directory = alias.join("commands/merged-inputs")
            }
            _ => unreachable!(),
        }
        second.save_bootstrap();
        let error = inspect()
            .err()
            .unwrap_or_else(|| panic!("group accepted shared {expected}"));
        let reason = if expected == "checkpoint" {
            "directory"
        } else {
            expected
        };
        assert!(error.to_string().contains(reason), "{expected}: {error}");
    }
    // The publisher owns one cursor namespace per bank and one request ledger across its topics.
    use tickwire_ipc::{ResourceHeader, Wire};
    second.bootstrap = original;
    let client = second.bootstrap.external_market.as_mut().unwrap();
    let mut other_bank = ResourceHeader::decode(&client.bank_header).unwrap();
    other_bank.resource_id = [84; 16];
    other_bank.channel_id = [85; 16];
    other_bank.topic_id += 1;
    client.bank_header = tickwire_market_source::protocol::wire_bytes(&other_bank).unwrap();
    client.cursor_id = first_external.cursor_id;
    second.save_bootstrap();
    assert_eq!(inspect().unwrap().len(), 2);
    second
        .bootstrap
        .external_market
        .as_mut()
        .unwrap()
        .request_id = first_external.request_id;
    second.save_bootstrap();
    assert!(inspect().err().unwrap().to_string().contains("request"));
    first.assert_no_launch_files();
    second.assert_no_launch_files();
}
