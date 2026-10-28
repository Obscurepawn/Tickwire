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
