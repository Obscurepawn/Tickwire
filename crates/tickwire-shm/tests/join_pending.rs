#![cfg(target_os = "linux")]
#![allow(unsafe_code)]

use tickwire_ipc::ResourceHeader;
use tickwire_shm::*;

fn header() -> ResourceHeader {
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        header_bytes: 4096,
        resource_kind: 1,
        target_arch: if cfg!(target_arch = "aarch64") { 2 } else { 1 },
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        record_bytes: 512,
        capacity: 64,
        resource_bytes: 8192 + 64 * 512,
        resource_id: 1u128.to_le_bytes(),
        channel_id: 2u128.to_le_bytes(),
        producer_instance: 3u128.to_le_bytes(),
        producer_generation: 1,
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        first_seq: 1,
        initial_membership_version: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        max_consumers: 4,
        schema_major: 1,
        clock_domain: 1,
        required_features: 0x1c,
        ..Default::default()
    }
}
fn key(p: &Publisher<4>, id: u128) -> SubscriptionKey {
    SubscriptionKey {
        bank: p.key(),
        instance: id,
        generation: 1,
        cursor: id + 100,
    }
}
fn record(seq: u64) -> [u8; 512] {
    let mut bytes = [0; 512];
    bytes[..8].copy_from_slice(&seq.to_le_bytes());
    bytes[8..].fill((seq % 251) as u8);
    bytes
}
fn pending(p: &mut Publisher<4>, id: u128) -> (SubscriptionKey, Subscriber<64>, u64) {
    let sub = key(p, id);
    let baseline = p.join(sub).unwrap() - 1;
    // SAFETY: this test owns both SDK endpoints and retains this exact gate.
    let reader = unsafe {
        Subscriber::attach(p.reader_fd().unwrap(), p.header().clone(), sub, baseline).unwrap()
    };
    // SAFETY: reader was created for the exact pending subscription above.
    unsafe {
        p.register_pending_cursor(sub, reader.cursor_fd().unwrap())
            .unwrap();
    }
    (sub, reader, baseline)
}
fn fill(p: &mut Publisher<4>) {
    for seq in 1..=64 {
        p.publish(&[&record(seq)]).unwrap();
    }
}
fn read_all(reader: &Subscriber<64>, apply: bool) {
    while let Some(guard) = reader.borrow().unwrap() {
        assert_eq!(guard.bytes(), record(guard.sequence()));
        if apply {
            reader.apply(guard.sequence()).unwrap();
        }
    }
}
fn full(p: &mut Publisher<4>) {
    assert!(matches!(
        p.publish(&[&record(65)]),
        Err(MappedError::Protocol(Error::Full))
    ));
}

#[test]
fn pending_cursor_release_does_not_advance_gate_before_activation() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let (sub, reader, baseline) = pending(&mut p, 1);
    assert_eq!(p.membership_version(), 2);
    assert_eq!(p.join(sub).unwrap(), 1); // Exact retry adds no membership.
    assert_eq!(p.membership_version(), 2);
    fill(&mut p);
    read_all(&reader, true);
    assert_eq!(
        reader.watermarks(),
        Watermarks {
            read: 64,
            applied: 64,
            released: 64,
        }
    );
    full(&mut p);
    assert_eq!(
        p.activate_join(sub, baseline, 64).unwrap(),
        reader.watermarks()
    );
    assert_eq!(p.membership_version(), 2);
    p.publish(&[&record(65)]).unwrap();
}

#[test]
fn failed_activation_preserves_gate_and_requires_actual_applied_prefix() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let (sub, reader, baseline) = pending(&mut p, 1);
    fill(&mut p);
    read_all(&reader, false);
    assert!(p.activate_join(sub, baseline, 64).is_err());
    full(&mut p);
    for seq in 1..=64 {
        reader.apply(seq).unwrap();
    }
    assert!(p.activate_join(sub, baseline + 1, 64).is_err());
    full(&mut p);
    assert!(p.activate_join(sub, baseline, 65).is_err());
    full(&mut p);
    let mut stale = sub;
    stale.generation += 1;
    assert!(p.activate_join(stale, baseline, 64).is_err());
    full(&mut p);
    p.activate_join(sub, baseline, 64).unwrap();
    p.publish(&[&record(65)]).unwrap();
}

#[test]
fn missing_or_duplicate_cursor_does_not_activate_or_replace_registration() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let sub = key(&p, 1);
    p.join(sub).unwrap();
    assert!(p.activate_join(sub, 0, 0).is_err());
    // SAFETY: exact SDK reader for this still-retained pending membership.
    let reader = unsafe {
        Subscriber::<64>::attach(p.reader_fd().unwrap(), p.header().clone(), sub, 0).unwrap()
    };
    unsafe {
        p.register_pending_cursor(sub, reader.cursor_fd().unwrap())
            .unwrap();
        assert!(
            p.register_pending_cursor(sub, reader.cursor_fd().unwrap())
                .is_err()
        );
    }
    fill(&mut p);
    read_all(&reader, true);
    full(&mut p);
    p.activate_join(sub, 0, 64).unwrap();
    p.publish(&[&record(65)]).unwrap();
}

#[test]
fn another_join_keeps_its_own_pending_baseline_after_first_activation() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let (a, reader_a, s_a) = pending(&mut p, 1);
    let (b, reader_b, s_b) = pending(&mut p, 2);
    assert_eq!(p.membership_version(), 3);
    fill(&mut p);
    read_all(&reader_a, true);
    read_all(&reader_b, true);
    p.activate_join(a, s_a, 64).unwrap();
    full(&mut p); // b still gates at zero even though its actual released is 64.
    p.activate_join(b, s_b, 64).unwrap();
    p.publish(&[&record(65)]).unwrap();
}

#[test]
fn detach_registered_requires_actual_quiescence_and_exact_final_watermarks() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let (sub, reader, _) = pending(&mut p, 1);
    p.publish(&[&record(1)]).unwrap();
    let held = reader.borrow().unwrap().unwrap();
    reader.start_quiesce();
    assert!(reader.quiesced().is_err());
    let claimed = Watermarks {
        read: 1,
        applied: 0,
        released: 1,
    };
    assert!(p.detach_registered(sub, claimed).is_err());
    assert_eq!(held.bytes(), record(1));
    drop(held);
    let actual = reader.quiesced().unwrap().watermarks();
    let wrong = Watermarks {
        applied: 1,
        ..actual
    };
    assert!(p.detach_registered(sub, wrong).is_err());
    let mut stale = sub;
    stale.generation += 1;
    assert!(p.detach_registered(stale, actual).is_err());
    p.detach_registered(sub, actual).unwrap();
    assert_eq!(p.membership_version(), 3);
    assert!(p.activate_join(sub, 0, 0).is_err());
}

#[test]
fn quiescing_cursor_cannot_be_activated_even_at_baseline() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let (sub, reader, baseline) = pending(&mut p, 1);
    reader.start_quiesce();
    assert!(p.activate_join(sub, baseline, baseline).is_err());
    let evidence = reader.quiesced().unwrap();
    assert!(p.activate_join(sub, baseline, baseline).is_err());
    p.detach_registered(sub, evidence.watermarks()).unwrap();
}
