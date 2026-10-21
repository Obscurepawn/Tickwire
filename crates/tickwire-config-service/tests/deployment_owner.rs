#[allow(dead_code)]
mod deployment_support;
use deployment_support::Fixture;
use std::{
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tickwire_config::{Candidate, Format, Limits as ParseLimits};
use tickwire_config_service::{
    Error,
    deployment::Limits,
    deployment_owner::{Assessment, Job, Owner},
    source_bundle::{Bundle, Limits as BundleLimits},
};
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
fn candidate(f: &Fixture) -> Candidate {
    Candidate::parse(
        Format::Json,
        f.candidate.canonical_bytes(),
        ParseLimits::default(),
    )
    .unwrap()
}
fn bundle(f: &Fixture) -> Bundle {
    Bundle::decode(&f.bundle_bytes(), BundleLimits::default()).unwrap()
}
fn fixture() -> Fixture {
    Fixture::new_at(now(), |_| {}, |_, _| {})
}
fn owner(f: &Fixture, max: usize) -> Owner {
    let mut limits = Limits::default();
    limits.accounts.max_age_ms = 60_000;
    limits.credentials.max_age_ms = 60_000;
    limits.resources.max_age_ms = 60_000;
    Owner::new(bundle(f), f.trust(1), limits, max).unwrap()
}
fn finish(o: &mut Owner, job: &mut Job) -> Result<Assessment, Error> {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        match o.poll(job)? {
            Some(result) => return Ok(result),
            None => assert!(Instant::now() < deadline, "bounded computation must finish"),
        }
        thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn actual_background_work_uses_service_time_and_retains_capacity_until_consumed() {
    let f = fixture();
    let mut o = owner(&f, 1);
    let before = now();
    let mut job = o.start(candidate(&f)).unwrap();
    assert_eq!(o.in_flight(), 1);
    assert!(o.start(candidate(&f)).is_err());
    let deadline = Instant::now() + Duration::from_secs(8);
    while !job.computation_finished() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        o.in_flight(),
        1,
        "completed unconsumed result still consumes capacity"
    );
    assert!(o.start(candidate(&f)).is_err());
    let assessment = finish(&mut o, &mut job).unwrap();
    o.revalidate(&assessment).unwrap();
    let checked = assessment.content();
    assert_eq!(checked.candidate_hash(), f.candidate.hash());
    assert!(checked.checked_unix_ms() >= before);
    assert!(checked.checked_unix_ms() <= now());
    assert_eq!(o.in_flight(), 0);
    assert!(o.poll(&mut job).is_err());
    let mut next = o.start(candidate(&f)).unwrap();
    finish(&mut o, &mut next).unwrap();
}
#[test]
fn replaced_context_and_cross_owner_jobs_cannot_return_old_validation() {
    let f = fixture();
    let mut a = owner(&f, 2);
    let mut b = owner(&f, 1);
    let mut job = a.start(candidate(&f)).unwrap();
    assert!(b.poll(&mut job).is_err());
    finish(&mut a, &mut job).unwrap();
    let mut stale = a.start(candidate(&f)).unwrap();
    a.replace(bundle(&f), f.trust(2), Limits::default())
        .unwrap();
    assert!(a.poll(&mut stale).is_err());
    drop(stale);
    let mut current = a.start(candidate(&f)).unwrap();
    let result = finish(&mut a, &mut current).unwrap();
    a.revalidate(&result).unwrap();
    assert!(b.revalidate(&result).is_err());
    assert_eq!(
        result
            .content()
            .accounts()
            .accounts()
            .values()
            .next()
            .unwrap()
            .evidence()
            .trust_hash(),
        f.trust(2).digest()
    );
    a.replace(bundle(&f), f.trust(3), Limits::default())
        .unwrap();
    assert!(a.revalidate(&result).is_err());
}
#[test]
fn failed_replacement_preserves_previous_context_and_dropped_jobs_release_capacity() {
    let f = fixture();
    let mut o = owner(&f, 1);
    let mut invalid = Limits::default();
    invalid.resources.max_age_ms = 0;
    assert!(o.replace(bundle(&f), f.trust(2), invalid).is_err());
    let job = o.start(candidate(&f)).unwrap();
    drop(job);
    let deadline = Instant::now() + Duration::from_secs(8);
    while o.in_flight() != 0 {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    let mut job = o.start(candidate(&f)).unwrap();
    let result = finish(&mut o, &mut job).unwrap();
    assert_eq!(
        result
            .content()
            .reports()
            .reports()
            .values()
            .next()
            .unwrap()
            .trust_hash(),
        f.trust(1).digest()
    );
    for count in [0, 9, usize::MAX] {
        assert!(Owner::new(bundle(&f), f.trust(1), Limits::default(), count).is_err());
    }
}
#[test]
fn stale_signed_observations_fail_with_service_time_without_http_supplied_time() {
    let f = Fixture::new_at(now() - 60_000, |_| {}, |_, _| {});
    let mut o = owner(&f, 1);
    let mut job = o.start(candidate(&f)).unwrap();
    assert!(finish(&mut o, &mut job).is_err());
    assert_eq!(o.in_flight(), 0);
}
