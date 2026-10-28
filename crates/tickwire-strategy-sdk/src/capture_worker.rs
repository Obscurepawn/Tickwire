//! Qualified capture process. This owner has no user factory, strategy or intent writer.
use crate::replay::rebuild::capture::{CaptureOnly, store::Attempt};
use crate::{
    Error, Result,
    capture::{Bootstrap, Receipt, SealRequest},
    reports::AccountReport,
};
use std::{
    collections::VecDeque,
    os::fd::AsFd,
    time::{Duration, Instant},
};
use tickwire_clock::MonoTime;
use tickwire_clock_sys::SystemClock;
use tickwire_control::Session;
use tickwire_ipc::{RecordHeader, ReplayControlBody, ResourceHeader, Wire};
use tickwire_market_source::Client;
use tickwire_recovery::{BlobStore, FileBlobReader, FileBlobStore, StoreKey};
use tickwire_release::{
    Object,
    rebuild::{AccountPrefix, Inputs, MarketSeed},
    replay::{Input, StreamKey},
};

fn sample(config: &Bootstrap, clock: &mut SystemClock) -> Result<MonoTime> {
    let now = clock.sample()?.monotonic;
    if now.domain() != config.market.domain()? || now.nanos() >= config.deadline_mono_ns {
        return Err(Error::Deadline);
    }
    Ok(now)
}
fn sessions(client: &mut Client, report: &Session, now: MonoTime) -> Result<()> {
    if now.nanos() >= client.session.deadline() || now.nanos() >= report.deadline() {
        return Err(Error::Deadline);
    }
    match client.connection.try_recv() {
        Ok(None) => Ok(()),
        Ok(Some(packet)) => {
            if !packet.fds().is_empty() {
                return Err(Error::Identity);
            }
            client.session.validate(packet.bytes(), now.nanos())?;
            Err(Error::NotReady)
        }
        Err(tickwire_control_sys::Error::System { source, .. })
            if source.kind() == std::io::ErrorKind::Interrupted =>
        {
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
fn healthy(config: &Bootstrap, client: &Client, now: MonoTime) -> bool {
    config.contract.markets.iter().all(|r| {
        client.replica.view(r.instrument).is_some_and(|v| {
            v.continuous()
                && v.published_mono_ns() <= now.nanos()
                && (if r.category == 3 {
                    v.live()
                        && now
                            .nanos()
                            .checked_sub(v.received_mono_ns())
                            .is_some_and(|age| age <= config.contract.maximum_age_ns)
                } else {
                    v.state_age_ns(r.category, now.nanos())
                        .is_some_and(|age| age <= config.contract.maximum_age_ns)
                })
                && r.known_bid_prices
                    .iter()
                    .all(|p| v.quantity(true, *p).is_some())
                && r.known_ask_prices
                    .iter()
                    .all(|p| v.quantity(false, *p).is_some())
        })
    })
}
fn queue(outbox: &mut VecDeque<Vec<u8>>, bytes: Vec<u8>) -> Result<()> {
    if outbox.len() >= 8 {
        return Err(Error::Bounds);
    }
    outbox.push_back(bytes);
    Ok(())
}
type Pending = (SealRequest, ReplayControlBody);
struct Sealed {
    request: SealRequest,
    body: ReplayControlBody,
    object: Object,
}
fn request_status(
    pending: Option<&Pending>,
    sealed: Option<&Sealed>,
    request: &SealRequest,
    current: u64,
) -> Option<u16> {
    if current > request.account_position {
        return Some(crate::market_transition::INVALID);
    }
    if pending.is_some_and(|(old, _)| old != request)
        || sealed.is_some_and(|old| old.request != *request)
    {
        return Some(crate::market_transition::BUSY);
    }
    None
}
// Called only by the qualified registered entrypoint after its static ID check.
pub(crate) fn run(config: Bootstrap) -> Result<()> {
    let mut clock = SystemClock::new(config.market.domain()?)?;
    config.validate(sample(&config, &mut clock)?)?;
    if crate::replay::live::executable_digest()? != config.identity.binary {
        return Err(Error::Identity);
    }
    sample(&config, &mut clock)?;
    match std::fs::symlink_metadata(&config.output_directory) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(Error::Identity),
    }
    sample(&config, &mut clock)?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(config.market.startup_timeout_ms))
        .ok_or(Error::Bounds)?;
    loop {
        let ready = crate::worker::read_config::<bool>(&config.market_admission_ready, 16);
        sample(&config, &mut clock)?;
        match ready {
            Ok(true) => break,
            Err(Error::Io(e))
                if e.kind() == std::io::ErrorKind::NotFound && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(1))
            }
            _ => return Err(Error::NotReady),
        }
    }
    let mut client = crate::worker::connect_market(&config.market, &mut clock)?;
    sample(&config, &mut clock)?;
    if client.consumer.watermarks().applied != client.replica.sequence() {
        return Err(Error::Identity);
    }
    let (mut connection, mut session) =
        crate::worker::connect(&config.report, &mut clock, deadline, [13; 16])?;
    sample(&config, &mut clock)?;
    let bank = ResourceHeader::decode(&config.report_header)?;
    let subscription = tickwire_shm::SubscriptionKey {
        bank: tickwire_shm::BankKey {
            resource: u128::from_le_bytes(bank.resource_id),
            channel: u128::from_le_bytes(bank.channel_id),
            channel_epoch: bank.channel_epoch,
            ring_epoch: bank.ring_epoch,
            feed_generation: bank.feed_generation,
        },
        instance: u128::from_le_bytes(config.identity.instance),
        generation: config.report_subscription_generation,
        cursor: u128::from_le_bytes(config.report_cursor_id),
    };
    let membership = bank
        .initial_membership_version
        .checked_add(1)
        .ok_or(Error::Bounds)?;
    let expires = session.deadline();
    let packet = connection.recv(deadline)?;
    let at = sample(&config, &mut clock)?;
    let fd = crate::worker::accept_offer(
        &mut session,
        packet,
        &bank,
        [14; 16],
        expires,
        membership,
        false,
        at.nanos(),
    )?;
    let objects = FileBlobReader::open(&config.report_directory, config.report.peer_uid)?;
    sample(&config, &mut clock)?;
    let mut reports = crate::worker::attach_reports(
        fd,
        bank.clone(),
        subscription,
        objects,
        crate::reports::ReportIdentity {
            logical_strategy_id: config.identity.logical_strategy,
            instance_id: config.identity.instance,
            strategy_generation: config.identity.generation,
            risk_domain: config.identity.risk_domain,
        },
    )?;
    let cursor = reports.cursor_fd()?;
    let ch = crate::worker::cursor_header(&bank, subscription)?;
    let offer = crate::worker::resource_offer(&session, &ch, [15; 16], expires, membership, false)?;
    connection.send(&offer, &[cursor.as_fd()], deadline)?;
    let reader = FileBlobReader::open(&config.report_directory, config.report.peer_uid)?;
    sample(&config, &mut clock)?;
    let mut prefix = AccountPrefix {
        baseline: vec![],
        records: vec![],
    };
    let mut account_stream: Option<StreamKey> = None;
    let mut owner: Option<CaptureOnly> = None;
    let mut attempt: Option<Attempt> = None;
    let mut pending: Option<Pending> = None;
    let mut sealed: Option<Sealed> = None;
    let mut outbox: VecDeque<Vec<u8>> = VecDeque::new();
    let stream = crate::replay::application::market_stream(&client.bank)?;
    loop {
        let at = sample(&config, &mut clock)?;
        sessions(&mut client, &session, at)?;
        if pending
            .as_ref()
            .is_some_and(|(_, body)| at.nanos() >= body.valid_until_mono_ns)
        {
            return Err(Error::Deadline);
        }
        if sealed
            .as_ref()
            .is_some_and(|done| at.nanos() >= done.body.valid_until_mono_ns)
        {
            return Err(Error::Deadline);
        }
        if let Some(bytes) = outbox.front()
            && crate::market_transition::try_send(&mut connection, bytes)?
        {
            outbox.pop_front();
        }
        for _ in 0..config.poll_records.min(8) {
            let packet = match connection.try_recv() {
                Ok(Some(p)) => p,
                Ok(None) => break,
                Err(tickwire_control_sys::Error::System { source, .. })
                    if source.kind() == std::io::ErrorKind::Interrupted =>
                {
                    break;
                }
                Err(e) => return Err(e.into()),
            };
            if !packet.fds().is_empty() {
                return Err(Error::Identity);
            }
            let frame = session.validate(packet.bytes(), sample(&config, &mut clock)?.nanos())?;
            let envelope = frame.envelope.clone();
            if envelope.opcode == 20 {
                if sealed.is_none() {
                    return Err(Error::NotReady);
                }
                let mut proof = crate::retirement::check_request(
                    &envelope,
                    frame.body,
                    &bank,
                    subscription,
                    membership,
                )?;
                reports.start_quiesce();
                let released = reports.quiesced()?;
                proof.released_seq = released.watermarks().released;
                proof.applied_seq = released.watermarks().applied;
                proof.evidence_kind = 1;
                let at = sample(&config, &mut clock)?;
                let limit = config
                    .deadline_mono_ns
                    .min(session.deadline())
                    .min(client.session.deadline());
                let remaining = limit
                    .checked_sub(at.nanos())
                    .filter(|n| *n != 0)
                    .ok_or(Error::Deadline)?;
                let until =
                    Instant::now() + Duration::from_nanos(remaining).min(Duration::from_secs(3));
                crate::retirement::depart_market(
                    &mut client.connection,
                    &mut client.session,
                    &client.consumer,
                    &client.join,
                    envelope.request_id,
                    &mut clock,
                    until,
                )?;
                let at = sample(&config, &mut clock)?;
                if at.nanos() >= limit {
                    return Err(Error::Deadline);
                }
                let reply = session.encode(
                    tickwire_ipc::ControlEnvelope {
                        flags: 10,
                        ..envelope
                    },
                    &crate::bootstrap::wire_bytes(&proof)?,
                )?;
                connection.send(&reply, &[], until)?;
                sample(&config, &mut clock)?;
                return Ok(());
            }
            let body = ReplayControlBody::decode(frame.body)?;
            if envelope.opcode != 41 || body.replay_kind != crate::capture::KIND {
                return Err(Error::Identity);
            }
            if body.object_bytes == 0 || body.object_bytes > crate::capture::MAX_BYTES as u64 {
                return Err(Error::Bounds);
            }
            let raw = reader.get_object(body.object_digest, crate::capture::MAX_BYTES)?;
            let now = sample(&config, &mut clock)?;
            if raw.len() as u64 != body.object_bytes {
                return Err(Error::Identity);
            }
            let request = SealRequest::decode(&raw)?;
            crate::capture::verify_request(
                &envelope,
                &body,
                &config,
                &request,
                now,
                session.deadline(),
            )?;
            if let Some(status) = request_status(
                pending.as_ref(),
                sealed.as_ref(),
                &request,
                reports.last_position(),
            ) {
                queue(
                    &mut outbox,
                    crate::capture::encode_rejection(&session, &body, status)?,
                )?;
            } else if let Some(done) = &sealed {
                if done.body != body {
                    return Err(Error::Identity);
                }
                queue(
                    &mut outbox,
                    crate::capture::encode_reply(&session, &body, done.object)?,
                )?;
            } else {
                pending = Some((request, body));
            }
        }
        if sealed.is_some() {
            if config.idle_sleep_us != 0 {
                std::thread::sleep(Duration::from_micros(config.idle_sleep_us));
            }
            continue;
        }
        for _ in 0..config.poll_records {
            let limit = if owner.is_none() {
                config.initial_account_position
            } else {
                pending
                    .as_ref()
                    .map_or(u64::MAX, |(r, _)| r.account_position)
            };
            if !prefix.baseline.is_empty() && reports.last_position() >= limit {
                break;
            }
            let Some(report) = reports.next()? else { break };
            let now = sample(&config, &mut clock)?;
            match report {
                AccountReport::Baseline(b) => {
                    if owner.is_some() || !prefix.baseline.is_empty() {
                        return Err(Error::Identity);
                    }
                    account_stream = Some(crate::replay::account::AccountReplica::stream(&b));
                    prefix.baseline = b.bytes().to_vec();
                }
                AccountReport::Record { position, record } => {
                    if let Some(capture) = &mut owner {
                        capture.push(
                            Input::AccountCallback {
                                stream: account_stream.clone().ok_or(Error::Identity)?,
                                position,
                                canonical: record.bytes().to_vec(),
                            },
                            now,
                        )?;
                    } else {
                        prefix.records.push(record.bytes().to_vec());
                    }
                }
            }
            if prefix
                .baseline
                .len()
                .checked_add(prefix.records.iter().map(Vec::len).sum::<usize>())
                .is_none_or(|n| n > config.contract.maximum_bytes as usize)
            {
                return Err(Error::Bounds);
            }
        }
        if owner.is_none()
            && !prefix.baseline.is_empty()
            && reports.last_position() == config.initial_account_position
        {
            let now = sample(&config, &mut clock)?;
            if client.consumer.watermarks().applied != client.replica.sequence() {
                return Err(Error::Identity);
            }
            let seed = Inputs {
                schema: 1,
                clock: config.identity.clock.clone(),
                start_ns: now.nanos(),
                config: config.config.clone(),
                models: config.models.clone(),
                markets: vec![MarketSeed {
                    resource: crate::bootstrap::wire_bytes(&client.bank)?,
                    checkpoint: client.replica.checkpoint()?,
                    maximum_levels: config.market.maximum_levels,
                }],
                accounts: vec![prefix.clone()],
                warm: vec![],
            };
            attempt = Some(Attempt::begin(
                &config.output_directory,
                &config.identity,
                &config.contract,
                &seed,
            )?);
            sample(&config, &mut clock)?;
            owner = Some(CaptureOnly::new(
                config.identity.clone(),
                config.contract.clone(),
                seed,
                now,
            )?);
        }
        if let Some(capture) = &mut owner {
            for _ in 0..config.poll_records {
                let Some(guard) = client.consumer.borrow()? else {
                    break;
                };
                let header = RecordHeader::decode(&guard.bytes()[..128])?;
                let raw = guard.bytes().to_vec();
                let payload = if header.message_type == 259 {
                    guard.depth_payload()?.to_vec()
                } else {
                    vec![]
                };
                let sequence = guard.sequence();
                drop(guard);
                let now = sample(&config, &mut clock)?;
                capture.push(
                    Input::MarketApplied {
                        stream: stream.clone(),
                        sequence,
                        received_ns: header.ingress_mono_ns,
                        record: raw.clone(),
                        payload: payload.clone(),
                    },
                    now,
                )?;
                let instrument = client.replica.apply(&raw, &payload)?;
                client.consumer.apply(sequence)?;
                let view = client.replica.view(instrument).ok_or(Error::Identity)?;
                let invoked = view.live()
                    && now
                        .nanos()
                        .checked_sub(view.received_mono_ns())
                        .is_some_and(|age| age <= config.contract.maximum_age_ns);
                capture.push(
                    Input::MarketCallback {
                        stream: stream.clone(),
                        instrument,
                        invoked,
                        maximum_age_ns: config.contract.maximum_age_ns,
                    },
                    now,
                )?;
                if pending
                    .as_ref()
                    .is_some_and(|(r, _)| reports.last_position() == r.account_position)
                    && capture.qualified().is_ok()
                {
                    break;
                }
            }
        }
        let now = sample(&config, &mut clock)?;
        if let Some((request, body)) = &pending
            && reports.last_position() == request.account_position
            && owner.as_ref().is_some_and(|c| c.qualified().is_ok())
            && healthy(&config, &client, now)
        {
            let captured = owner.take().ok_or(Error::NotReady)?.finish()?;
            let completion = attempt.take().ok_or(Error::NotReady)?.finish(captured)?;
            sample(&config, &mut clock)?;
            let window = crate::replay::rebuild::capture::store::read_back(
                &config.output_directory,
                &completion,
            )?;
            let now = sample(&config, &mut clock)?;
            sessions(&mut client, &session, now)?;
            if now.nanos() >= body.valid_until_mono_ns || !healthy(&config, &client, now) {
                return Err(Error::Deadline);
            }
            let receipt = Receipt {
                schema: 1,
                bootstrap: request.bootstrap,
                request: Object::of(&request.bytes()?),
                completion,
                pid: std::process::id(),
                report_ring_sequence: reports.last_ring_sequence()?,
                market_applied: client.consumer.watermarks().applied,
                observed_mono_ns: now.nanos(),
            };
            receipt.verify(&config, request, std::process::id(), now, &window)?;
            let bytes = receipt.bytes()?;
            let object = Object::of(&bytes);
            let mut store = FileBlobStore::open(&config.output_directory)?;
            sample(&config, &mut clock)?;
            for dependency in [config.bytes()?, request.bytes()?] {
                store.put_sync(
                    StoreKey::Object(tickwire_recovery::digest(&dependency)),
                    &dependency,
                )?;
                let now = sample(&config, &mut clock)?;
                if now.nanos() >= body.valid_until_mono_ns {
                    return Err(Error::Deadline);
                }
            }
            store.put_sync(StoreKey::Object(object.digest), &bytes)?;
            let now = sample(&config, &mut clock)?;
            sessions(&mut client, &session, now)?;
            if now.nanos() >= body.valid_until_mono_ns || !healthy(&config, &client, now) {
                return Err(Error::Deadline);
            }
            receipt.verify(&config, request, std::process::id(), now, &window)?;
            queue(
                &mut outbox,
                crate::capture::encode_reply(&session, body, object)?,
            )?;
            sealed = Some(Sealed {
                request: request.clone(),
                body: body.clone(),
                object,
            });
            pending = None;
        }
        if config.idle_sleep_us != 0 {
            std::thread::sleep(Duration::from_micros(config.idle_sleep_us));
        }
    }
}
