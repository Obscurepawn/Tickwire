//! Audited Unix `SOCK_SEQPACKET` SPSC lane factories (`prepare_external_strategy_lanes`) attaching
//! an out-of-process `tickwire-strategy-worker` (`1,024B` `IntentReader` on `"tw-ors-req"` and
//! `512B` `ReportWriter` on `"tw-ors-resp"`) directly inside `OrsProcess` without extra ring
//! copies or `unsafe impl Send`.

use std::os::fd::AsFd;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tickwire_clock::ClockDomain;
use tickwire_clock_sys::SystemClock;
use tickwire_control::{ProcessIdentity, Session};
use tickwire_control_sys::{Connection as ControlConnection, ControlListener};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::protocol::{
    Endpoint, accept_offer, cursor_header, resource_offer, wire_bytes,
};
use tickwire_shm::{BankKey, Publisher, SubscriptionKey};
use tickwire_strategy_sdk::{IntentReader, ReportIdentity, ReportWriter};

/// Static configuration for an external out-of-process strategy worker connecting over Unix
/// `SOCK_SEQPACKET` (`intent.sock` + `report.sock`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalStrategyLaneConfig {
    pub strat_id: u8,
    pub instance_id: [u8; 16],
    pub strategy_generation: u64,
    pub clock_domain: u8,
    pub handshake_timeout_ms: u64,
    pub intent_endpoint: Endpoint,
    pub intent_header_bytes: Vec<u8>,
    pub intent_subscription_generation: u64,
    pub intent_cursor_id: [u8; 16],
    pub report_endpoint: Endpoint,
    pub report_header_bytes: Vec<u8>,
    pub report_subscription_generation: u64,
    pub report_cursor_id: [u8; 16],
}

/// Retains accepted Unix `SOCK_SEQPACKET` control sockets for the lifetime of `OrsProcess` so
/// strategy workers never observe premature control-channel EOF while trading.
#[derive(Clone, Default)]
pub struct ExternalLaneKeepAlive {
    connections: Arc<Mutex<Vec<ControlConnection>>>,
}

impl ExternalLaneKeepAlive {
    pub fn push(&self, conn: ControlConnection) {
        if let Ok(mut guard) = self.connections.lock() {
            guard.push(conn);
        }
    }
}

fn decode_resource_header(bytes: &[u8]) -> Result<ResourceHeader, String> {
    if bytes.len() != ResourceHeader::SIZE {
        return Err(format!(
            "invalid ResourceHeader byte length {} (expected {})",
            bytes.len(),
            ResourceHeader::SIZE
        ));
    }
    let arr: &[u8; ResourceHeader::SIZE] = bytes
        .try_into()
        .map_err(|_| String::from("ResourceHeader slice conversion failed"))?;
    ResourceHeader::decode(arr).map_err(|e| format!("ResourceHeader::decode failed: {e:?}"))
}

fn decode_clock_domain(
    host_boot_id: [u8; 16],
    clock_generation: u64,
) -> Result<ClockDomain, String> {
    ClockDomain::new(host_boot_id, clock_generation)
        .map_err(|e| format!("ClockDomain::new failed: {e:?}"))
}

/// Binds `intent_endpoint.path` and `report_endpoint.path` immediately on the cold startup thread
/// and returns `(IntentLaneFactory, ReportLaneFactory, ExternalLaneKeepAlive)` for
/// `OrderRoutingSystem::attach_strategy_lane_factories`.
pub fn prepare_external_strategy_lanes(
    cfg: ExternalStrategyLaneConfig,
) -> Result<
    (
        tickwire_runtime::IntentLaneFactory,
        tickwire_runtime::ReportLaneFactory,
        ExternalLaneKeepAlive,
    ),
    String,
> {
    let intent_path: PathBuf = cfg.intent_endpoint.path.clone();
    let report_path: PathBuf = cfg.report_endpoint.path.clone();
    let _ = std::fs::remove_file(&intent_path);
    let _ = std::fs::remove_file(&report_path);

    let intent_listener = ControlListener::bind(&intent_path).map_err(|e| {
        format!(
            "failed to bind intent socket {}: {e}",
            intent_path.display()
        )
    })?;
    let report_listener = ControlListener::bind(&report_path).map_err(|e| {
        format!(
            "failed to bind report socket {}: {e}",
            report_path.display()
        )
    })?;

    let intent_header = decode_resource_header(&cfg.intent_header_bytes)?;
    let report_header = decode_resource_header(&cfg.report_header_bytes)?;
    let clock_dom = decode_clock_domain(intent_header.host_boot_id, u64::from(cfg.clock_domain))?;
    let timeout = Duration::from_millis(cfg.handshake_timeout_ms.max(1_000));

    let keep_alive = ExternalLaneKeepAlive::default();
    let keep_intent = keep_alive.clone();
    let keep_report = keep_alive.clone();

    let cfg_intent = cfg.clone();
    let intent_factory: tickwire_runtime::IntentLaneFactory = Box::new(move || {
        let mut clock = SystemClock::new(clock_dom)
            .map_err(|e| format!("SystemClock::new failed on intent lane: {e}"))?;
        let deadline = Instant::now() + timeout;

        let mut conn = intent_listener
            .accept(deadline)
            .map_err(|e| format!("intent_listener.accept failed: {e}"))?;
        let creds = conn.peer_credentials();
        if creds.uid != cfg_intent.intent_endpoint.peer_uid
            || creds.gid != cfg_intent.intent_endpoint.peer_gid
        {
            return Err(String::from("intent peer uid/gid mismatch"));
        }
        let peer_proc = ProcessIdentity {
            pid: creds.pid,
            uid: creds.uid,
            gid: creds.gid,
        };
        let hello = conn
            .recv(deadline)
            .map_err(|e| format!("intent hello recv failed: {e}"))?;
        if !hello.fds().is_empty() {
            return Err(String::from("unexpected fd in intent hello"));
        }

        let mut pol = cfg_intent.intent_endpoint.policy();
        std::mem::swap(&mut pol.local, &mut pol.peer);
        pol.peer_process = peer_proc;

        let now_ns = clock
            .sample()
            .map_err(|e| format!("clock sample failed: {e}"))?
            .monotonic
            .nanos();
        let (mut session, welcome) = Session::accept(&pol, peer_proc, hello.bytes(), now_ns)
            .map_err(|e| format!("intent Session::accept failed: {e:?}"))?;
        conn.send(&welcome, &[], deadline)
            .map_err(|e| format!("intent welcome send failed: {e}"))?;

        let mut expected = intent_header;
        expected.descriptor_hash = [0; 32];
        let raw_hdr =
            wire_bytes(&expected).map_err(|e| format!("wire_bytes(intent) failed: {e:?}"))?;
        expected.descriptor_hash = Sha256::digest(raw_hdr).into();

        let membership = expected
            .initial_membership_version
            .checked_add(1)
            .ok_or_else(|| String::from("intent membership overflow"))?;
        let expires = session.deadline();
        let offer_pkt = conn
            .recv(deadline)
            .map_err(|e| format!("intent offer recv failed: {e}"))?;
        let now_offer = clock
            .sample()
            .map_err(|e| format!("clock sample failed: {e}"))?
            .monotonic
            .nanos();
        let bank_fd = accept_offer(
            &mut session,
            offer_pkt,
            &expected,
            [7; 16],
            expires,
            membership,
            false,
            now_offer,
        )
        .map_err(|e| format!("accept_offer(intent) failed: {e:?}"))?;

        let sub = SubscriptionKey {
            bank: BankKey {
                resource: u128::from_le_bytes(expected.resource_id),
                channel: u128::from_le_bytes(expected.channel_id),
                channel_epoch: expected.channel_epoch,
                ring_epoch: expected.ring_epoch,
                feed_generation: expected.feed_generation,
            },
            instance: u128::from_le_bytes(cfg_intent.intent_endpoint.peer.instance),
            generation: cfg_intent.intent_subscription_generation,
            cursor: u128::from_le_bytes(cfg_intent.intent_cursor_id),
        };

        // SAFETY: `bank_fd` was verified by `Session::accept` + `accept_offer` against `expected`
        // for this single authenticated strategy writer, and `IntentReader` stays on `"tw-ors-req"`.
        #[allow(unsafe_code)]
        let reader = unsafe { IntentReader::attach(bank_fd, expected.clone(), sub, None) }
            .map_err(|e| format!("IntentReader::attach failed: {e}"))?;
        let cursor_fd = reader
            .cursor_fd()
            .map_err(|e| format!("IntentReader::cursor_fd failed: {e}"))?;
        let ch = cursor_header(&expected, sub)
            .map_err(|e| format!("cursor_header(intent) failed: {e:?}"))?;
        let reply = resource_offer(&session, &ch, [8; 16], expires, membership, false)
            .map_err(|e| format!("resource_offer(intent cursor) failed: {e:?}"))?;
        conn.send(&reply, &[cursor_fd.as_fd()], deadline)
            .map_err(|e| format!("intent cursor send failed: {e}"))?;

        keep_intent.push(conn);
        Ok(reader)
    });

    let report_factory: tickwire_runtime::ReportLaneFactory = Box::new(move || {
        let mut clock = SystemClock::new(clock_dom)
            .map_err(|e| format!("SystemClock::new failed on report lane: {e}"))?;
        let deadline = Instant::now() + timeout;

        let mut publisher = Publisher::<1>::create(report_header)
            .map_err(|e| format!("Report Publisher::create failed: {e}"))?;
        let report_sub = SubscriptionKey {
            bank: publisher.key(),
            instance: u128::from_le_bytes(cfg.instance_id),
            generation: cfg.report_subscription_generation,
            cursor: u128::from_le_bytes(cfg.report_cursor_id),
        };
        publisher
            .join(report_sub)
            .map_err(|e| format!("Report Publisher::join failed: {e}"))?;

        let mut conn = report_listener
            .accept(deadline)
            .map_err(|e| format!("report_listener.accept failed: {e}"))?;
        let creds = conn.peer_credentials();
        if creds.uid != cfg.report_endpoint.peer_uid || creds.gid != cfg.report_endpoint.peer_gid {
            return Err(String::from("report peer uid/gid mismatch"));
        }
        let peer_proc = ProcessIdentity {
            pid: creds.pid,
            uid: creds.uid,
            gid: creds.gid,
        };
        let hello = conn
            .recv(deadline)
            .map_err(|e| format!("report hello recv failed: {e}"))?;
        if !hello.fds().is_empty() {
            return Err(String::from("unexpected fd in report hello"));
        }

        let mut pol = cfg.report_endpoint.policy();
        std::mem::swap(&mut pol.local, &mut pol.peer);
        pol.peer_process = peer_proc;

        let now_ns = clock
            .sample()
            .map_err(|e| format!("clock sample failed: {e}"))?
            .monotonic
            .nanos();
        let (mut session, welcome) = Session::accept(&pol, peer_proc, hello.bytes(), now_ns)
            .map_err(|e| format!("report Session::accept failed: {e:?}"))?;
        conn.send(&welcome, &[], deadline)
            .map_err(|e| format!("report welcome send failed: {e}"))?;

        let membership = publisher.membership_version();
        let expires = session.deadline();
        let offered = resource_offer(
            &session,
            publisher.header(),
            [14; 16],
            expires,
            membership,
            false,
        )
        .map_err(|e| format!("resource_offer(report bank) failed: {e:?}"))?;
        let bank_fd = publisher
            .reader_fd()
            .map_err(|e| format!("publisher.reader_fd failed: {e}"))?;
        conn.send(&offered, &[bank_fd.as_fd()], deadline)
            .map_err(|e| format!("report bank offer send failed: {e}"))?;

        let ch = cursor_header(publisher.header(), report_sub)
            .map_err(|e| format!("cursor_header(report) failed: {e:?}"))?;
        let cursor_pkt = conn
            .recv(deadline)
            .map_err(|e| format!("report cursor recv failed: {e}"))?;
        let now_cur = clock
            .sample()
            .map_err(|e| format!("clock sample failed: {e}"))?
            .monotonic
            .nanos();
        let cursor_fd = accept_offer(
            &mut session,
            cursor_pkt,
            &ch,
            [15; 16],
            expires,
            membership,
            false,
            now_cur,
        )
        .map_err(|e| format!("accept_offer(report cursor) failed: {e:?}"))?;

        // SAFETY: `cursor_fd` was verified via `accept_offer` for `report_sub` on the fresh
        // single-consumer `Publisher<1>` owned exclusively by `"tw-ors-resp"`.
        #[allow(unsafe_code)]
        unsafe {
            publisher
                .register_cursor(report_sub, cursor_fd)
                .map_err(|e| format!("publisher.register_cursor failed: {e}"))?;
        }
        let writer = ReportWriter::with_arena(
            publisher,
            Some(report_sub),
            None,
            ReportIdentity {
                strat_id: cfg.strat_id,
                instance_id: cfg.instance_id,
                strategy_generation: cfg.strategy_generation,
            },
        )
        .map_err(|e| format!("ReportWriter::with_arena failed: {e}"))?;

        keep_report.push(conn);
        Ok(writer)
    });

    Ok((intent_factory, report_factory, keep_alive))
}
