use super::*;
use std::{
    sync::mpsc::{self, Receiver, Sender},
    time::{Duration, Instant},
};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::{
    protocol::{Instrument, wire_bytes},
    transition::Phase,
};
mod fixture;
pub(crate) fn now(domain: ClockDomain) -> MonoTime {
    tickwire_binance_clock::SystemClock::new(domain)
        .unwrap()
        .sample()
        .unwrap()
        .monotonic
}
fn approval(previous: &ConnectConfig, at: u64) -> Approval {
    let mut next_template = previous.clone();
    let mut bank = ResourceHeader::decode(&previous.bank_header).unwrap();
    bank.resource_id = [200; 16];
    bank.predecessor_ring_epoch = bank.ring_epoch;
    bank.ring_epoch += 1;
    bank.feed_generation += 1;
    bank.cutover_seq = 0;
    bank.first_seq = 1;
    bank.created_mono_ns = 0;
    bank.descriptor_hash = [0; 32];
    next_template.bank_header = wire_bytes(&bank).unwrap();
    next_template.instrument_set_version += 1;
    next_template.subscription_generation += 1;
    next_template.cursor_id = [204; 16];
    next_template.request_id = [205; 16];
    next_template.endpoint.control_epoch += 1;
    next_template.endpoint.nonce = [206; 32];
    next_template.instruments.push(Instrument {
        id: (1 << 63) | 1002,
        version: previous.instruments[0].version,
        required_states: 1,
    });
    Approval {
        schema: 1,
        operation_id: [99; 16],
        approval_digest: [98; 32],
        revision: 2,
        previous: previous.clone(),
        next_template,
        deadline_mono_ns: at + 10_000_000_000,
    }
}
pub(crate) enum Command {
    Quiesce,
    Stop,
}
pub(crate) struct Peer {
    pub(crate) command: Sender<Command>,
    pub(crate) commit: Receiver<Commit>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Peer {
    pub(crate) fn start(expand: bool) -> (Self, Approval) {
        let (command, rx) = mpsc::channel();
        let (config_tx, config_rx) = mpsc::sync_channel(1);
        let (commit_tx, commit) = mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || {
            let domain = ClockDomain::new([8; 16], 1).unwrap();
            let mut f = fixture::Fixture::new(now(domain).nanos());
            let admission = f.admission(1);
            tickwire_md::server::register_admission(&f.config.admission_directory, &admission)
                .unwrap();
            let a = approval(&admission.client, f.now);
            config_tx.send(a.clone()).unwrap();
            let mut started = false;
            let mut committed = false;
            let mut retired = None;
            loop {
                match rx.try_recv() {
                    Ok(Command::Stop) | Err(mpsc::TryRecvError::Disconnected) => break,
                    Ok(Command::Quiesce) => {
                        f.server
                            .begin_expansion(
                                0,
                                a.operation_id,
                                std::slice::from_ref(&admission),
                                &mut f.topics[0],
                                now(domain).nanos(),
                            )
                            .unwrap();
                        started = true;
                    }
                    Err(mpsc::TryRecvError::Empty) => {}
                }
                f.server.poll(&mut f.topics, now(domain).nanos()).unwrap();
                if expand
                    && started
                    && !committed
                    && f.server
                        .expansion_quiesced(0, a.operation_id, &mut f.topics[0])
                        .unwrap()
                {
                    retired = Some(f.expand());
                    let mut successor = admission.clone();
                    successor.client = a.next_template.clone();
                    successor.client.bank_header =
                        wire_bytes(f.topics[0].split().0.header()).unwrap();
                    successor.snapshot_id = [207; 16];
                    tickwire_md::server::register_admission(
                        &f.config.admission_directory,
                        &successor,
                    )
                    .unwrap();
                    f.server
                        .resume_expanded_topic(0, a.operation_id, &mut f.topics[0])
                        .unwrap();
                    commit_tx
                        .send(Commit {
                            schema: 1,
                            operation_id: a.operation_id,
                            approval_digest: a.approval_digest,
                            revision: a.revision,
                            config: successor.client,
                        })
                        .unwrap();
                    committed = true;
                }
                std::thread::yield_now();
            }
            drop(retired);
        });
        let a = config_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        (
            Self {
                command,
                commit,
                thread: Some(thread),
            },
            a,
        )
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.command.send(Command::Stop);
        let result = self.thread.take().unwrap().join();
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}
#[derive(Default)]
pub(crate) struct Sink {
    observations: Vec<tickwire_market_source::FeedObservation>,
    invalidated: usize,
}
impl MarketSink for Sink {
    fn observed(&mut self, observation: tickwire_market_source::FeedObservation) -> bool {
        self.observations.push(observation);
        true
    }
    fn invalidated(&mut self, _: tickwire_market_source::Invalidation) -> bool {
        self.invalidated += 1;
        true
    }
}
#[test]
fn real_sources_share_approved_handoff_and_withhold_new_observations_until_release() {
    let (peer, a) = Peer::start(true);
    let domain = a.previous.domain().unwrap();
    let mut sources = Sources::connect(
        std::slice::from_ref(&a.previous),
        domain,
        a.previous.endpoint.local.instance,
        a.previous.endpoint.local.generation,
    )
    .unwrap();
    let control = sources.clone();
    let mut sink = Sink::default();
    sources
        .poll(now(domain), 10_000_000_000, 32, &mut sink)
        .unwrap();
    assert_eq!(sink.observations.len(), 1);
    let mut wrong = a.clone();
    wrong.previous.cursor_id = [1; 16];
    assert!(control.arm(&wrong, now(domain)).is_err());
    assert!(control.receipt(&a).is_none());
    control.arm(&a, now(domain)).unwrap();
    assert_eq!(control.receipt(&a).unwrap().phase, Phase::Armed);
    peer.command.send(Command::Quiesce).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while control.receipt(&a).unwrap().phase != Phase::Quiesced {
        assert!(Instant::now() < deadline);
        sources
            .poll(now(domain), 10_000_000_000, 32, &mut sink)
            .unwrap();
        std::thread::yield_now();
    }
    let c = peer.commit.recv_timeout(Duration::from_secs(3)).unwrap();
    control.commit(&a, &c, now(domain)).unwrap();
    let observations = sink.observations.len();
    while control.receipt(&a).unwrap().phase != Phase::Activated {
        assert!(Instant::now() < deadline);
        sources
            .poll(now(domain), 10_000_000_000, 32, &mut sink)
            .unwrap();
        std::thread::yield_now();
    }
    sources
        .poll(now(domain), 10_000_000_000, 32, &mut sink)
        .unwrap();
    assert_eq!(sink.observations.len(), observations);
    assert!(!sources.is_live(now(domain), 10_000_000_000));
    control.release(&a, &c, now(domain)).unwrap();
    sources
        .poll(now(domain), 10_000_000_000, 32, &mut sink)
        .unwrap();
    assert_eq!(sink.observations.len(), observations + 2);
    assert!(
        sink.observations[observations..]
            .iter()
            .all(|v| v.feed_generation == 2 && v.ring_epoch == 2)
    );
    assert!(sources.is_live(now(domain), 10_000_000_000));
}
#[test]
fn real_sources_reject_unapproved_quiescence_and_never_offer_a_transition_receipt() {
    let (peer, a) = Peer::start(false);
    let domain = a.previous.domain().unwrap();
    let mut sources = Sources::connect(
        std::slice::from_ref(&a.previous),
        domain,
        a.previous.endpoint.local.instance,
        a.previous.endpoint.local.generation,
    )
    .unwrap();
    let control = sources.clone();
    let mut sink = Sink::default();
    sources
        .poll(now(domain), 10_000_000_000, 32, &mut sink)
        .unwrap();
    peer.command.send(Command::Quiesce).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while sink.invalidated == 0 {
        assert!(Instant::now() < deadline);
        sources
            .poll(now(domain), 10_000_000_000, 32, &mut sink)
            .unwrap();
        std::thread::yield_now();
    }
    assert!(!sources.is_live(now(domain), 10_000_000_000));
    assert!(control.receipt(&a).is_none());
    assert!(control.arm(&a, now(domain)).is_err());
}
