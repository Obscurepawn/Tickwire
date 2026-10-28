use std::{path::PathBuf, sync::mpsc, time::Duration};
use tickwire_binance_public::{DepthEncoding, Notice, SyncOutcome};
use tickwire_book::{BinanceSpot, Book, BookKey, DepthEvent, Level, PriceTicks, QtyLots, Snapshot};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_clock_sys::SystemClock;
#[path = "../../../tickwire-strategy-host/tests/capture_process/market.rs"]
#[allow(dead_code)]
mod shared;

enum Action {
    Publish(i64, mpsc::Sender<u64>),
    Stop,
}
pub struct Market {
    pub admission: tickwire_md::server::Admission,
    pub directory: PathBuf,
    pub instrument: tickwire_venue::InstrumentBinding,
    tx: mpsc::Sender<Action>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Market {
    pub fn start() -> Self {
        let (tx, rx) = mpsc::channel();
        let (info_tx, info_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let mut clock = SystemClock::new(ClockDomain::new([8; 16], 1).unwrap()).unwrap();
            let mut fixture = shared::Fixture::new(clock.sample().unwrap().monotonic.nanos());
            let instrument = fixture.config.feeds[0]
                .instrument
                .build(tickwire_binance_public::Source::SpotProduction)
                .unwrap();
            info_tx
                .send((
                    fixture.admission(1),
                    fixture.config.admission_directory.clone(),
                    instrument,
                ))
                .unwrap();
            let mut sequence = 2;
            let mut previous_bid = 100;
            loop {
                match rx.try_recv() {
                    Ok(Action::Stop) | Err(mpsc::TryRecvError::Disconnected) => break,
                    Ok(Action::Publish(bid, reply)) => {
                        sequence += 1;
                        let now = clock.sample().unwrap().monotonic.nanos();
                        publish(&mut fixture, sequence, previous_bid, bid, now);
                        previous_bid = bid;
                        let published = fixture.topics[0].split().0.published();
                        reply.send(published).unwrap();
                    }
                    Err(mpsc::TryRecvError::Empty) => {}
                }
                fixture
                    .server
                    .poll(
                        &mut fixture.topics,
                        clock.sample().unwrap().monotonic.nanos(),
                    )
                    .unwrap();
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let (admission, directory, instrument) = info_rx.recv().unwrap();
        Self {
            admission,
            directory,
            instrument,
            tx,
            thread: Some(thread),
        }
    }
    pub fn now(&self) -> MonoTime {
        SystemClock::new(self.admission.client.domain().unwrap())
            .unwrap()
            .sample()
            .unwrap()
            .monotonic
    }
    pub fn publish(&self, bid: i64) -> u64 {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Action::Publish(bid, tx)).unwrap();
        rx.recv_timeout(Duration::from_secs(5)).unwrap()
    }
}
impl Drop for Market {
    fn drop(&mut self) {
        let _ = self.tx.send(Action::Stop);
        if let Some(t) = self.thread.take() {
            t.join().unwrap();
        }
    }
}
fn publish(f: &mut shared::Fixture, sequence: u64, previous_bid: i64, bid: i64, now: u64) {
    let key = BookKey {
        venue: 1,
        instrument: 0,
        source: 1,
    };
    let mut book = Book::<BinanceSpot, 8, 8>::new(key, 1);
    book.install_snapshot(Snapshot {
        key,
        generation: 1,
        last_update_id: sequence - 1,
        bids: &[Level {
            price: PriceTicks(bid),
            quantity: QtyLots(sequence as i64),
        }],
        asks: &[Level {
            price: PriceTicks(bid + 1),
            quantity: QtyLots(sequence as i64),
        }],
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    book.apply(DepthEvent {
        key,
        generation: 1,
        first_update_id: sequence,
        last_update_id: sequence,
        previous_update_id: None,
        received_at_ns: now,
        bids: &[],
        asks: &[],
    })
    .unwrap();
    struct Faults;
    impl tickwire_market_publisher::FaultSink for Faults {
        fn invalidate(&mut self, f: tickwire_market_publisher::Fault) {
            panic!("{f:?}")
        }
    }
    let binding = f.config.feeds[0]
        .instrument
        .build(tickwire_binance_public::Source::SpotProduction)
        .unwrap();
    let price = |ticks: i64| {
        tickwire_venue::Decimal::new(
            i128::from(ticks) * binding.price_tick.coefficient(),
            binding.price_tick.exponent(),
        )
        .unwrap()
        .as_plain_string()
    };
    let quantity = tickwire_venue::Decimal::new(
        i128::from(sequence) * binding.quantity_step.coefficient(),
        binding.quantity_step.exponent(),
    )
    .unwrap()
    .as_plain_string();
    let mut bids = vec![];
    let mut asks = vec![];
    if previous_bid != bid {
        bids.push([price(previous_bid), "0".into()]);
        asks.push([price(previous_bid + 1), "0".into()]);
    }
    bids.push([price(bid), quantity.clone()]);
    asks.push([price(bid + 1), quantity]);
    // Publish the actual delta as well as the view: quote-only updates do not
    // change the strategy's owned depth ladder used by Counting::best_bid.
    let raw=serde_json::to_vec(&serde_json::json!({"e":"depthUpdate","E":1,"s":binding.symbol.as_str(),"U":sequence,"u":sequence,"b":bids,"a":asks})).unwrap();
    f.topics[0]
        .on_notice(
            f.config.feeds[0].instrument.id,
            Notice::Sync {
                connection_epoch: 1,
                book_generation: 1,
                received_mono_ns: Some(now),
                raw_delta: Some(&raw),
                encoding: DepthEncoding::Json,
                outcome: SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
                    last_update_id: sequence,
                    became_live: false,
                }),
                view: Some(book.view(now, 1_000_000).unwrap()),
            },
            now,
            &mut Faults,
        )
        .unwrap();
}
