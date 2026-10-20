use crate::{Error, Result};
use std::collections::BTreeMap;
use tickwire_ipc::*;
use tickwire_snapshot::ValidatedTopic;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeedMark {
    pub topic_id: u64,
    pub feed_generation: u64,
    pub ring_epoch: u64,
    pub sequence: u64,
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub observed_mono_ns: u64,
}
struct Book {
    id: u64,
    version: u64,
    feed: u16,
    received: u64,
    published: u64,
    last_event: u64,
    bids: Vec<DepthLevel>,
    asks: Vec<DepthLevel>,
    coverage: DepthCoverage,
}
pub struct MarketReplica {
    topic: u64,
    generation: u64,
    ring: u64,
    sequence: u64,
    books: BTreeMap<u64, Book>,
    maximum_levels: usize,
    blocked: bool,
}
pub struct MarketView<'a> {
    replica: &'a MarketReplica,
    book: &'a Book,
}
impl MarketView<'_> {
    pub fn instrument(&self) -> u64 {
        self.book.id
    }
    /// Continuity is independent of whether the best price lies inside coverage.
    pub fn continuous(&self) -> bool {
        !self.replica.blocked && self.book.feed == 2
    }
    pub fn live(&self) -> bool {
        !self.replica.blocked
            && self.book.feed == 2
            && self.best_bid().is_some()
            && self.best_ask().is_some()
    }
    pub fn bids(&self) -> &[DepthLevel] {
        &self.book.bids
    }
    pub fn asks(&self) -> &[DepthLevel] {
        &self.book.asks
    }
    pub fn best_bid(&self) -> Option<&DepthLevel> {
        self.book.bids.first().filter(|l| {
            contains(
                self.book.coverage.bid_kind,
                self.book.coverage.bid_boundary_ticks,
                true,
                l.price_ticks,
            )
        })
    }
    pub fn best_ask(&self) -> Option<&DepthLevel> {
        self.book.asks.first().filter(|l| {
            contains(
                self.book.coverage.ask_kind,
                self.book.coverage.ask_boundary_ticks,
                false,
                l.price_ticks,
            )
        })
    }
    /// None is unknown; Some(0) is a proven absent price.
    pub fn quantity(&self, bid: bool, price: i64) -> Option<i64> {
        let (levels, kind, bound) = if bid {
            (
                &self.book.bids,
                self.book.coverage.bid_kind,
                self.book.coverage.bid_boundary_ticks,
            )
        } else {
            (
                &self.book.asks,
                self.book.coverage.ask_kind,
                self.book.coverage.ask_boundary_ticks,
            )
        };
        levels
            .iter()
            .find(|l| l.price_ticks == price)
            .map(|l| l.qty_lots)
            .or_else(|| contains(kind, bound, bid, price).then_some(0))
    }
    pub fn version(&self) -> u64 {
        self.book.version
    }
    pub fn published_mono_ns(&self) -> u64 {
        self.book.published
    }
    pub fn received_mono_ns(&self) -> u64 {
        self.book.received
    }
    pub fn trigger(&self) -> FeedMark {
        self.trigger_at(self.book.published)
    }
    pub fn trigger_at(&self, observed_mono_ns: u64) -> FeedMark {
        FeedMark {
            topic_id: self.replica.topic,
            feed_generation: self.replica.generation,
            ring_epoch: self.replica.ring,
            sequence: self.book.last_event,
            instrument_id: self.book.id,
            instrument_version: self.book.version,
            observed_mono_ns,
        }
    }
}
fn contains(kind: u8, bound: i64, bid: bool, price: i64) -> bool {
    kind == 1 || (kind == 2 && if bid { price >= bound } else { price <= bound })
}
impl MarketReplica {
    pub fn load(
        topic: u64,
        ring: u64,
        snapshot: &ValidatedTopic<'_>,
        maximum_levels: usize,
    ) -> tickwire_ipc::Result<Self> {
        if topic == 0 || ring == 0 || maximum_levels == 0 || maximum_levels > 10000 {
            return Err(tickwire_ipc::Error::Bounds);
        }
        let mut books = BTreeMap::new();
        for index in 0..snapshot.len() {
            let m = snapshot.member(index)?;
            let coverage = m.coverage.ok_or(tickwire_ipc::Error::UnsupportedFeature)?;
            if m.header.bid_count as usize > maximum_levels
                || m.header.ask_count as usize > maximum_levels
            {
                return Err(tickwire_ipc::Error::Bounds);
            }
            let mut received = 0;
            let mut published = 0;
            for bytes in m.state_records.as_chunks::<512>().0 {
                let r = MarketRecord::decode(bytes)?;
                if r.header.message_type == 257 || r.header.message_type == 261 {
                    received = received.max(r.header.ingress_mono_ns);
                    published = published.max(r.header.publish_mono_ns);
                }
            }
            let levels = m
                .depth_levels
                .as_chunks::<16>()
                .0
                .iter()
                .map(|b| DepthLevel::decode(b))
                .collect::<tickwire_ipc::Result<Vec<_>>>()?;
            let split = m.header.bid_count as usize;
            books.insert(
                m.entry.instrument_id,
                Book {
                    id: m.entry.instrument_id,
                    version: m.entry.instrument_version,
                    feed: m.entry.feed_state,
                    received,
                    published,
                    last_event: m.entry.last_instrument_event_seq,
                    bids: levels[..split].to_vec(),
                    asks: levels[split..].to_vec(),
                    coverage,
                },
            );
        }
        Ok(Self {
            topic,
            generation: snapshot.header().feed_generation,
            ring,
            sequence: snapshot.header().sequence,
            books,
            maximum_levels,
            blocked: false,
        })
    }
    pub fn instruments(&self) -> impl Iterator<Item = u64> + '_ {
        self.books.keys().copied()
    }
    pub fn invalidate(&mut self) {
        self.blocked = true;
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn view(&self, instrument: u64) -> Option<MarketView<'_>> {
        self.books.get(&instrument).map(|book| MarketView {
            replica: self,
            book,
        })
    }
    pub fn apply(&mut self, bytes: &[u8], payload: &[u8]) -> Result<u64> {
        let result = self.apply_inner(bytes, payload);
        if result.is_err() {
            self.blocked = true;
        }
        result
    }
    fn apply_inner(&mut self, bytes: &[u8], payload: &[u8]) -> Result<u64> {
        let Record::Market(r) = decode_record(bytes)? else {
            return Err(Error::Identity);
        };
        let h = &r.header;
        if self.blocked
            || h.sequence != self.sequence.checked_add(1).ok_or(Error::Bounds)?
            || h.topic_or_scope_id != self.topic
            || h.feed_generation != self.generation
        {
            return Err(Error::Stale);
        }
        let book = self
            .books
            .get_mut(&h.instrument_id)
            .ok_or(Error::Identity)?;
        if h.instrument_version != book.version {
            return Err(Error::Identity);
        }
        match h.message_type {
            259 => {
                validate_depth_payload(payload, r.body.bid_count, r.body.ask_count)?;
                let levels = payload
                    .as_chunks::<16>()
                    .0
                    .iter()
                    .map(|b| DepthLevel::decode(b))
                    .collect::<tickwire_ipc::Result<Vec<_>>>()?;
                let split = r.body.bid_count as usize;
                let mut bids = book.bids.clone();
                let mut asks = book.asks.clone();
                if r.body.depth_flags & 2 != 0 {
                    return Err(Error::NotReady);
                }
                apply_levels(&mut bids, &levels[..split], true, self.maximum_levels)?;
                apply_levels(&mut asks, &levels[split..], false, self.maximum_levels)?;
                if bids
                    .first()
                    .zip(asks.first())
                    .is_some_and(|(b, a)| b.price_ticks >= a.price_ticks)
                {
                    return Err(Error::Identity);
                }
                book.bids = bids;
                book.asks = asks;
                book.feed = r.body.feed_state;
                book.received = h.ingress_mono_ns;
            }
            257 => {
                // Quote is a derived observation; full depth remains authoritative.
                book.feed = r.body.feed_state;
                book.received = h.ingress_mono_ns;
            }
            261 => {
                book.feed = r.body.feed_state;
                book.received = h.ingress_mono_ns;
            }
            258 | 260 => {}
            262 | 263 => return Err(Error::NotReady), // requires an authenticated fresh JOIN baseline
            _ => return Err(Error::Identity),
        }
        book.published = h.publish_mono_ns;
        book.last_event = h.sequence;
        self.sequence = h.sequence;
        Ok(book.id)
    }
}

fn apply_levels(
    levels: &mut Vec<DepthLevel>,
    updates: &[DepthLevel],
    bid: bool,
    max: usize,
) -> Result<()> {
    for update in updates {
        if update.price_ticks <= 0 || update.qty_lots < 0 {
            return Err(Error::Identity);
        }
        match levels.binary_search_by(|l| {
            if bid {
                update.price_ticks.cmp(&l.price_ticks)
            } else {
                l.price_ticks.cmp(&update.price_ticks)
            }
        }) {
            Ok(i) if update.qty_lots == 0 => {
                levels.remove(i);
            }
            Ok(i) => levels[i] = update.clone(),
            Err(_) if update.qty_lots == 0 => {}
            Err(i) => {
                levels.insert(i, update.clone());
            }
        }
    }
    if levels.len() > max {
        return Err(Error::Bounds);
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn replica() -> MarketReplica {
        let book = Book {
            id: 12,
            version: 13,
            feed: 2,
            received: 49,
            published: 50,
            last_event: 5,
            bids: vec![DepthLevel {
                price_ticks: 100,
                qty_lots: 2,
            }],
            asks: vec![DepthLevel {
                price_ticks: 102,
                qty_lots: 3,
            }],
            coverage: DepthCoverage {
                version: 1,
                bid_kind: 2,
                ask_kind: 2,
                bid_boundary_ticks: 100,
                ask_boundary_ticks: 102,
                source_book_generation: 4,
            },
        };
        MarketReplica {
            topic: 1,
            generation: 2,
            ring: 3,
            sequence: 5,
            books: BTreeMap::from([(12, book)]),
            maximum_levels: 2,
            blocked: false,
        }
    }
    fn depth(seq: u64, levels: &[DepthLevel]) -> (Vec<u8>, Vec<u8>) {
        let r = MarketRecord {
            header: RecordHeader {
                sequence: seq,
                message_type: 259,
                schema_major: 1,
                header_bytes: 128,
                body_bytes: 384,
                flags: 6,
                payload_kind: 1,
                payload_length: (levels.len() * 16) as u32,
                topic_or_scope_id: 1,
                feed_generation: 2,
                producer_generation: 1,
                instrument_id: 12,
                instrument_version: 13,
                ingress_mono_ns: 60,
                publish_mono_ns: 61,
                ..Default::default()
            },
            body: MarketBody {
                bid_count: levels.len() as u32,
                depth_flags: 1,
                level_count: levels.len() as u32,
                feed_state: 2,
                ..Default::default()
            },
        };
        let mut payload = vec![0; levels.len() * 16];
        for (l, b) in levels
            .iter()
            .zip(payload.as_chunks_mut::<16>().0.iter_mut())
        {
            l.encode(b).unwrap();
        }
        let mut bytes = vec![0; 512];
        r.encode(&mut bytes).unwrap();
        bytes[320..320 + payload.len()].copy_from_slice(&payload);
        (bytes, payload)
    }
    #[test]
    fn exact_coverage_and_original_receive_time_survive_outside_updates() {
        let mut r = replica();
        let (b, p) = depth(
            6,
            &[DepthLevel {
                price_ticks: 99,
                qty_lots: 8,
            }],
        );
        r.apply(&b, &p).unwrap();
        let v = r.view(12).unwrap();
        assert_eq!(v.quantity(true, 99), Some(8));
        assert_eq!(v.quantity(true, 98), None);
        assert_eq!(v.quantity(true, 101), Some(0));
        assert_eq!(v.received_mono_ns(), 60);
        assert_eq!(v.trigger_at(63).observed_mono_ns, 63);
    }
    #[test]
    fn failed_batch_keeps_both_sides_and_closes_visibility() {
        let mut r = replica();
        let (b, p) = depth(
            6,
            &[
                DepthLevel {
                    price_ticks: 99,
                    qty_lots: 1,
                },
                DepthLevel {
                    price_ticks: 98,
                    qty_lots: 1,
                },
            ],
        );
        assert!(r.apply(&b, &p).is_err());
        assert_eq!(r.sequence(), 5);
        let v = r.view(12).unwrap();
        assert_eq!(v.bids().len(), 1);
        assert_eq!(v.asks().len(), 1);
        assert!(!v.live());
    }
    #[test]
    fn a_sequence_gap_cannot_revive_the_replica() {
        let mut r = replica();
        let (b, p) = depth(7, &[]);
        assert!(r.apply(&b, &p).is_err());
        let (b, p) = depth(6, &[]);
        assert!(r.apply(&b, &p).is_err());
        assert!(!r.view(12).unwrap().live());
    }
}
