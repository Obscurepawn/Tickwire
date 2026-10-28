use super::*;
use sha2::{Digest, Sha256};
const MAX_BYTES: usize = 64 * 1024 * 1024;
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let b = self.0.get(..n).ok_or(Error::Bounds)?;
        self.0 = &self.0[n..];
        Ok(b)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<usize> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize)
    }
    fn zero(&mut self, n: usize) -> Result<()> {
        if self.take(n)?.iter().any(|v| *v != 0) {
            Err(Error::Identity)
        } else {
            Ok(())
        }
    }
}
impl MarketReplica {
    pub(super) fn encode_checkpoint(&self) -> Result<Vec<u8>> {
        if self.books.len() > 4096 {
            return Err(Error::Bounds);
        }
        let size = self
            .books
            .iter()
            .try_fold(88usize, |n, b| {
                n.checked_add(96)?
                    .checked_add(b.states.len().checked_mul(512)?)?
                    .checked_add((b.bids.len() + b.asks.len()).checked_mul(16)?)
            })
            .ok_or(Error::Bounds)?;
        if size > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let mut out = Vec::with_capacity(size);
        out.extend_from_slice(b"TWMR0001");
        for n in [
            self.topic,
            self.generation,
            self.ring,
            self.sequence,
            self.maximum_levels as u64,
        ] {
            out.extend_from_slice(&n.to_le_bytes());
        }
        out.extend_from_slice(&(self.books.len() as u32).to_le_bytes());
        out.push(u8::from(self.blocked));
        out.extend_from_slice(&[0; 3]);
        for b in &self.books {
            for n in [b.id, b.version] {
                out.extend_from_slice(&n.to_le_bytes());
            }
            out.extend_from_slice(&b.feed.to_le_bytes());
            out.extend_from_slice(&[0; 6]);
            for n in [b.received, b.published, b.last_event] {
                out.extend_from_slice(&n.to_le_bytes());
            }
            let mut coverage = [0; 32];
            b.coverage.encode(&mut coverage)?;
            out.extend_from_slice(&coverage);
            for n in [b.bids.len(), b.asks.len(), b.states.len()] {
                out.extend_from_slice(&(n as u32).to_le_bytes());
            }
            out.extend_from_slice(&[0; 4]);
            for s in b.states.values() {
                let mut bytes = [0; 512];
                s.encode(&mut bytes)?;
                out.extend_from_slice(&bytes);
            }
            for l in b.bids.iter().chain(&b.asks) {
                let mut bytes = [0; 16];
                l.encode(&mut bytes)?;
                out.extend_from_slice(&bytes);
            }
        }
        let hash = Sha256::digest(&out);
        out.extend_from_slice(&hash);
        debug_assert_eq!(out.len(), size);
        Ok(out)
    }
    pub(super) fn decode_checkpoint(bytes: &[u8], maximum_levels: usize) -> Result<Self> {
        if !(88..=MAX_BYTES).contains(&bytes.len()) || maximum_levels == 0 || maximum_levels > 10000
        {
            return Err(Error::Bounds);
        }
        let (data, hash) = bytes.split_at(bytes.len() - 32);
        if Sha256::digest(data).as_slice() != hash {
            return Err(Error::Identity);
        }
        let mut r = Reader(data);
        if r.take(8)? != b"TWMR0001" {
            return Err(Error::Identity);
        }
        let topic = r.u64()?;
        let generation = r.u64()?;
        let ring = r.u64()?;
        let sequence = r.u64()?;
        let stored_max = r.u64()?;
        // Capacity is part of the materialized state: silently reducing it would change later admission.
        if topic == 0
            || generation == 0
            || ring == 0
            || sequence >= 1 << 63
            || stored_max != maximum_levels as u64
        {
            return Err(Error::Identity);
        }
        let count = r.u32()?;
        if count == 0 || count > 4096 {
            return Err(Error::Bounds);
        }
        let blocked = match r.take(1)?[0] {
            0 => false,
            1 => true,
            _ => return Err(Error::Identity),
        };
        r.zero(3)?;
        let mut books = Vec::with_capacity(count);
        let mut previous = 0;
        for _ in 0..count {
            let id = r.u64()?;
            let version = r.u64()?;
            let feed = u16::from_le_bytes(r.take(2)?.try_into().unwrap());
            r.zero(6)?;
            let received = r.u64()?;
            let published = r.u64()?;
            let last_event = r.u64()?;
            let coverage = DepthCoverage::decode(r.take(32)?)?;
            let bids_n = r.u32()?;
            let asks_n = r.u32()?;
            let states_n = r.u32()?;
            r.zero(4)?;
            if id <= previous
                || version == 0
                || !(1..=5).contains(&feed)
                || received > published
                || last_event > sequence
                || bids_n > maximum_levels
                || asks_n > maximum_levels
                || states_n == 0
                || states_n > 4
                || coverage.version != 1
                || coverage.bid_kind > 2
                || coverage.ask_kind > 2
                || (coverage.bid_kind != 2 && coverage.bid_boundary_ticks != 0)
                || (coverage.ask_kind != 2 && coverage.ask_boundary_ticks != 0)
                || (coverage.bid_kind == 2 && coverage.bid_boundary_ticks <= 0)
                || (coverage.ask_kind == 2 && coverage.ask_boundary_ticks <= 0)
            {
                return Err(Error::Identity);
            }
            previous = id;
            let mut states = StateSlots::default();
            let mut prior_category = 0;
            for _ in 0..states_n {
                let state = MarketState::decode(r.take(512)?)?;
                let h = state.header();
                let cat = market_state_category(h.message_type)?;
                if cat <= prior_category
                    || h.topic_or_scope_id != topic
                    || h.feed_generation != generation
                    || h.instrument_id != id
                    || h.instrument_version != version
                    // Snapshot records use bounded row ordinals, whereas live
                    // records retain the topic sequence. Bootstrapping may be S=0.
                    || h.sequence > last_event.max(4)
                    || h.publish_mono_ns > published
                    || h.ingress_mono_ns > published
                    || cat == 8 && state.feed_state() != feed
                {
                    return Err(Error::Identity);
                }
                prior_category = cat;
                states.insert(cat, state)?;
            }
            if !states.contains_key(&8) {
                return Err(Error::Identity);
            }
            let mut levels = |n: usize, bid: bool| -> Result<Vec<DepthLevel>> {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = DepthLevel::decode(r.take(16)?)?;
                    if l.price_ticks <= 0
                        || l.qty_lots <= 0
                        || v.last().is_some_and(|old: &DepthLevel| {
                            if bid {
                                old.price_ticks <= l.price_ticks
                            } else {
                                old.price_ticks >= l.price_ticks
                            }
                        })
                    {
                        return Err(Error::Identity);
                    }
                    v.push(l);
                }
                Ok(v)
            };
            let bids = levels(bids_n, true)?;
            let asks = levels(asks_n, false)?;
            if bids
                .first()
                .zip(asks.first())
                .is_some_and(|(b, a)| b.price_ticks >= a.price_ticks)
            {
                return Err(Error::Identity);
            }
            books.push(Book {
                id,
                version,
                feed,
                received,
                published,
                last_event,
                bids,
                asks,
                coverage,
                states,
            });
        }
        if !r.0.is_empty() {
            return Err(Error::Bounds);
        }
        let index = FixedIndex::new(&books.iter().map(|book| book.id).collect::<Vec<_>>())
            .map_err(|_| Error::Bounds)?;
        Ok(Self {
            topic,
            generation,
            ring,
            sequence,
            books,
            index,
            depth_workspace: DepthWorkspace::new(maximum_levels),
            maximum_levels,
            blocked,
        })
    }
}
