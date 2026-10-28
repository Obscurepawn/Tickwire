use super::{Feed, Result, invalid};
use tickwire_book::BookKey;
use tickwire_instrument_index::{AppendIndex, DenseId, FixedIndex};

/// Process-local book identities from the complete trusted startup feed set.
/// Topic row ordering and externally visible instrument IDs are independent.
pub struct FeedIndex {
    instruments: Lookup,
    // Cold owner metadata preserves order when preparing a complete successor.
    ids: Vec<u64>,
}
enum Lookup {
    Fixed(FixedIndex),
    Append(AppendIndex),
}
impl Lookup {
    fn lookup(&self, id: u64) -> Option<DenseId> {
        match self {
            Self::Fixed(index) => index.lookup(id),
            Self::Append(index) => index.lookup(id),
        }
    }
}
impl FeedIndex {
    /// Ordered stable IDs whose positions are the process-local book indices.
    pub fn instrument_ids(&self) -> &[u64] {
        &self.ids
    }
    /// Restore an exact recorded directory, independently of feed ordering.
    pub fn restore(feeds: &[Feed], ids: &[u64]) -> Result<Self> {
        if feeds.is_empty()
            || feeds.len() > 32
            || ids.is_empty()
            || ids.len() > 32
            || ids
                .iter()
                .enumerate()
                .any(|(i, id)| *id == 0 || ids[..i].contains(id))
            || feeds.iter().any(|feed| !ids.contains(&feed.instrument.id))
            || ids
                .iter()
                .any(|id| !feeds.iter().any(|feed| feed.instrument.id == *id))
        {
            return Err(invalid("MD restored index membership/capacity"));
        }
        Ok(Self {
            instruments: Lookup::Fixed(
                FixedIndex::new(ids).map_err(|_| invalid("MD restored instrument index"))?,
            ),
            ids: ids.to_vec(),
        })
    }
    /// Prepare a complete successor without modifying this active index.
    pub fn expanded(&self, feeds: &[Feed]) -> Result<Self> {
        if feeds.is_empty()
            || feeds.len() > 32
            || feeds.iter().any(|feed| feed.instrument.id == 0)
            || self
                .ids
                .iter()
                .any(|id| !feeds.iter().any(|feed| feed.instrument.id == *id))
        {
            return Err(invalid("MD index expansion membership/capacity"));
        }
        let mut ids = self.ids.clone();
        for feed in feeds {
            if !ids.contains(&feed.instrument.id) {
                ids.push(feed.instrument.id);
            }
        }
        let mut index = AppendIndex::with_capacity(ids.len())
            .map_err(|_| invalid("MD instrument index allocation"))?;
        for &id in &ids {
            index
                .insert(id)
                .map_err(|_| invalid("MD instrument index append"))?;
        }
        Ok(Self {
            instruments: Lookup::Append(index),
            ids,
        })
    }
    pub fn new(feeds: &[Feed]) -> Result<Self> {
        if feeds.is_empty() || feeds.len() > 32 {
            return Err(invalid("MD feed index capacity"));
        }
        let mut ids = Vec::with_capacity(feeds.len());
        for feed in feeds {
            if feed.instrument.id == 0 {
                return Err(invalid("MD feed instrument identity"));
            }
            // Startup has at most 32 feeds; preserve first occurrence order.
            if !ids.contains(&feed.instrument.id) {
                ids.push(feed.instrument.id);
            }
        }
        Ok(Self {
            instruments: Lookup::Fixed(
                FixedIndex::new(&ids).map_err(|_| invalid("MD instrument index"))?,
            ),
            ids,
        })
    }
    /// Bind a known stable ID before constructing a depth owner or publisher.
    /// An unknown ID is never registered by this lookup.
    pub fn book_key(&self, feed: &Feed) -> Result<BookKey> {
        let slot = self
            .instruments
            .lookup(feed.instrument.id)
            .ok_or_else(|| invalid("unknown MD feed instrument"))?;
        if feed.venue_id == 0 || feed.stream_id == 0 {
            return Err(invalid("MD book venue/source identity"));
        }
        Ok(BookKey {
            venue: feed.venue_id,
            instrument: slot
                .index()
                .try_into()
                .map_err(|_| invalid("MD book slot range"))?,
            source: feed
                .stream_id
                .try_into()
                .map_err(|_| invalid("book stream range"))?,
        })
    }
}

#[cfg(test)]
#[path = "feed_index_tests.rs"]
mod tests;
