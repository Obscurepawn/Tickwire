use super::{Feed, Result, invalid};
use tickwire_book::BookKey;
use tickwire_instrument_index::FixedIndex;

/// Process-local book identities from the complete trusted startup feed set.
/// Topic row ordering and externally visible instrument IDs are independent.
pub struct FeedIndex {
    instruments: FixedIndex,
}
impl FeedIndex {
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
            instruments: FixedIndex::new(&ids).map_err(|_| invalid("MD instrument index"))?,
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
