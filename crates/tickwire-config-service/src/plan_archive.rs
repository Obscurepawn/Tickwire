//! Durable preparation content. Saving these bytes grants no deployment authority.
pub(crate) mod content;
mod recording;
pub(crate) use content::validate_record;
pub(crate) use recording::ArchiveRequest;
pub use recording::{PendingRecord, RecordProgress};
