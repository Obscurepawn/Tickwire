//! In-memory venue request replies. An ACK does not establish a terminal account fact.
use crate::{ClientId, Decimal, OrderStatus, Side, VenueOrderId};
#[derive(Debug, PartialEq, Eq)]
pub enum RequestReply {
    Acknowledged {
        client_id: Option<ClientId>,
        venue_order_id: VenueOrderId,
    },
    Order(OrderSnapshot),
    /// The venue returned an error code, which does not establish the original order's state.
    RequestIssue {
        code: String,
    },
    Unknown,
}
#[derive(Debug, PartialEq, Eq)]
pub struct OrderSnapshot {
    pub venue_order_id: VenueOrderId,
    pub client_id: Option<ClientId>,
    pub status: OrderStatus,
    pub side: Side,
    pub cumulative_base: Decimal,
    pub venue_time_ns: u64,
}
