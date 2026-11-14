use super::*;
use crate::origins::Origin;
use serde_json::Value;
use tickwire_account::{Applied, OrderKey};
use tickwire_recovery::digest;
use tickwire_spot_execution::{
    SpotVenue,
    recovery::{RemoteOrder, decode_order_row},
};
use tickwire_venue as v;

fn lane(source: [u8; 16], label: &[u8]) -> [u8; 16] {
    let mut bytes = b"tickwire.cash.input.v1".to_vec();
    bytes.extend(source);
    bytes.extend(label);
    digest(&bytes)[..16].try_into().unwrap()
}
#[derive(Clone, Copy)]
pub(crate) struct Input {
    pub(crate) stream: [u8; 16],
    pub(crate) seq: u64,
    pub(crate) epoch: u64,
    pub(crate) received: u64,
}
impl Input {
    fn private(c: PrivateCut, kind: &[u8], received: u64) -> Self {
        Self {
            stream: lane(c.stream, kind),
            seq: c.through,
            epoch: c.epoch,
            received,
        }
    }
    fn rest(c: PrivateCut, kind: &[u8], raw: &[u8], received: u64) -> Self {
        let mut id = kind.to_vec();
        id.extend(digest(raw));
        Self {
            stream: lane(c.stream, &id),
            seq: 1,
            epoch: c.epoch,
            received,
        }
    }
}
impl<
    'a,
    B: Backend,
    A: Authority,
    T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
    S: BlobStore,
> AccountRunner<'a, B, A, T, S>
{
    // A re-read of the same immutable REST row must reproduce its original ingress envelope.
    // The gateway still re-compares the canonical bytes before declaring it a duplicate.
    pub(crate) fn rest_input(
        &self,
        cut: PrivateCut,
        kind: &[u8],
        row: &[u8],
        received: u64,
    ) -> Input {
        let mut i = Input::rest(cut, kind, row, received);
        if let Some(prior) = self.session.gateway().ingress_event(i.stream, i.seq) {
            i.epoch = prior.event().identity.source_connection_epoch;
            i.received = prior.event().identity.received_mono_ns;
        }
        i
    }
    pub(crate) fn origin(&self, client: &str) -> Result<(Origin, OrderKey)> {
        let origin = self
            .origins
            .lookup(self.scope(), b"client", client.as_bytes())?;
        let cmd = self
            .session
            .gateway()
            .command(origin.intent)
            .ok_or(Error::Origin("unadmitted native client ID"))?;
        if cmd.digest() != origin.digest {
            return Err(Error::Origin(
                "native client ID differs from the account WAL",
            ));
        }
        let key = OrderKey {
            intent: origin.intent,
            client_id: v::ClientId::new(client)
                .map_err(|_| Error::Origin("invalid native client ID"))?,
        };
        Ok((origin, key))
    }
    pub(crate) fn record_order(&mut self, remote: &RemoteOrder, i: Input) -> Result<()> {
        let (origin, k) = self.origin(&remote.client_id)?;

        let cmd = self
            .session
            .gateway()
            .command(origin.intent)
            .ok_or(Error::Origin("order was not admitted"))?;
        let context = &cmd.command().context;
        let r = &context.request;
        let native = v::VenueOrderId::new(&remote.native_id)
            .map_err(|_| Error::Origin("invalid native order ID"))?;
        let venue_ns = remote
            .updated_ms
            .checked_mul(1_000_000)
            .ok_or(Error::Origin("native order time overflow"))?;
        let event = v::CanonicalEvent::new(v::VenueEvent {
            identity: v::EventIdentity {
                risk_domain_id: r.risk_domain_id,
                venue: r.venue,
                environment: r.environment,
                account: r.account,
                product: r.product,
                instrument_id: context.instrument.instrument_id,
                instrument_version: context.instrument.instrument_version,
                source_connection_epoch: i.epoch,
                source_sequence: None,
                event_id: v::BoundedText::new(format!(
                    "{:032x}-{}",
                    u128::from_be_bytes(i.stream),
                    i.seq
                ))
                .map_err(|_| Error::Origin("invalid source position"))?,
                venue_time_ns: Some(venue_ns),
                received_mono_ns: i.received,
            },
            body: v::VenueEventBody::Order(Box::new(v::OrderObservation {
                order: v::OrderIdentity {
                    order_id: origin.order,
                    original_intent_id: k.intent.intent_id,
                    original_intent_exec_epoch: k.intent.epoch,
                    client_id: Some(k.client_id.clone()),
                    venue_order_id: Some(native),
                    parent_order_id: None,
                    list_id: None,
                    venue_algo_id: None,
                    venue_list_id: None,
                    list_client_id: None,
                    algo_client_id: None,
                },
                status: remote.status,
                side: remote.side,
                position_side: v::PositionSide::Both,
                original_quantity: Some(remote.qty),
                cumulative_quantity: Some(remote.cumulative),
                cumulative_quote: None,
                leaves_quantity: None,
                order_price: remote.price,
            })),
        })
        .map_err(|_| Error::Origin("invalid canonical cash order"))?;
        if self
            .session
            .gateway()
            .ingress_event(i.stream, i.seq)
            .is_none()
        {
            compare::terms(self.account(), &k, remote)?;
        } else {
            // The canonical order observation omits type and time in force. Even a
            // known cursor must still match the terms of the original admission.
            compare::admitted_terms(self.account(), &k, remote)?;
        }
        self.session
            .gateway_mut()
            .record_ingress_event(i.stream, i.seq, event)
            .map_err(|e| Error::Session(SessionError::Gateway(e)))?;
        Ok(())
    }
    pub fn apply_private_cash(
        &mut self,
        channel: &str,
        row: &Value,
        cut: PrivateCut,
        received_ns: u64,
    ) -> Result<CashUpdate> {
        let outcome = self.apply_private_row(channel, row, cut, received_ns);
        if !matches!(outcome, Ok(CashUpdate::Accounted)) {
            self.freeze_cash();
        }
        outcome
    }
    fn apply_private_row(
        &mut self,
        channel: &str,
        row: &Value,
        cut: PrivateCut,
        received_ns: u64,
    ) -> Result<CashUpdate> {
        PrivateCut::stable([cut; 2])?;
        let bybit = self.scope().venue == SpotVenue::Bybit.id();
        let venue = SpotVenue::from_id(self.scope().venue).ok_or(Error::Origin("cash venue"))?;
        match channel {
            "balance" => Ok(CashUpdate::Reconcile), // A wallet notification forces a new complete inventory check.
            "fills" | "orders" => {
                let mut fill_client: Option<String> = None;
                if channel == "fills"
                    || (!bybit
                        && row["tradeId"]
                            .as_str()
                            .is_some_and(|s| !s.is_empty() && s != "0"))
                {
                    let client = row[if bybit { "orderLinkId" } else { "clOrdId" }]
                        .as_str()
                        .ok_or(Error::Origin("no client ID for private fill"))?;
                    let i = Input::private(cut, b"ws-fill", received_ns);
                    self.record_fill(
                        client,
                        i.stream,
                        i.seq,
                        i.epoch,
                        i.received,
                        &serde_json::to_vec(row)
                            .map_err(|_| Error::Origin("invalid private fill"))?,
                    )?;
                    fill_client = Some(client.to_owned());
                }
                if channel == "orders" {
                    let order = decode_order_row(venue, row)
                        .map_err(|_| Error::Origin("unsupported or invalid private order"))?;
                    self.record_order(&order, Input::private(cut, b"ws-order", received_ns))?;
                    let (_, key) = self.origin(&order.client_id)?;
                    let view = self
                        .account()
                        .order(&key)
                        .ok_or(Error::Origin("missing order"))?;
                    let cum_fixed =
                        compare::amount(order.cumulative, view.filled_quantity.scale())?;
                    if self.query_desk.is_none() {
                        if !compare::open(order.status) || cum_fixed != view.filled_quantity {
                            return Ok(CashUpdate::Reconcile);
                        }
                    } else if self.account().frozen()
                        || cum_fixed.atoms() < view.filled_quantity.atoms()
                    {
                        return Ok(CashUpdate::Reconcile);
                    } else if cum_fixed.atoms() > view.filled_quantity.atoms() {
                        self.note_expected_fill_atoms(&key, cum_fixed.atoms());
                        if !compare::open(order.status)
                            || self.account().order_in_doubt(&key)
                                != Some(tickwire_account::DoubtReason::CancelAmbiguous)
                        {
                            self.session
                                .gateway_mut()
                                .isolate_intent_in_doubt(
                                    key.intent,
                                    tickwire_account::DoubtReason::FillCatchup,
                                )
                                .map_err(|e| Error::Session(SessionError::Gateway(e)))?;
                        }
                        if !self.enqueue_query_desk_trades(&key, received_ns) {
                            return Ok(CashUpdate::Reconcile);
                        }
                        return Ok(CashUpdate::Accounted);
                    } else if !compare::open(order.status) {
                        self.clear_expected_fill_atoms(&key);
                        self.close_terminal_fees_if_ready(&key, received_ns)?;
                        return Ok(if self.account().frozen() {
                            CashUpdate::Reconcile
                        } else {
                            CashUpdate::Accounted
                        });
                    } else if self.account().order_in_doubt(&key).is_none() {
                        self.clear_expected_fill_atoms(&key);
                        if self
                            .query_desk
                            .as_ref()
                            .is_some_and(|d| d.has_pending_cancel(&key))
                        {
                            self.session
                                .gateway_mut()
                                .isolate_intent_in_doubt(
                                    key.intent,
                                    tickwire_account::DoubtReason::CancelAmbiguous,
                                )
                                .map_err(|e| Error::Session(SessionError::Gateway(e)))?;
                        } else if let Some(desk) = self.query_desk.as_mut() {
                            desk.remove_order(&key);
                        }
                    }
                } else if let Some(client) = fill_client
                    && self.query_desk.is_some()
                    && !self.account().frozen()
                {
                    let (_, key) = self.origin(&client)?;
                    if let Some(view) = self.account().order(&key) {
                        if view.filled_quantity.atoms() >= self.expected_fill_atoms(&key) {
                            self.clear_expected_fill_atoms(&key);
                        } else if view.terminal
                            || self.account().order_in_doubt(&key)
                                != Some(tickwire_account::DoubtReason::CancelAmbiguous)
                        {
                            self.session
                                .gateway_mut()
                                .isolate_intent_in_doubt(
                                    key.intent,
                                    tickwire_account::DoubtReason::FillCatchup,
                                )
                                .map_err(|e| Error::Session(SessionError::Gateway(e)))?;
                        }
                    }
                    self.close_terminal_fees_if_ready(&key, received_ns)?;
                }
                Ok(if self.account().frozen() {
                    CashUpdate::Reconcile
                } else {
                    CashUpdate::Accounted
                })
            }
            _ => Err(Error::Origin("unknown private channel")),
        }
    }
    pub(super) fn apply_scan(
        &mut self,
        e: &RestEvidence,
        cut: PrivateCut,
        received: u64,
    ) -> Result<()> {
        let orders = compare::orders(e)?;
        for o in orders.values() {
            let (_, k) = self.origin(&o.client_id)?;
            compare::terms(self.account(), &k, o)?;
        }
        for f in &e.fills {
            let (_, k) = self.origin(&f.client_id)?;
            let (instrument, spec) = self
                .account()
                .order_terms(&k)
                .ok_or(Error::Origin("REST fill missing local order"))?;
            if instrument.symbol.as_str() != f.symbol
                || spec.side != f.side
                || self
                    .account()
                    .venue_order_id(&k)
                    .is_some_and(|id| id.as_str() != f.native_id)
            {
                return Err(Error::Origin("REST fill differs from admission"));
            }
            let i = self.rest_input(cut, b"rest-fill", &f.wire_row, received);
            self.record_fill(
                &f.client_id,
                i.stream,
                i.seq,
                i.epoch,
                i.received,
                &f.wire_row,
            )?;
        }
        for o in orders.values() {
            let (_, key) = self.origin(&o.client_id)?;
            let local = self
                .account()
                .order(&key)
                .ok_or(Error::Origin("missing local order"))?;
            if compare::amount(o.cumulative, local.filled_quantity.scale())?
                != local.filled_quantity
            {
                return Err(Error::Origin(
                    "native cumulative is not explained by individual fills",
                ));
            }
            let raw = serde_json::to_vec(&(
                &o.symbol,
                &o.client_id,
                &o.native_id,
                o.updated_ms,
                format!("{:?}", o.status),
                o.cumulative.as_plain_string(),
            ))
            .map_err(|_| Error::Origin("invalid REST order source"))?;
            let i = self.rest_input(cut, b"rest-order", &raw, received);
            self.record_order(o, i)?;
        }
        // Release the residual fee buffer only after all fills and balances agree, even if the
        // native order is already terminal. Crash recovery replays the synchronized account facts.
        compare::account(self.session.gateway().baseline(), self.account(), e)?;
        for o in orders.values().filter(|o| !compare::open(o.status)) {
            let (_, key) = self.origin(&o.client_id)?;
            let local = self
                .account()
                .order(&key)
                .ok_or(Error::Origin("missing terminal order"))?;
            if !local.fee_reconciled {
                let applied = self
                    .session
                    .gateway_mut()
                    .record_fact(RecoveryRecord::FeesReconciled {
                        order: key.clone(),
                        cumulative: local.filled_quantity,
                    })
                    .map_err(|e| Error::Session(SessionError::Gateway(e)))?;
                if applied != Applied::New {
                    return Err(Error::Origin("fee closure did not advance the account"));
                }
            }
            if !self.account().is_intent_active(key.intent) {
                let stamp = self
                    .account()
                    .intent_retired_mono_ns(key.intent)
                    .unwrap_or(received);
                self.origins.retire_intent(key.intent, stamp);
            }
        }
        self.origins.evict_expired(received);
        Ok(())
    }
}
