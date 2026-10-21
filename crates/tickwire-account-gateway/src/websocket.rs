//! Closed protocol-control operations. Application Text frames use normal admission.
use crate::*;
use tickwire_transport::{
    http::ResponseDecoder,
    ws::{ClientHandshake, Opcode},
};

/// Carries only WebSocket control payloads; no application Text variant exists.
pub enum WebSocketControl<'a> {
    Ping(&'a [u8]),
    Pong(&'a [u8]),
    Close(&'a [u8]),
}
#[derive(Clone, Copy)]
struct ControlSlot {
    token: u64,
    close: bool,
}
pub(crate) struct WebSocketState {
    handshake: Option<ClientHandshake>,
    response: ResponseDecoder,
    deadline: Instant,
    ready: bool,
    control: Option<ControlSlot>,
}
impl<S: Backend, A: Authority, E: Encoder> AccountGateway<S, A, E> {
    /// Queue the fixed venue Upgrade only. This method does no output I/O.
    pub fn begin_websocket(&mut self, deadline: Instant) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        if self.rest_started || self.websocket.is_some() || self.pending.is_some() {
            return Err(Error::Busy);
        }
        let path = match self.config.route.product {
            tickwire_venue::Product::Spot => "/ws-api/v3",
            tickwire_venue::Product::LinearPerpetual | tickwire_venue::Product::LinearFuture => {
                "/ws-fapi/v1"
            }
            _ => return Err(Error::Identity),
        };
        let handshake =
            ClientHandshake::new(&self.config.route.authority, path, &[], self.max_send_bytes)?;
        let send = PreparedSend::new(handshake.request(), deadline, self.max_send_bytes)?;
        let token = self.connection.as_mut().ok_or(Error::Halted)?.queue(send)?;
        self.websocket = Some(WebSocketState {
            handshake: Some(handshake),
            response: ResponseDecoder::new(32_768, 64)?,
            deadline,
            ready: false,
            control: Some(ControlSlot {
                token,
                close: false,
            }),
        });
        Ok(())
    }
    pub fn websocket_ready(&self) -> bool {
        !self.halted && self.websocket.as_ref().is_some_and(|s| s.ready)
    }
    pub fn websocket_control_pending(&self) -> bool {
        self.websocket.as_ref().is_some_and(|s| s.control.is_some())
    }
    pub(crate) fn websocket_blocks_application(&self) -> bool {
        self.websocket
            .as_ref()
            .is_some_and(|s| !s.ready || s.control.is_some())
    }
    /// Queue a bounded control frame only at an application frame boundary.
    pub fn queue_websocket_control(
        &mut self,
        control: WebSocketControl<'_>,
        deadline: Instant,
    ) -> Result<()> {
        if !self.websocket_ready() {
            return Err(Error::Halted);
        }
        if self.pending.is_some() || self.websocket_control_pending() {
            return Err(Error::Busy);
        }
        let (opcode, payload) = match control {
            WebSocketControl::Ping(p) => (Opcode::Ping, p),
            WebSocketControl::Pong(p) => (Opcode::Pong, p),
            WebSocketControl::Close(p) => (Opcode::Close, p),
        };
        let bytes = tickwire_transport::ws::encode_client(opcode, payload, self.max_send_bytes)?;
        let token = self
            .connection
            .as_mut()
            .ok_or(Error::Halted)?
            .queue(PreparedSend::new(&bytes, deadline, self.max_send_bytes)?)?;
        self.websocket.as_mut().unwrap().control = Some(ControlSlot {
            token,
            close: opcode == Opcode::Close,
        });
        Ok(())
    }
    /// `permit` applies only to Upgrade/Ping/Pong/Close. Business frames always
    /// use Authority::commit_state/current_gate inside the normal account poll.
    pub fn poll_websocket(
        &mut self,
        now: Instant,
        budget: IoBudget,
        mut permit: impl FnMut() -> bool,
    ) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        if self.websocket.is_none() {
            return Err(Error::Invalid);
        }
        if !self.websocket_blocks_application() {
            return self.poll(now, budget);
        }
        let result = self.poll_protocol(now, budget, &mut permit);
        if result.is_err() {
            self.halt();
        }
        result
    }
    fn poll_protocol(
        &mut self,
        now: Instant,
        budget: IoBudget,
        permit: &mut impl FnMut() -> bool,
    ) -> Result<()> {
        let driven = self
            .connection
            .as_mut()
            .ok_or(Error::Halted)?
            .drive(now, budget)?;
        if driven.peer_closed {
            self.halt();
            return Ok(());
        }
        self.settle_control()?;
        if self.halted {
            return Ok(());
        }
        if self.websocket_control_pending() {
            let report = self
                .connection
                .as_mut()
                .ok_or(Error::Halted)?
                .try_send(now, permit)?;
            self.check_control_report(report)?;
        }
        if self.halted {
            return Ok(());
        }
        if !self.websocket.as_ref().unwrap().ready {
            if now >= self.websocket.as_ref().unwrap().deadline {
                return Err(Error::Transport(tickwire_transport::Error::Protocol(
                    "websocket upgrade deadline",
                )));
            }
            let mut bytes = [0; 4096];
            let count = self
                .connection
                .as_mut()
                .ok_or(Error::Halted)?
                .read(&mut bytes);
            let ws = self.websocket.as_mut().unwrap();
            if count > 0 {
                ws.response.push(&bytes[..count])?;
            }
            if let Some(response) = ws.response.next_response()? {
                ws.handshake
                    .as_ref()
                    .ok_or(Error::Invalid)?
                    .validate(&response)?;
                let tail = ws.response.take_buffered();
                if self.retained_receive.len() + tail.len() > self.max_receive_bytes {
                    return Err(Error::Capacity);
                }
                self.retained_receive.extend(tail);
                ws.handshake = None;
                ws.ready = true;
            }
        }
        Ok(())
    }
    fn settle_control(&mut self) -> Result<()> {
        if !self.websocket_control_pending() {
            return Ok(());
        }
        let report = self
            .connection
            .as_ref()
            .and_then(Connection::send_report)
            .ok_or(Error::Invalid)?;
        self.check_control_report(report)
    }
    fn check_control_report(&mut self, report: SendReport) -> Result<()> {
        let slot = self
            .websocket
            .as_ref()
            .and_then(|s| s.control)
            .ok_or(Error::Invalid)?;
        if slot.token != report.id {
            return Err(Error::Identity);
        }
        match report.state {
            SendState::Queued | SendState::MayHaveSent => {}
            SendState::Flushed => {
                self.connection.as_mut().unwrap().take_finished();
                self.websocket.as_mut().unwrap().control = None;
                if slot.close {
                    self.halt();
                }
            }
            SendState::NoBytes(_) | SendState::Failed(_) => self.halt(),
        }
        Ok(())
    }
}
