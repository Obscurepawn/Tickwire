//! The adapter shared by durable private intake and the short-lived network observer.
use super::{Batch, PrivateFeed, Reaction};
use crate::{Credentials, SpotVenue, network::Result};
use std::{
    io::{ErrorKind, Read, Write},
    time::{Duration, Instant},
};
use tickwire_transport::{
    http::ResponseDecoder,
    ws::{ClientHandshake, Decoder, Opcode, PollMessage, encode_client},
};
pub(super) const LIMIT: usize = 1024 * 1024;
pub(super) enum Event {
    Idle,
    Control,
    Ready,
    Data(Batch, Vec<u8>),
}
#[derive(Clone, Copy)]
pub(super) struct Timing {
    pub setup: Duration,
    pub ping: Duration,
    pub idle: Duration,
}
impl Default for Timing {
    fn default() -> Self {
        Self {
            setup: Duration::from_secs(10),
            ping: Duration::from_secs(10),
            idle: Duration::from_secs(25),
        }
    }
}
fn frame(s: &mut impl Write, op: Opcode, data: &[u8]) -> Result<()> {
    s.write_all(&encode_client(op, data, LIMIT)?)?;
    s.flush()?;
    Ok(())
}
fn read(s: &mut impl Read, b: &mut [u8]) -> Result<usize> {
    match s.read(b) {
        Ok(0) => Err("private peer closed".into()),
        Err(e)
            if matches!(
                e.kind(),
                ErrorKind::TimedOut | ErrorKind::WouldBlock | ErrorKind::Interrupted
            ) =>
        {
            Ok(0)
        }
        v => Ok(v?),
    }
}
pub(super) struct Socket<S> {
    io: S,
    ws: Decoder,
    feed: PrivateFeed,
    venue: SpotVenue,
    ready: bool,
    started: Instant,
    seen: Instant,
    ping: Instant,
    timing: Timing,
    idle: bool,
}
impl<S: Read + Write> Socket<S> {
    pub(super) fn open(
        mut io: S,
        host: &str,
        path: &str,
        creds: &Credentials<'_>,
        stamp: impl FnOnce() -> Result<u64>,
        timing: Timing,
    ) -> Result<Self> {
        let started = Instant::now();
        let upgrade = ClientHandshake::new(host, path, &[], 4096)?;
        io.write_all(upgrade.request())?;
        io.flush()?;
        let mut http = ResponseDecoder::new(8192, 64)?;
        let mut ws = Decoder::new(LIMIT, LIMIT)?;
        let mut buf = [0; 8192];
        loop {
            if started.elapsed() >= timing.setup {
                return Err("private upgrade timeout".into());
            }
            let n = read(&mut io, &mut buf)?;
            http.push(&buf[..n])?;
            if let Some(resp) = http.next_response()? {
                upgrade.validate(&resp)?;
                ws.push(&http.take_buffered())?;
                break;
            }
        }
        let feed = PrivateFeed::new(creds, stamp()?)?;
        frame(&mut io, Opcode::Text, &serde_json::to_vec(feed.login())?)?;
        Ok(Self {
            io,
            ws,
            feed,
            venue: creds.venue,
            ready: false,
            started,
            seen: started,
            ping: started,
            timing,
            idle: false,
        })
    }
    pub(super) fn awaiting_input(&self) -> bool {
        self.idle
    }
    pub(super) fn step(&mut self, mut pending: impl FnMut()) -> Result<Event> {
        self.idle = false;
        let now = Instant::now();
        if !self.ready && now.duration_since(self.started) >= self.timing.setup {
            return Err("private subscription timed out".into());
        }
        if now.duration_since(self.seen) >= self.timing.idle {
            return Err("private heartbeat expired".into());
        }
        if self.ready && now.duration_since(self.ping) >= self.timing.ping {
            frame(
                &mut self.io,
                Opcode::Text,
                if self.venue == SpotVenue::Bybit {
                    br#"{"op":"ping"}"#
                } else {
                    b"ping"
                },
            )?;
            self.ping = now;
        }
        match self.ws.poll_message()? {
            PollMessage::NeedInput => {
                let mut buf = [0; 8192];
                let n = read(&mut self.io, &mut buf)?;
                if n > 0 {
                    pending();
                    self.ws.push(&buf[..n])?;
                } else {
                    self.idle = self.ws.at_message_boundary();
                }
                Ok(Event::Idle)
            }
            PollMessage::Yielded => Ok(Event::Idle),
            PollMessage::Message(msg) => {
                self.seen = now;
                match msg.opcode {
                    Opcode::Text => match self.feed.on_text(&msg.payload)? {
                        Reaction::Subscribe(sub) => {
                            frame(&mut self.io, Opcode::Text, &serde_json::to_vec(&sub)?)?;
                            Ok(Event::Control)
                        }
                        Reaction::Data(batch) => Ok(Event::Data(batch, msg.payload)),
                        Reaction::Control if !self.ready && self.feed.subscribed() => {
                            self.ready = true;
                            Ok(Event::Ready)
                        }
                        Reaction::Control => Ok(Event::Control),
                    },
                    Opcode::Pong => Ok(Event::Control),
                    Opcode::Ping => {
                        frame(&mut self.io, Opcode::Pong, &msg.payload)?;
                        Ok(Event::Control)
                    }
                    _ => Err("private socket closed or unexpected opcode".into()),
                }
            }
        }
    }
    pub(super) fn close(&mut self) -> Result<()> {
        frame(&mut self.io, Opcode::Close, &1000u16.to_be_bytes())
    }
}
