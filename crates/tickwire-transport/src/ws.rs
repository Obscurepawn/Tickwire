use crate::{
    http::{self, Response},
    *,
};
use base64::Engine;
use sha1::Digest;
const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Opcode {
    Text = 1,
    Binary = 2,
    Close = 8,
    Ping = 9,
    Pong = 10,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Message {
    pub opcode: Opcode,
    pub payload: Vec<u8>,
}
pub struct ClientHandshake {
    request: Vec<u8>,
    accept: Vec<u8>,
}
impl ClientHandshake {
    pub fn new(host: &str, path: &str, headers: &[(&str, &str)], max: usize) -> Result<Self> {
        if host.is_empty() {
            return Err(Error::Protocol("empty host"));
        }
        if headers.iter().any(|(n, _)| {
            ["host", "connection", "upgrade"]
                .iter()
                .any(|v| n.eq_ignore_ascii_case(v))
                || n.to_ascii_lowercase().starts_with("sec-websocket-")
        }) {
            return Err(Error::Protocol("reserved websocket header"));
        }
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).map_err(|_| Error::Protocol("random source"))?;
        let key = base64::engine::general_purpose::STANDARD.encode(nonce);
        let mut hash = sha1::Sha1::new();
        hash.update(&key);
        hash.update(GUID);
        let accept = base64::engine::general_purpose::STANDARD
            .encode(hash.finalize())
            .into_bytes();
        let mut h = vec![
            ("Host", host),
            ("Upgrade", "websocket"),
            ("Connection", "Upgrade"),
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Key", key.as_str()),
        ];
        h.extend_from_slice(headers);
        let request = http::encode_request("GET", path, &h, &[], max)?;
        Ok(Self { request, accept })
    }
    pub fn request(&self) -> &[u8] {
        &self.request
    }
    pub fn validate(&self, response: &Response) -> Result<()> {
        if response.status != 101 || !response.body.is_empty() {
            return Err(Error::Protocol("websocket upgrade status"));
        }
        let get = |name: &str| -> Result<&[u8]> {
            let mut values = response
                .headers
                .iter()
                .filter(|(n, _)| n.eq_ignore_ascii_case(name));
            let value = values
                .next()
                .ok_or(Error::Protocol("missing upgrade header"))?;
            if values.next().is_some() {
                return Err(Error::Protocol("duplicate upgrade header"));
            }
            Ok(&value.1)
        };
        if !get("upgrade")?.eq_ignore_ascii_case(b"websocket")
            || get("sec-websocket-accept")? != self.accept
            || !get("connection")?
                .split(|b| *b == b',')
                .any(|s| s.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
        {
            return Err(Error::Protocol("websocket upgrade verification"));
        }
        if response.headers.iter().any(|(n, _)| {
            n.eq_ignore_ascii_case("sec-websocket-extensions")
                || n.eq_ignore_ascii_case("sec-websocket-protocol")
        }) {
            return Err(Error::Protocol("unsolicited extension/subprotocol"));
        }
        Ok(())
    }
}
fn valid_payload(opcode: Opcode, payload: &[u8]) -> Result<()> {
    if matches!(opcode, Opcode::Close | Opcode::Ping | Opcode::Pong) && payload.len() > 125 {
        return Err(Error::Protocol("control length"));
    }
    if opcode == Opcode::Text && std::str::from_utf8(payload).is_err() {
        return Err(Error::Protocol("text utf8"));
    }
    if opcode == Opcode::Close {
        if payload.len() == 1 {
            return Err(Error::Protocol("close length"));
        }
        if payload.len() >= 2 {
            let code = u16::from_be_bytes([payload[0], payload[1]]);
            if !((1000..=1014).contains(&code) && ![1004, 1005, 1006].contains(&code)
                || (3000..=4999).contains(&code))
                || std::str::from_utf8(&payload[2..]).is_err()
            {
                return Err(Error::Protocol("close status/reason"));
            }
        }
    }
    Ok(())
}
pub fn encode_client(opcode: Opcode, payload: &[u8], max: usize) -> Result<Vec<u8>> {
    valid_payload(opcode, payload)?;
    let header = if payload.len() < 126 {
        6
    } else if payload.len() <= 65535 {
        8
    } else {
        14
    };
    let len = payload.len().checked_add(header).ok_or(Error::Bounds)?;
    if len > max || max > 64 * 1024 * 1024 {
        return Err(Error::Bounds);
    }
    let mut mask = [0; 4];
    getrandom::fill(&mut mask).map_err(|_| Error::Protocol("random source"))?;
    let mut out = Vec::with_capacity(len);
    out.push(0x80 | opcode as u8);
    if payload.len() < 126 {
        out.push(0x80 | payload.len() as u8)
    } else if payload.len() <= 65535 {
        out.push(0x80 | 126);
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes())
    } else {
        out.push(0x80 | 127);
        out.extend_from_slice(&(payload.len() as u64).to_be_bytes())
    }
    out.extend_from_slice(&mask);
    out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    Ok(out)
}
#[derive(Debug, PartialEq, Eq)]
pub enum PollMessage {
    Message(Message),
    NeedInput,
    Yielded,
}
pub struct Decoder {
    buffer: Vec<u8>,
    fragments: Vec<u8>,
    fragment_opcode: Option<Opcode>,
    max_frame: usize,
    max_message: usize,
    failed: bool,
    closed: bool,
}
impl Decoder {
    pub fn finish(&mut self) -> Result<()> {
        if self.failed {
            return Err(Error::Closed);
        }
        self.failed = true;
        if !self.closed || !self.buffer.is_empty() {
            return Err(Error::Protocol("websocket EOF before complete Close"));
        }
        Ok(())
    }
    pub fn new(max_frame: usize, max_message: usize) -> Result<Self> {
        if max_frame == 0
            || max_message == 0
            || max_frame > 64 * 1024 * 1024
            || max_message > 64 * 1024 * 1024
        {
            return Err(Error::Bounds);
        }
        Ok(Self {
            buffer: Vec::new(),
            fragments: Vec::new(),
            fragment_opcode: None,
            max_frame,
            max_message,
            failed: false,
            closed: false,
        })
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<()> {
        if self.failed || self.closed {
            return Err(Error::Closed);
        }
        if bytes.len() > self.max_message + self.max_frame + 14 - self.buffer.len() {
            self.failed = true;
            return Err(Error::Bounds);
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }
    /// Compatibility adapter. Reactor callers should use poll_message to
    /// distinguish an exhausted processing budget from missing network input.
    pub fn next_message(&mut self) -> Result<Option<Message>> {
        Ok(match self.poll_message()? {
            PollMessage::Message(message) => Some(message),
            PollMessage::NeedInput | PollMessage::Yielded => None,
        })
    }
    pub fn poll_message(&mut self) -> Result<PollMessage> {
        if self.failed || self.closed {
            return Err(Error::Closed);
        }
        let result = self.parse();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn parse(&mut self) -> Result<PollMessage> {
        // A call consumes at most 32 frames, including empty continuation frames.
        for _ in 0..32 {
            if self.buffer.len() < 2 {
                return Ok(PollMessage::NeedInput);
            }
            let a = self.buffer[0];
            let b = self.buffer[1];
            let fin = a & 0x80 != 0;
            let code = a & 15;
            if a & 0x70 != 0 || b & 0x80 != 0 || ![0, 1, 2, 8, 9, 10].contains(&code) {
                return Err(Error::Protocol("websocket frame flags"));
            }
            let control = code >= 8;
            let small = b & 127;
            if control && (!fin || small > 125) {
                return Err(Error::Protocol("control frame"));
            }
            let (header, len) = match small {
                126 => {
                    if self.buffer.len() < 4 {
                        return Ok(PollMessage::NeedInput);
                    }
                    let n = u16::from_be_bytes([self.buffer[2], self.buffer[3]]) as usize;
                    if n < 126 {
                        return Err(Error::Protocol("nonminimal frame length"));
                    }
                    (4, n)
                }
                127 => {
                    if self.buffer.len() < 10 {
                        return Ok(PollMessage::NeedInput);
                    }
                    let n = u64::from_be_bytes(self.buffer[2..10].try_into().unwrap());
                    if n < 65536 || n >> 63 != 0 {
                        return Err(Error::Protocol("frame length"));
                    }
                    (10, usize::try_from(n).map_err(|_| Error::Bounds)?)
                }
                n => (2, n as usize),
            };
            if len > self.max_frame
                || (!control && len > self.max_message.saturating_sub(self.fragments.len()))
            {
                return Err(Error::Bounds);
            }
            if code == 0 && self.fragment_opcode.is_none()
                || matches!(code, 1 | 2) && self.fragment_opcode.is_some()
            {
                return Err(Error::Protocol("fragment order"));
            }
            let end = header + len;
            if self.buffer.len() < end {
                return Ok(PollMessage::NeedInput);
            }
            let payload = self.buffer[header..end].to_vec();
            self.buffer.drain(..end);
            let opcode = match code {
                0 => self.fragment_opcode.unwrap(),
                1 => Opcode::Text,
                2 => Opcode::Binary,
                8 => Opcode::Close,
                9 => Opcode::Ping,
                10 => Opcode::Pong,
                _ => unreachable!(),
            };
            if control {
                valid_payload(opcode, &payload)?;
                if opcode == Opcode::Close {
                    self.closed = true
                }
                return Ok(PollMessage::Message(Message { opcode, payload }));
            }
            if code != 0 && !fin {
                self.fragment_opcode = Some(opcode)
            }
            if code == 0 || !fin {
                self.fragments.extend_from_slice(&payload);
                if !fin {
                    continue;
                }
                let payload = std::mem::take(&mut self.fragments);
                self.fragment_opcode = None;
                valid_payload(opcode, &payload)?;
                return Ok(PollMessage::Message(Message { opcode, payload }));
            }
            valid_payload(opcode, &payload)?;
            return Ok(PollMessage::Message(Message { opcode, payload }));
        }
        Ok(if self.buffer.is_empty() {
            PollMessage::NeedInput
        } else {
            PollMessage::Yielded
        })
    }
}
