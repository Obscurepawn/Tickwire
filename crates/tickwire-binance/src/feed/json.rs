//! Allocation-free JSON depth grammar. Unknown values are validated and skipped.
use super::*;

const MAX_NESTING: usize = 128;

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    fn whitespace(&mut self) {
        while self
            .bytes
            .get(self.at)
            .is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.at += 1;
        }
    }
    fn peek(&mut self) -> Option<u8> {
        self.whitespace();
        self.bytes.get(self.at).copied()
    }
    fn take(&mut self, byte: u8) -> Result<()> {
        if self.peek() != Some(byte) {
            return Err(Error::InvalidResponse);
        }
        self.at += 1;
        Ok(())
    }
    fn end(&mut self) -> Result<()> {
        if self.peek().is_some() {
            Err(Error::InvalidResponse)
        } else {
            Ok(())
        }
    }
    fn hex(&mut self) -> Result<u16> {
        let mut out = 0;
        for _ in 0..4 {
            let byte = *self.bytes.get(self.at).ok_or(Error::InvalidResponse)?;
            self.at += 1;
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(Error::InvalidResponse),
            };
            out = out * 16 + u16::from(digit);
        }
        Ok(out)
    }
    fn string(&mut self, mut emit: impl FnMut(u8) -> Result<()>) -> Result<()> {
        self.take(b'"')?;
        loop {
            let byte = *self.bytes.get(self.at).ok_or(Error::InvalidResponse)?;
            self.at += 1;
            match byte {
                b'"' => return Ok(()),
                0..=31 => return Err(Error::InvalidResponse),
                b'\\' => {
                    let escaped = *self.bytes.get(self.at).ok_or(Error::InvalidResponse)?;
                    self.at += 1;
                    match escaped {
                        b'"' | b'\\' | b'/' => emit(escaped)?,
                        b'b' => emit(8)?,
                        b'f' => emit(12)?,
                        b'n' => emit(b'\n')?,
                        b'r' => emit(b'\r')?,
                        b't' => emit(b'\t')?,
                        b'u' => {
                            let first = self.hex()?;
                            let scalar = if (0xD800..=0xDBFF).contains(&first) {
                                if self.bytes.get(self.at..self.at + 2) != Some(b"\\u") {
                                    return Err(Error::InvalidResponse);
                                }
                                self.at += 2;
                                let second = self.hex()?;
                                if !(0xDC00..=0xDFFF).contains(&second) {
                                    return Err(Error::InvalidResponse);
                                }
                                0x10000
                                    + ((u32::from(first) - 0xD800) << 10)
                                    + (u32::from(second) - 0xDC00)
                            } else {
                                u32::from(first)
                            };
                            let value = char::from_u32(scalar).ok_or(Error::InvalidResponse)?;
                            let mut encoded = [0; 4];
                            for byte in value.encode_utf8(&mut encoded).bytes() {
                                emit(byte)?;
                            }
                        }
                        _ => return Err(Error::InvalidResponse),
                    }
                }
                _ => emit(byte)?,
            }
        }
    }
    fn number(&mut self) -> Result<()> {
        if self.peek() == Some(b'-') {
            self.at += 1;
        }
        match self.bytes.get(self.at) {
            Some(b'0') => self.at += 1,
            Some(b'1'..=b'9') => {
                self.at += 1;
                while self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
                    self.at += 1;
                }
            }
            _ => return Err(Error::InvalidResponse),
        }
        if self.bytes.get(self.at) == Some(&b'.') {
            self.at += 1;
            self.digits()?;
        }
        if matches!(self.bytes.get(self.at), Some(b'e' | b'E')) {
            self.at += 1;
            if matches!(self.bytes.get(self.at), Some(b'+' | b'-')) {
                self.at += 1;
            }
            self.digits()?;
        }
        Ok(())
    }
    fn digits(&mut self) -> Result<()> {
        let first = self.at;
        while self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
            self.at += 1;
        }
        if first == self.at {
            Err(Error::InvalidResponse)
        } else {
            Ok(())
        }
    }
    fn value(&mut self, depth: usize) -> Result<()> {
        match self.peek().ok_or(Error::InvalidResponse)? {
            b'"' => self.string(|_| Ok(())),
            b'{' | b'[' => {
                if depth == MAX_NESTING {
                    return Err(Error::Capacity);
                }
                let object = self.bytes[self.at] == b'{';
                self.at += 1;
                let close = if object { b'}' } else { b']' };
                if self.peek() == Some(close) {
                    self.at += 1;
                    return Ok(());
                }
                loop {
                    if object {
                        self.string(|_| Ok(()))?;
                        self.take(b':')?;
                    }
                    self.value(depth + 1)?;
                    if self.peek() == Some(close) {
                        self.at += 1;
                        return Ok(());
                    }
                    self.take(b',')?;
                }
            }
            b'n' | b't' | b'f' => {
                let literal: &[u8] = match self.bytes[self.at] {
                    b'n' => b"null",
                    b't' => b"true",
                    _ => b"false",
                };
                if self.bytes.get(self.at..self.at + literal.len()) != Some(literal) {
                    return Err(Error::InvalidResponse);
                }
                self.at += literal.len();
                Ok(())
            }
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err(Error::InvalidResponse),
        }
    }
}

fn fields<'a, const N: usize>(bytes: &'a [u8], names: [&str; N]) -> Result<[Option<&'a [u8]>; N]> {
    let mut c = Cursor::new(bytes);
    c.take(b'{')?;
    let mut out = [None; N];
    if c.peek() == Some(b'}') {
        c.at += 1;
        c.end()?;
        return Ok(out);
    }
    loop {
        let mut key = [0; 8];
        let mut len = 0usize;
        c.string(|byte| {
            if len < key.len() {
                key[len] = byte;
            }
            len += 1;
            Ok(())
        })?;
        let index = names
            .iter()
            .position(|name| name.len() == len && name.as_bytes() == &key[..len.min(key.len())]);
        c.take(b':')?;
        c.whitespace();
        let start = c.at;
        c.value(1)?;
        if let Some(i) = index {
            if out[i].is_some() {
                return Err(Error::InvalidResponse);
            }
            out[i] = Some(&bytes[start..c.at]);
        }
        if c.peek() == Some(b'}') {
            c.at += 1;
            c.end()?;
            return Ok(out);
        }
        c.take(b',')?;
    }
}
fn string_eq(bytes: &[u8], expected: &str) -> Result<bool> {
    let mut c = Cursor::new(bytes);
    let mut at = 0;
    let mut equal = true;
    c.string(|byte| {
        equal &= expected.as_bytes().get(at) == Some(&byte);
        at += 1;
        Ok(())
    })?;
    c.end()?;
    Ok(equal && at == expected.len())
}
fn unsigned(raw: &[u8]) -> Result<u64> {
    if raw.is_empty() || !raw.iter().all(u8::is_ascii_digit) {
        return Err(Error::InvalidResponse);
    }
    std::str::from_utf8(raw)
        .map_err(|_| Error::InvalidResponse)?
        .parse()
        .map_err(|_| Error::InvalidResponse)
}
fn optional(raw: Option<&[u8]>) -> Result<Option<u64>> {
    raw.filter(|raw| *raw != b"null").map(unsigned).transpose()
}
fn required(raw: Option<&[u8]>) -> Result<&[u8]> {
    raw.ok_or(Error::InvalidResponse)
}

fn decimal(c: &mut Cursor<'_>) -> Result<Decimal> {
    let mut bytes = [0; 80];
    let mut len = 0;
    c.string(|byte| {
        if len == bytes.len() {
            return Err(Error::InvalidResponse);
        }
        bytes[len] = byte;
        len += 1;
        Ok(())
    })?;
    Decimal::parse(std::str::from_utf8(&bytes[..len]).map_err(|_| Error::InvalidResponse)?)
        .map_err(|_| Error::InvalidResponse)
}
fn levels(
    raw: &[u8],
    context: &DepthContext,
    mut emit: impl FnMut(Level) -> Result<()>,
) -> Result<()> {
    let mut c = Cursor::new(raw);
    c.take(b'[')?;
    if c.peek() == Some(b']') {
        c.at += 1;
        return c.end();
    }
    loop {
        c.take(b'[')?;
        let price = decimal(&mut c)?;
        c.take(b',')?;
        let quantity = decimal(&mut c)?;
        c.take(b']')?;
        emit(normalize_level(price, quantity, context)?)?;
        if c.peek() == Some(b']') {
            c.at += 1;
            return c.end();
        }
        c.take(b',')?;
    }
}
pub(super) fn decode(
    bytes: &[u8],
    context: &DepthContext,
    mut emit: impl FnMut(Side, Level) -> Result<()>,
) -> Result<DepthFields> {
    std::str::from_utf8(bytes).map_err(|_| Error::InvalidResponse)?;
    let [stream, data] = fields(bytes, ["stream", "data"])?;
    let payload = match (
        stream.filter(|v| *v != b"null"),
        data.filter(|v| *v != b"null"),
    ) {
        (None, None) => bytes,
        (Some(stream), Some(data)) => {
            // Compare incrementally without materializing the escaped stream name.
            let mut name = [0; 128];
            let mut len = 0;
            let mut c = Cursor::new(stream);
            c.string(|byte| {
                if len == name.len() {
                    return Err(Error::IdentityMismatch);
                }
                name[len] = byte;
                len += 1;
                Ok(())
            })?;
            c.end()?;
            if !context.stream_matches(
                std::str::from_utf8(&name[..len]).map_err(|_| Error::IdentityMismatch)?,
            ) {
                return Err(Error::IdentityMismatch);
            }
            data
        }
        _ => return Err(Error::IdentityMismatch),
    };
    let [
        event,
        symbol,
        time,
        transaction,
        first,
        last,
        previous,
        product,
        bids,
        asks,
    ] = fields(
        payload,
        ["e", "s", "E", "T", "U", "u", "pu", "st", "b", "a"],
    )?;
    let first = unsigned(required(first)?)?;
    let last = unsigned(required(last)?)?;
    let previous = optional(previous)?;
    let product = optional(product)?;
    let event_time = unsigned(required(time)?)?;
    let transaction = optional(transaction)?;
    if !string_eq(required(event)?, "depthUpdate")?
        || !string_eq(required(symbol)?, context.instrument.symbol.as_str())?
        || (context.market == Market::Spot && (previous.is_some() || product.is_some()))
        || (context.market == Market::Usdm && product.is_some_and(|p| p != 1))
    {
        return Err(Error::IdentityMismatch);
    }
    if first > last
        || (context.market == Market::Usdm && (previous.is_none() || transaction.is_none()))
    {
        return Err(Error::InvalidResponse);
    }
    levels(required(bids)?, context, |level| emit(Side::Bid, level))?;
    levels(required(asks)?, context, |level| emit(Side::Ask, level))?;
    Ok(DepthFields {
        first,
        last,
        previous,
        event_time_ns: Some(context.time_unit.nanoseconds(event_time)?),
        transaction_time_ns: transaction
            .map(|v| context.time_unit.nanoseconds(v))
            .transpose()?,
    })
}
