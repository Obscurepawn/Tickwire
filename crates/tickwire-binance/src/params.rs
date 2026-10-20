use crate::{
    Error, Result,
    buffer::{Sink, atomic, json_string, percent},
};

pub const MAX_VALUE_BYTES: usize = 4096;

#[derive(Clone, Copy)]
pub enum Value<'a> {
    String(&'a str),
    /// An exact JSON number, used for numeric recvWindow rather than prices or quantities.
    Number(&'a str),
    Integer(u64),
    Boolean(bool),
}
#[derive(Clone, Copy)]
struct Param<'a> {
    name: &'a str,
    value: Value<'a>,
}
pub struct Params<'a, const N: usize> {
    entries: [Option<Param<'a>>; N],
    len: usize,
}
impl<'a, const N: usize> Default for Params<'a, N> {
    fn default() -> Self {
        Self::new()
    }
}
impl<'a, const N: usize> Params<'a, N> {
    pub fn new() -> Self {
        Self {
            entries: [None; N],
            len: 0,
        }
    }
    pub fn push(&mut self, name: &'a str, value: Value<'a>) -> Result<()> {
        if name.is_empty()
            || name.len() > 64
            || name == "signature"
            || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            || !name.as_bytes()[0].is_ascii_alphabetic()
            || matches!(value, Value::String(v) | Value::Number(v) if v.len() > MAX_VALUE_BYTES)
            || matches!(value, Value::Number(v) if !valid_number(v))
        {
            return Err(Error::InvalidParameter);
        }
        if self.entries[..self.len]
            .iter()
            .flatten()
            .any(|p| p.name == name)
        {
            return Err(Error::DuplicateParameter);
        }
        if self.len == N {
            return Err(Error::Capacity);
        }
        self.entries[self.len] = Some(Param { name, value });
        self.len += 1;
        Ok(())
    }
    pub fn rest(&self, out: &mut [u8]) -> Result<usize> {
        atomic(out, |s| {
            for (i, p) in self.entries[..self.len].iter().flatten().enumerate() {
                if i != 0 {
                    s.byte(b'&')?;
                }
                s.bytes(p.name.as_bytes())?;
                s.byte(b'=')?;
                match p.value {
                    Value::String(v) => percent(s, v.as_bytes())?,
                    _ => raw_value(s, p.value)?,
                }
            }
            Ok(())
        })
    }
    pub fn websocket_preimage(&self, out: &mut [u8]) -> Result<usize> {
        let mut sorted = self.entries;
        sorted[..self.len].sort_unstable_by_key(|p| p.map(|v| v.name));
        atomic(out, |s| {
            for (i, p) in sorted[..self.len].iter().flatten().enumerate() {
                if i != 0 {
                    s.byte(b'&')?;
                }
                s.bytes(p.name.as_bytes())?;
                s.byte(b'=')?;
                raw_value(s, p.value)?;
            }
            Ok(())
        })
    }
    pub fn json(&self, out: &mut [u8]) -> Result<usize> {
        atomic(out, |s| {
            s.byte(b'{')?;
            for (i, p) in self.entries[..self.len].iter().flatten().enumerate() {
                if i != 0 {
                    s.byte(b',')?;
                }
                json_string(s, p.name)?;
                s.byte(b':')?;
                match p.value {
                    Value::String(v) => json_string(s, v)?,
                    _ => raw_value(s, p.value)?,
                }
            }
            s.byte(b'}')
        })
    }
}
fn raw_value(s: &mut Sink<'_>, value: Value<'_>) -> Result<()> {
    match value {
        Value::String(v) | Value::Number(v) => s.bytes(v.as_bytes()),
        Value::Integer(v) => s.integer(v as u128),
        Value::Boolean(v) => s.bytes(if v { b"true" } else { b"false" }),
    }
}
pub fn rest_preimage(query: &[u8], body: &[u8], out: &mut [u8]) -> Result<usize> {
    atomic(out, |s| {
        s.bytes(query)?;
        s.bytes(body)
    })
}

fn valid_number(v: &str) -> bool {
    let unsigned = v.strip_prefix('-').unwrap_or(v);
    crate::decimal::Decimal::parse(v).is_ok()
        && !(unsigned.starts_with('0')
            && unsigned.as_bytes().get(1).is_some_and(u8::is_ascii_digit))
}
