use crate::{Error, Result};
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};
use std::{borrow::Cow, cell::Cell, fmt};

const SAFE_INTEGER: i64 = 9_007_199_254_740_991;
const MAX_NODES: usize = 262_144;

struct Context {
    failure: Cell<Option<Error>>,
    nodes: Cell<usize>,
}
impl Context {
    fn reject<E: serde::de::Error>(&self, error: Error) -> E {
        self.failure.set(Some(error));
        E::custom("configuration value rejected")
    }
    fn node(&self) -> Result<()> {
        let n = self.nodes.get();
        if n >= MAX_NODES {
            return Err(Error::Capacity);
        }
        self.nodes.set(n + 1);
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Seed<'a> {
    depth: usize,
    context: &'a Context,
}
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Value;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> core::result::Result<Value, D::Error> {
        self.context
            .node()
            .map_err(|e| self.context.reject::<D::Error>(e))?;
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded configuration value")
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> core::result::Result<Value, E> {
        Ok(Value::Bool(value))
    }
    fn visit_unit<E: serde::de::Error>(self) -> core::result::Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> core::result::Result<Value, E> {
        if !(-SAFE_INTEGER..=SAFE_INTEGER).contains(&value) {
            return Err(self.context.reject(Error::Scalar));
        }
        Ok(value.into())
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> core::result::Result<Value, E> {
        if value > SAFE_INTEGER as u64 {
            return Err(self.context.reject(Error::Scalar));
        }
        Ok(value.into())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> core::result::Result<Value, E> {
        Err(self.context.reject(Error::Scalar))
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> core::result::Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> core::result::Result<Value, E> {
        Ok(Value::String(value))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut input: A) -> core::result::Result<Value, A::Error> {
        if self.depth == 0 {
            return Err(self.context.reject(Error::Depth));
        }
        let child = Self {
            depth: self.depth - 1,
            ..self
        };
        let mut values = Vec::new();
        while let Some(value) = input.next_element_seed(child)? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut input: A) -> core::result::Result<Value, A::Error> {
        if self.depth == 0 {
            return Err(self.context.reject(Error::Depth));
        }
        let child = Self {
            depth: self.depth - 1,
            ..self
        };
        let mut values = Map::new();
        while let Some(key) = input.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(self.context.reject(Error::Duplicate));
            }
            values.insert(key, input.next_value_seed(child)?);
        }
        Ok(Value::Object(values))
    }
}

pub fn json(bytes: &[u8], depth: usize) -> Result<Value> {
    let context = Context {
        failure: Cell::new(None),
        nodes: Cell::new(0),
    };
    let normalized = normalize_integer_zero(bytes);
    let mut parser = serde_json::Deserializer::from_slice(&normalized);
    let value = Seed {
        depth,
        context: &context,
    }
    .deserialize(&mut parser)
    .map_err(|_| context.failure.get().unwrap_or(Error::Encoding))?;
    parser.end().map_err(|_| Error::Encoding)?;
    Ok(value)
}

fn normalize_integer_zero(bytes: &[u8]) -> Cow<'_, [u8]> {
    // serde_json dispatches integer syntax -0 to visit_f64. Preserve the
    // integer-only contract by normalizing only that exact lexical token.
    // Replacing the minus with whitespace preserves all token boundaries.
    let mut output = Cow::Borrowed(bytes);
    let mut inside_string = false;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if inside_string => {
                index += 2;
                continue;
            }
            b'"' => inside_string = !inside_string,
            b'-' if !inside_string && bytes.get(index + 1) == Some(&b'0') => {
                let boundary_before = index == 0
                    || matches!(
                        bytes[index - 1],
                        b'[' | b',' | b':' | b' ' | b'\t' | b'\r' | b'\n'
                    );
                let boundary_after = index + 2 == bytes.len()
                    || matches!(
                        bytes[index + 2],
                        b']' | b'}' | b',' | b' ' | b'\t' | b'\r' | b'\n'
                    );
                if boundary_before && boundary_after {
                    output.to_mut()[index] = b' ';
                }
            }
            _ => {}
        }
        index += 1;
    }
    output
}

pub fn toml(bytes: &[u8], depth: usize) -> Result<Value> {
    let input = std::str::from_utf8(bytes).map_err(|_| Error::Encoding)?;
    let table: toml::Table = input.parse().map_err(|_| Error::Encoding)?;
    let context = Context {
        failure: Cell::new(None),
        nodes: Cell::new(0),
    };
    from_toml(toml::Value::Table(table), depth, &context)
}

fn from_toml(input: toml::Value, depth: usize, context: &Context) -> Result<Value> {
    context.node()?;
    match input {
        toml::Value::Float(_) | toml::Value::Datetime(_) => Err(Error::Scalar),
        toml::Value::Integer(n) => {
            if !(-SAFE_INTEGER..=SAFE_INTEGER).contains(&n) {
                Err(Error::Scalar)
            } else {
                Ok(n.into())
            }
        }
        toml::Value::String(s) => Ok(s.into()),
        toml::Value::Boolean(b) => Ok(b.into()),
        toml::Value::Array(items) => {
            let child_depth = depth.checked_sub(1).ok_or(Error::Depth)?;
            items
                .into_iter()
                .map(|value| from_toml(value, child_depth, context))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        }
        toml::Value::Table(items) => {
            let child_depth = depth.checked_sub(1).ok_or(Error::Depth)?;
            items
                .into_iter()
                .map(|(key, value)| Ok((key, from_toml(value, child_depth, context)?)))
                .collect::<Result<Map<_, _>>>()
                .map(Value::Object)
        }
    }
}

pub fn canonical(value: &Value) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    append(value, &mut output)?;
    Ok(output)
}

fn append(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Object(entries) => {
            let mut keys: Vec<_> = entries.keys().collect();
            keys.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
            output.push(b'{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key).map_err(|_| Error::Encoding)?;
                output.push(b':');
                append(&entries[key], output)?;
            }
            output.push(b'}');
        }
        Value::Array(items) => {
            output.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    output.push(b',');
                }
                append(item, output)?;
            }
            output.push(b']');
        }
        _ => serde_json::to_writer(&mut *output, value).map_err(|_| Error::Encoding)?,
    }
    Ok(())
}
