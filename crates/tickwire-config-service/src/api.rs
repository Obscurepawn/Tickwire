//! Bounded control HTTP input. A parsed request carries no deployment authority.
use crate::{Hash, auth::AuthenticatedStream};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{self, Read},
    sync::OnceLock,
};
use tickwire_config::{Candidate, Format, Limits};

pub const MAX_HEADER_BYTES: usize = 16 * 1024;
pub const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Validate,
    Preview,
    Plans,
    Plan(String),
    PlanDiff(String),
    Commit,
    RollbackPlan,
    Head,
    Desired,
    Revision(u64),
    Effective(Option<String>),
    Operation(String),
    OperationByKey(String),
    Resume(String),
    Watch,
    Schema,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    Syntax,
    Route,
    Method,
    Capacity,
    Precondition,
    Schema,
    Transport,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error {
    pub status: u16,
    pub code: Code,
}
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Debug)]
pub struct Request {
    endpoint: Endpoint,
    if_match: Option<String>,
    idempotency_key: Option<String>,
    last_event_id: Option<String>,
    body: Option<Value>,
    digest: Hash,
    body_bytes: usize,
}
impl Request {
    pub fn parse(bytes: &[u8], max_body: usize) -> Result<Self> {
        limits(max_body)?;
        let end = bytes
            .windows(4)
            .position(|b| b == b"\r\n\r\n")
            .and_then(|n| n.checked_add(4))
            .ok_or(syntax())?;
        let header = parse_header(&bytes[..end], max_body)?;
        if end.checked_add(header.length) != Some(bytes.len()) {
            return Err(syntax());
        }
        let (body, canonical) = if let Some(name) = header.endpoint.body_schema() {
            let candidate = Candidate::parse(
                Format::Json,
                &bytes[end..],
                Limits {
                    max_bytes: max_body,
                    max_depth: 64,
                },
            )
            .map_err(|e| match e {
                tickwire_config::Error::Capacity => capacity(),
                _ => syntax(),
            })?;
            validate_body(name, candidate.value())?;
            (
                Some(candidate.value().clone()),
                candidate.canonical_bytes().to_vec(),
            )
        } else {
            (None, Vec::new())
        };
        let mut hash = Sha256::new();
        hash.update(b"tickwire.control.http-request.v1");
        for bytes in [
            header.method.as_bytes(),
            header.endpoint.route().as_bytes(),
            canonical.as_slice(),
        ] {
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
        Ok(Self {
            endpoint: header.endpoint,
            if_match: header.if_match,
            idempotency_key: header.idempotency_key,
            last_event_id: header.last_event_id,
            body,
            digest: hash.finalize().into(),
            body_bytes: header.length,
        })
    }
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
    pub fn if_match(&self) -> Option<&str> {
        self.if_match.as_deref()
    }
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }
    pub fn last_event_id(&self) -> Option<&str> {
        self.last_event_id.as_deref()
    }
    pub fn body(&self) -> Option<&Value> {
        self.body.as_ref()
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
    pub fn body_bytes(&self) -> usize {
        self.body_bytes
    }
}
pub fn read_request(stream: &mut AuthenticatedStream, max_body: usize) -> Result<Request> {
    limits(max_body)?;
    let mut bytes = Vec::with_capacity(1024);
    loop {
        if bytes.len() >= MAX_HEADER_BYTES {
            return Err(capacity());
        }
        let mut byte = [0];
        stream.read_exact(&mut byte).map_err(read_error)?;
        bytes.push(byte[0]);
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let header = parse_header(&bytes, max_body)?;
    let start = bytes.len();
    bytes.resize(start + header.length, 0);
    stream.read_exact(&mut bytes[start..]).map_err(read_error)?;
    Request::parse(&bytes, max_body)
}

fn read_error(error: io::Error) -> Error {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        syntax()
    } else {
        Error {
            status: 503,
            code: Code::Transport,
        }
    }
}
fn syntax() -> Error {
    Error {
        status: 400,
        code: Code::Syntax,
    }
}
fn capacity() -> Error {
    Error {
        status: 413,
        code: Code::Capacity,
    }
}
fn route_error() -> Error {
    Error {
        status: 404,
        code: Code::Route,
    }
}
fn limits(max: usize) -> Result<()> {
    if !(1..=MAX_BODY_BYTES).contains(&max) {
        return Err(capacity());
    }
    Ok(())
}
struct Header {
    method: String,
    endpoint: Endpoint,
    length: usize,
    if_match: Option<String>,
    idempotency_key: Option<String>,
    last_event_id: Option<String>,
}
fn parse_header(bytes: &[u8], max_body: usize) -> Result<Header> {
    if bytes.len() > MAX_HEADER_BYTES {
        return Err(capacity());
    }
    if bytes.first().is_none_or(u8::is_ascii_whitespace)
        || bytes.iter().enumerate().any(|(i, &b)| {
            (b == b'\n' && (i == 0 || bytes[i - 1] != b'\r'))
                || (b == b'\r' && bytes.get(i + 1) != Some(&b'\n'))
        })
    {
        return Err(syntax());
    }
    let mut storage = [httparse::EMPTY_HEADER; 64];
    let mut parsed = httparse::Request::new(&mut storage);
    let status = parsed.parse(bytes).map_err(|_| syntax())?;
    if status != httparse::Status::Complete(bytes.len()) || parsed.version != Some(1) {
        return Err(syntax());
    }
    let method = parsed.method.ok_or(syntax())?;
    if method != "GET" && method != "POST" {
        return Err(Error {
            status: 405,
            code: Code::Method,
        });
    }
    let target = parsed.path.ok_or(syntax())?;
    let endpoint = route(method, target)?;
    let mut headers = BTreeMap::new();
    for header in parsed.headers {
        let name = header.name.to_ascii_lowercase();
        let value = std::str::from_utf8(header.value)
            .map_err(|_| syntax())?
            .trim_matches([' ', '\t']);
        if value.bytes().any(|b| !(32..=126).contains(&b)) || headers.insert(name, value).is_some()
        {
            return Err(syntax());
        }
    }
    let host = headers.get("host").ok_or(syntax())?;
    if host.is_empty()
        || host.len() > 256
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-:[]".contains(&b))
        || headers.contains_key("transfer-encoding")
        || headers.contains_key("content-encoding")
    {
        return Err(syntax());
    }
    let length = match headers.get("content-length") {
        Some(value) if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => {
            value.parse::<usize>().map_err(|_| capacity())?
        }
        None if method == "GET" => 0,
        _ => return Err(syntax()),
    };
    if length > max_body {
        return Err(capacity());
    }
    if method == "GET" && length != 0 {
        return Err(syntax());
    }
    if method == "POST" {
        let content_type = headers
            .get("content-type")
            .ok_or(syntax())?
            .to_ascii_lowercase();
        let mut parts = content_type.split(';').map(str::trim);
        if parts.next() != Some("application/json")
            || parts.next().is_some_and(|p| p != "charset=utf-8")
            || parts.next().is_some()
        {
            return Err(syntax());
        }
    }
    let if_match = match (endpoint.requires_match(), headers.get("if-match")) {
        (true, Some(value)) if valid_etag(value) => Some((*value).to_owned()),
        (true, None) => {
            return Err(Error {
                status: 428,
                code: Code::Precondition,
            });
        }
        (false, None) => None,
        _ => return Err(syntax()),
    };
    let idempotency_key = match (endpoint.requires_key(), headers.get("idempotency-key")) {
        (true, Some(value)) => Some(uuid(value)?),
        (false, None) => None,
        _ => return Err(syntax()),
    };
    let last_event_id = match headers.get("last-event-id") {
        Some(value)
            if endpoint == Endpoint::Watch
                && !value.is_empty()
                && value.len() <= 128
                && value.bytes().all(|b| (33..=126).contains(&b)) =>
        {
            Some((*value).to_owned())
        }
        None => None,
        _ => return Err(syntax()),
    };
    Ok(Header {
        method: method.into(),
        endpoint,
        length,
        if_match,
        idempotency_key,
        last_event_id,
    })
}
fn revision(value: &str) -> Result<u64> {
    if value.is_empty()
        || value.starts_with('0')
        || value.len() > 19
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(syntax());
    }
    value
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0 && *n < 1 << 63)
        .ok_or(syntax())
}
fn uuid(value: &str) -> Result<String> {
    if value.len() != 36
        || value.bytes().enumerate().any(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b != b'-'
            } else {
                !b.is_ascii_hexdigit()
            }
        })
    {
        return Err(syntax());
    }
    Ok(value.to_ascii_lowercase())
}
fn valid_etag(value: &str) -> bool {
    let Some(inner) = value
        .strip_prefix("\"cfg-")
        .and_then(|s| s.strip_suffix('"'))
    else {
        return false;
    };
    let Some((rev, hash)) = inner.split_once('-') else {
        return false;
    };
    revision(rev).is_ok()
        && hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn process_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
fn route(method: &str, target: &str) -> Result<Endpoint> {
    if target.len() > 2048 || !target.is_ascii() || target.contains(['%', '#', '\\']) {
        return Err(route_error());
    }
    let (path, query) = target
        .split_once('?')
        .map_or((target, None), |(p, q)| (p, Some(q)));
    if method == "GET" {
        match (path, query) {
            ("/v1/config/head", None) => return Ok(Endpoint::Head),
            ("/v1/config/desired", None) => return Ok(Endpoint::Desired),
            ("/v1/config/schema", None) => return Ok(Endpoint::Schema),
            ("/v1/config/watch", None) => return Ok(Endpoint::Watch),
            ("/v1/config/effective", None) => return Ok(Endpoint::Effective(None)),
            ("/v1/config/effective", Some(q)) => {
                let id = q
                    .strip_prefix("process_id=")
                    .filter(|id| process_id(id))
                    .ok_or(route_error())?;
                return Ok(Endpoint::Effective(Some(id.to_owned())));
            }
            ("/v1/config/operations", Some(q)) => {
                let key = q.strip_prefix("idempotency_key=").ok_or(route_error())?;
                return Ok(Endpoint::OperationByKey(uuid(key)?));
            }
            _ => {}
        }
    }
    if query.is_some() {
        return Err(route_error());
    }
    match (method, path) {
        ("POST", "/v1/config/validate") => return Ok(Endpoint::Validate),
        ("POST", "/v1/config/preview") => return Ok(Endpoint::Preview),
        ("POST", "/v1/config/plans") => return Ok(Endpoint::Plans),
        ("POST", "/v1/config/commits") => return Ok(Endpoint::Commit),
        ("POST", "/v1/config/rollback-plans") => return Ok(Endpoint::RollbackPlan),
        _ => {}
    }
    let parts: Vec<_> = path.split('/').collect();
    match parts.as_slice() {
        ["", "v1", "config", "revisions", id] if method == "GET" => {
            Ok(Endpoint::Revision(revision(id)?))
        }
        ["", "v1", "config", "plans", id] if method == "GET" => Ok(Endpoint::Plan(uuid(id)?)),
        ["", "v1", "config", "plans", id, "diff"] if method == "GET" => {
            Ok(Endpoint::PlanDiff(uuid(id)?))
        }
        ["", "v1", "config", "operations", id] if method == "GET" => {
            Ok(Endpoint::Operation(uuid(id)?))
        }
        ["", "v1", "config", "operations", id, "resume"] if method == "POST" => {
            Ok(Endpoint::Resume(uuid(id)?))
        }
        _ => Err(route_error()),
    }
}
impl Endpoint {
    fn requires_match(&self) -> bool {
        matches!(
            self,
            Self::Preview | Self::Plans | Self::Commit | Self::RollbackPlan | Self::Resume(_)
        )
    }
    fn requires_key(&self) -> bool {
        matches!(
            self,
            Self::Plans | Self::Commit | Self::RollbackPlan | Self::Resume(_)
        )
    }
    fn body_schema(&self) -> Option<&'static str> {
        match self {
            Self::Validate => Some("ValidationRequest"),
            Self::Preview | Self::Plans => Some("PlanRequest"),
            Self::Commit => Some("CommitRequest"),
            Self::RollbackPlan => Some("RollbackPlanRequest"),
            Self::Resume(_) => Some("ResumeRequest"),
            _ => None,
        }
    }
    fn route(&self) -> String {
        let suffix = match self {
            Self::Validate => "validate".into(),
            Self::Preview => "preview".into(),
            Self::Plans => "plans".into(),
            Self::Plan(id) => format!("plans/{id}"),
            Self::PlanDiff(id) => format!("plans/{id}/diff"),
            Self::Commit => "commits".into(),
            Self::RollbackPlan => "rollback-plans".into(),
            Self::Head => "head".into(),
            Self::Desired => "desired".into(),
            Self::Revision(rev) => format!("revisions/{rev}"),
            Self::Effective(None) => "effective".into(),
            Self::Effective(Some(id)) => format!("effective?process_id={id}"),
            Self::Operation(id) => format!("operations/{id}"),
            Self::OperationByKey(id) => format!("operations?idempotency_key={id}"),
            Self::Resume(id) => format!("operations/{id}/resume"),
            Self::Watch => "watch".into(),
            Self::Schema => "schema".into(),
        };
        format!("/v1/config/{suffix}")
    }
}
fn validate_body(name: &str, value: &Value) -> Result<()> {
    static VALIDATORS: OnceLock<
        core::result::Result<BTreeMap<&'static str, jsonschema::Validator>, ()>,
    > = OnceLock::new();
    let validators = VALIDATORS
        .get_or_init(|| {
            let api: Value = serde_json::from_str(include_str!(
                "../../../specs/contracts/control-api.openapi.json"
            ))
            .map_err(|_| ())?;
            let system_schema: Value = serde_json::from_str(include_str!(
                "../../../specs/contracts/system-config.schema.json"
            ))
            .map_err(|_| ())?;
            let registry = jsonschema::Registry::new()
                .add(
                    "https://tickwire.invalid/contracts/system-config.schema.json",
                    system_schema,
                )
                .map_err(|_| ())?
                .prepare()
                .map_err(|_| ())?;
            let mut result = BTreeMap::new();
            for name in [
                "ValidationRequest",
                "PlanRequest",
                "CommitRequest",
                "RollbackPlanRequest",
                "ResumeRequest",
            ] {
                let schema = json!({
                    "$ref":format!("#/components/schemas/{name}"),
                    "components":{"schemas":api["components"]["schemas"]}
                });
                let validator = jsonschema::options()
                    .with_draft(jsonschema::Draft::Draft202012)
                    .should_validate_formats(true)
                    .with_base_uri("https://tickwire.invalid/contracts/control-api.openapi.json")
                    .with_registry(&registry)
                    .offline()
                    .build(&schema)
                    .map_err(|_| ())?;
                result.insert(name, validator);
            }
            Ok(result)
        })
        .as_ref()
        .map_err(|_| Error {
            status: 503,
            code: Code::Schema,
        })?;
    if !validators.get(name).is_some_and(|v| v.is_valid(value)) {
        return Err(Error {
            status: 400,
            code: Code::Schema,
        });
    }
    Ok(())
}
