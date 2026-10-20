use crate::*;
const HARD_LIMIT: usize = 64 * 1024 * 1024;
#[derive(Debug, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}
impl Response {
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_slice())
    }
}
fn token(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}
pub fn encode_request(
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    max: usize,
) -> Result<Vec<u8>> {
    if method.is_empty()
        || !method.bytes().all(token)
        || !path.starts_with('/')
        || !path.bytes().all(|b| (0x21..=0x7e).contains(&b))
    {
        return Err(Error::Protocol("request line"));
    }
    if max == 0 || max > HARD_LIMIT || headers.len() > 256 {
        return Err(Error::Bounds);
    }
    let mut size = method
        .len()
        .checked_add(path.len())
        .and_then(|n| n.checked_add(12))
        .ok_or(Error::Bounds)?;
    for &(name, value) in headers {
        if name.is_empty()
            || !name.bytes().all(token)
            || !value
                .bytes()
                .all(|b| b == b'\t' || (0x20..=0x7e).contains(&b))
            || name.eq_ignore_ascii_case("Content-Length")
            || name.eq_ignore_ascii_case("Transfer-Encoding")
        {
            return Err(Error::Protocol("request header"));
        }
        size = size
            .checked_add(name.len() + value.len() + 4)
            .ok_or(Error::Bounds)?;
    }
    let length = body.len().to_string();
    let exact = size
        .checked_add(body.len())
        .and_then(|n| n.checked_add(20 + length.len()))
        .ok_or(Error::Bounds)?;
    if exact > max {
        return Err(Error::Bounds);
    }
    let mut out = Vec::with_capacity(exact);
    out.extend_from_slice(method.as_bytes());
    out.push(b' ');
    out.extend_from_slice(path.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\n");
    for &(name, value) in headers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"Content-Length: ");
    out.extend_from_slice(length.as_bytes());
    out.extend_from_slice(b"\r\n\r\n");
    out.extend_from_slice(body);
    Ok(out)
}
pub struct ResponseDecoder {
    buffer: Vec<u8>,
    max: usize,
    max_headers: usize,
    eof: bool,
    failed: bool,
}
impl ResponseDecoder {
    pub fn new(max: usize, max_headers: usize) -> Result<Self> {
        if max == 0 || max > HARD_LIMIT || max_headers == 0 || max_headers > 256 {
            return Err(Error::Bounds);
        }
        Ok(Self {
            buffer: Vec::new(),
            max,
            max_headers,
            eof: false,
            failed: false,
        })
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<()> {
        if self.failed || self.eof {
            return Err(Error::Closed);
        }
        if bytes.len() > self.max - self.buffer.len() {
            self.failed = true;
            return Err(Error::Bounds);
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }
    pub fn finish(&mut self) {
        self.eof = true
    }
    pub fn take_buffered(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }
    pub fn next_response(&mut self) -> Result<Option<Response>> {
        self.next(false)
    }
    pub fn next_head_response(&mut self) -> Result<Option<Response>> {
        self.next(true)
    }
    fn next(&mut self, head: bool) -> Result<Option<Response>> {
        if self.failed {
            return Err(Error::Closed);
        }
        let r = self.parse(head);
        if r.is_err() {
            self.failed = true;
        }
        r
    }
    fn incomplete(&self) -> Result<Option<Response>> {
        if self.eof && !self.buffer.is_empty() {
            Err(Error::Protocol("truncated HTTP response"))
        } else {
            Ok(None)
        }
    }
    fn parse(&mut self, head: bool) -> Result<Option<Response>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let mut scratch = vec![httparse::EMPTY_HEADER; self.max_headers];
        let mut parsed = httparse::Response::new(&mut scratch);
        let start = match parsed
            .parse(&self.buffer)
            .map_err(|_| Error::Protocol("HTTP header"))?
        {
            httparse::Status::Partial => return self.incomplete(),
            httparse::Status::Complete(n) => n,
        };
        if parsed.version != Some(1) {
            return Err(Error::Protocol("HTTP version"));
        }
        let status = parsed.code.ok_or(Error::Protocol("status"))?;
        let headers: Vec<_> = parsed
            .headers
            .iter()
            .map(|h| (h.name.to_owned(), h.value.to_vec()))
            .collect();
        let lengths: Vec<_> = headers
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case("content-length"))
            .collect();
        let transfers: Vec<_> = headers
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case("transfer-encoding"))
            .collect();
        if lengths.len() > 1
            || transfers.len() > 1
            || (!lengths.is_empty() && !transfers.is_empty())
        {
            return Err(Error::Protocol("ambiguous message length"));
        }
        let length = if let Some((_, v)) = lengths.first() {
            Some(decimal(v)?)
        } else {
            None
        };
        if !(head || (100..200).contains(&status) || status == 204 || status == 304)
            && length.is_some_and(|n| n > self.max - start)
        {
            return Err(Error::Bounds);
        }
        let chunked = if let Some((_, v)) = transfers.first() {
            if !v.eq_ignore_ascii_case(b"chunked") {
                return Err(Error::Protocol("unsupported transfer coding"));
            }
            true
        } else {
            false
        };
        let (end, body) = if head || (100..200).contains(&status) || status == 204 || status == 304
        {
            (start, Vec::new())
        } else if chunked {
            match chunks(&self.buffer[start..], self.max_headers, self.max - start)? {
                Some((n, b)) => (start + n, b),
                None => return self.incomplete(),
            }
        } else if let Some(n) = length {
            let end = start.checked_add(n).ok_or(Error::Bounds)?;
            if self.buffer.len() < end {
                return self.incomplete();
            }
            (end, self.buffer[start..end].to_vec())
        } else if self.eof {
            (self.buffer.len(), self.buffer[start..].to_vec())
        } else {
            return Ok(None);
        };
        self.buffer.drain(..end);
        Ok(Some(Response {
            status,
            headers,
            body,
        }))
    }
}
fn decimal(bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(Error::Protocol("decimal length"));
    }
    bytes.iter().try_fold(0usize, |n, b| {
        n.checked_mul(10)
            .and_then(|n| n.checked_add(usize::from(b - b'0')))
            .ok_or(Error::Bounds)
    })
}
fn crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|v| v == b"\r\n")
}
fn chunks(bytes: &[u8], max_headers: usize, max_bytes: usize) -> Result<Option<(usize, Vec<u8>)>> {
    let mut p = 0;
    let mut out = Vec::new();
    let mut count_chunks = 0;
    loop {
        count_chunks += 1;
        if count_chunks > 1024 {
            return Err(Error::Bounds);
        }
        let Some(line) = crlf(&bytes[p..]) else {
            return Ok(None);
        };
        let raw = &bytes[p..p + line];
        let size = raw.split(|b| *b == b';').next().unwrap();
        if size.is_empty()
            || size.len() > 16
            || !size.iter().all(u8::is_ascii_hexdigit)
            || raw.iter().any(|b| *b < 0x20 || *b > 0x7e)
        {
            return Err(Error::Protocol("chunk length"));
        }
        let size = usize::from_str_radix(std::str::from_utf8(size).unwrap(), 16)
            .map_err(|_| Error::Bounds)?;
        p += line + 2;
        if size > max_bytes.saturating_sub(out.len()) {
            return Err(Error::Bounds);
        }
        if size == 0 {
            let mut count = 0;
            loop {
                let Some(line) = crlf(&bytes[p..]) else {
                    return Ok(None);
                };
                if line == 0 {
                    return Ok(Some((p + 2, out)));
                }
                count += 1;
                if count > max_headers {
                    return Err(Error::Bounds);
                }
                let raw = &bytes[p..p + line];
                let Some(colon) = raw.iter().position(|b| *b == b':') else {
                    return Err(Error::Protocol("trailer"));
                };
                let name = &raw[..colon];
                if name.is_empty()
                    || !name.iter().all(|b| token(*b))
                    || name.eq_ignore_ascii_case(b"content-length")
                    || name.eq_ignore_ascii_case(b"transfer-encoding")
                    || raw[colon + 1..].iter().any(|b| *b < 0x20 && *b != b'\t')
                {
                    return Err(Error::Protocol("trailer"));
                }
                p += line + 2;
            }
        }
        let end = p
            .checked_add(size)
            .and_then(|n| n.checked_add(2))
            .ok_or(Error::Bounds)?;
        if end > bytes.len() {
            return Ok(None);
        }
        if &bytes[end - 2..end] != b"\r\n" {
            return Err(Error::Protocol("chunk terminator"));
        }
        out.extend_from_slice(&bytes[p..end - 2]);
        p = end;
    }
}
