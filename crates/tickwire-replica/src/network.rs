//! Bounded mutual TLS request worker with pinned certificates and signed frames.
use crate::{
    cbor::{Reader, Writer},
    *,
};
use ring::rand::{SecureRandom, SystemRandom};
use std::{
    collections::{BTreeMap, VecDeque},
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    time::Duration,
};
use tickwire_journal::store::Store;
use tickwire_recovery::digest;
#[derive(Clone, Copy, Debug)]
pub struct NetworkLimits {
    pub timeout: Duration,
    pub max_requests_per_poll: usize,
    pub max_bytes_per_poll: usize,
}
impl NetworkLimits {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.timeout.is_zero()
            || self.timeout > Duration::from_secs(5)
            || self.max_requests_per_poll < 2
            || self.max_requests_per_poll > 128
            || self.max_bytes_per_poll < 1024
            || self.max_bytes_per_poll > 8 * 1024 * 1024
        {
            return Err(Error::Bounds);
        }
        Ok(self)
    }
}
fn send(stream: &mut impl Write, bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(Error::Bounds);
    }
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()?;
    Ok(())
}
fn receive(stream: &mut impl Read, max: usize) -> Result<Vec<u8>> {
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > MAX_FRAME_BYTES.min(max) {
        return Err(Error::Bounds);
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}
pub struct PeerClient {
    address: SocketAddr,
    stream_digest: Hash,
    signer: Signer,
    receiver_key: Hash,
    limits: NetworkLimits,
    tls: ClientTls,
    hello: Option<PeerHello>,
}
impl PeerClient {
    pub fn new(
        address: SocketAddr,
        stream: &Stream,
        source_signer: Signer,
        receiver_key: Hash,
        tls: ClientTls,
        limits: NetworkLimits,
    ) -> Result<Self> {
        if receiver_key == [0; 32] {
            return Err(Error::Signature);
        }
        Ok(Self {
            address,
            stream_digest: stream.digest(),
            signer: source_signer,
            receiver_key,
            limits: limits.validate()?,
            tls,
            hello: None,
        })
    }
    fn exchange(&mut self, body: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0; 32];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| Error::Signature)?;
        let mut w = Writer::new();
        w.array(5);
        w.bytes(b"tickwire.replica.request");
        w.uint(1);
        w.bytes(&self.stream_digest);
        w.bytes(&nonce);
        w.bytes(body);
        let request = self.signer.sign(w.0);
        let request_bytes = request.bytes();
        let request_hash = digest(&request_bytes);
        let socket = TcpStream::connect_timeout(&self.address, self.limits.timeout)?;
        let mut socket = self.tls.connect(socket, self.limits.timeout)?;
        send(&mut socket, &request_bytes)?;
        let signed = Signed::decode(&receive(&mut socket, MAX_FRAME_BYTES)?)?;
        signed.verify(self.receiver_key)?;
        let mut r = Reader::new(signed.payload());
        r.array(5)?;
        crate::protocol::tag(&mut r, b"tickwire.replica.response")?;
        if r.fixed::<32>()? != request_hash || r.fixed::<32>()? != nonce {
            return Err(Error::Conflict);
        }
        let body = r.bytes(MAX_FRAME_BYTES - 512)?.to_vec();
        r.end()?;
        let mut response = Reader::new(&body);
        response.array(3)?;
        let code = response.uint()?;
        let next = response.uint()?;
        let out = response.bytes(MAX_FRAME_BYTES - 1024)?.to_vec();
        response.end()?;
        match code {
            0 => Ok(out),
            2 => Err(Error::Missing),
            3 => Err(Error::Gap { next }),
            4 => Err(Error::Frozen),
            5 => Err(Error::Conflict),
            _ => Err(Error::Invalid("remote request rejected")),
        }
    }
    pub fn handshake<S: BlobStore>(&mut self, source: &mut SourceWorker<S>) -> Result<PeerHello> {
        let mut w = Writer::new();
        w.array(1);
        w.uint(1);
        let hello = PeerHello::decode(&self.exchange(&w.0)?)?;
        source.bind_peer(&hello, self.receiver_key)?;
        self.hello = Some(hello.clone());
        Ok(hello)
    }
    fn has_object(&mut self, o: &ObjectRef) -> Result<bool> {
        let mut w = Writer::new();
        w.array(2);
        w.uint(2);
        o.write(&mut w);
        let bytes = self.exchange(&w.0)?;
        match bytes.as_slice() {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(Error::Invalid("object response")),
        }
    }
    fn chunk(&mut self, o: &ObjectRef, offset: u64, bytes: &[u8]) -> Result<()> {
        let mut w = Writer::new();
        w.array(5);
        w.uint(3);
        o.write(&mut w);
        w.uint(offset);
        w.bytes(bytes);
        w.uint(tickwire_journal::frame::crc32c(bytes) as u64);
        self.exchange(&w.0)?;
        Ok(())
    }
    fn manifest(&mut self, bytes: &[u8]) -> Result<()> {
        let mut w = Writer::new();
        w.array(2);
        w.uint(4);
        w.bytes(bytes);
        self.exchange(&w.0)?;
        Ok(())
    }
    fn append(&mut self, b: &AppendBatch) -> Result<Signed> {
        let mut w = Writer::new();
        w.array(2);
        w.uint(5);
        w.bytes(&b.bytes());
        Signed::decode(&self.exchange(&w.0)?)
    }
    fn ack(&mut self, seq: u64) -> Result<Signed> {
        let mut w = Writer::new();
        w.array(2);
        w.uint(6);
        w.uint(seq);
        Signed::decode(&self.exchange(&w.0)?)
    }
}
pub struct MirrorListener<T: Store, S: BlobStore> {
    listener: TcpListener,
    mirror: Mirror<T, S>,
    signer: Signer,
    source_key: Hash,
    tls: ServerTls,
    limits: NetworkLimits,
}
impl<T: Store, S: BlobStore> MirrorListener<T, S> {
    pub fn bind(
        address: SocketAddr,
        mirror: Mirror<T, S>,
        receiver_signer: Signer,
        source_key: Hash,
        tls: ServerTls,
        limits: NetworkLimits,
    ) -> Result<Self> {
        if source_key == [0; 32] {
            return Err(Error::Signature);
        }
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            mirror,
            signer: receiver_signer,
            source_key,
            tls,
            limits: limits.validate()?,
        })
    }
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }
    pub fn mirror(&self) -> &Mirror<T, S> {
        &self.mirror
    }
    pub fn mirror_mut(&mut self) -> &mut Mirror<T, S> {
        &mut self.mirror
    }
    /// Each accepted request has a hard frame bound and read/write timeout.
    pub fn poll(&mut self) -> Result<usize> {
        let mut requests = 0;
        let mut bytes = 0;
        while requests < self.limits.max_requests_per_poll && bytes < self.limits.max_bytes_per_poll
        {
            let (socket, _) = match self.listener.accept() {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            };
            socket.set_nonblocking(false)?;
            let mut socket = match self.tls.accept(socket, self.limits.timeout) {
                Ok(s) => s,
                Err(_) => {
                    requests += 1;
                    continue;
                }
            };
            // An unauthenticated/broken connection cannot advance replica state.
            let raw = match receive(&mut socket, MAX_FRAME_BYTES) {
                Ok(raw) => raw,
                Err(_) => {
                    requests += 1;
                    continue;
                }
            };
            bytes += raw.len();
            requests += 1;
            let signed = match Signed::decode(&raw) {
                Ok(s) => s,
                Err(_) => continue,
            };
            if signed.verify(self.source_key).is_err() {
                continue;
            }
            let parsed = (|| -> Result<([u8; 32], Vec<u8>)> {
                let mut r = Reader::new(signed.payload());
                r.array(5)?;
                crate::protocol::tag(&mut r, b"tickwire.replica.request")?;
                if r.fixed::<32>()? != self.mirror.stream().digest() {
                    return Err(Error::Conflict);
                }
                let nonce = r.fixed()?;
                let body = r.bytes(MAX_FRAME_BYTES - 512)?.to_vec();
                r.end()?;
                Ok((nonce, body))
            })();
            let Ok((nonce, body)) = parsed else { continue };
            let result = self.dispatch(&body);
            let mut response = Writer::new();
            response.array(3);
            match result {
                Ok(b) => {
                    response.uint(0);
                    response.uint(0);
                    response.bytes(&b)
                }
                Err(e) => {
                    let (code, next) = match e {
                        Error::Missing | Error::Recovery(tickwire_recovery::Error::Missing) => {
                            (2, 0)
                        }
                        Error::Gap { next } => (3, next),
                        Error::Frozen => (4, 0),
                        Error::Conflict => (5, 0),
                        _ => (1, 0),
                    };
                    response.uint(code);
                    response.uint(next);
                    response.bytes(&[]);
                }
            }
            let mut w = Writer::new();
            w.array(5);
            w.bytes(b"tickwire.replica.response");
            w.uint(1);
            w.bytes(&digest(&raw));
            w.bytes(&nonce);
            w.bytes(&response.0);
            let out = self.signer.sign(w.0);
            let _ = send(&mut socket, &out.bytes());
        }
        Ok(requests)
    }
    fn dispatch(&mut self, body: &[u8]) -> Result<Vec<u8>> {
        let mut r = Reader::new(body);
        let n = r.count(5)?;
        let op = r.uint()?;
        let result = match (op, n) {
            (1, 1) => {
                r.end()?;
                self.mirror.hello().bytes()
            }
            (2, 2) => {
                let o = ObjectRef::read(&mut r)?;
                r.end()?;
                vec![u8::from(self.mirror.has_object(&o)?)]
            }
            (3, 5) => {
                let o = ObjectRef::read(&mut r)?;
                let offset = r.uint()?;
                let bytes = r.bytes(CHUNK_BYTES)?;
                let crc = u32::try_from(r.uint()?).map_err(|_| Error::Bounds)?;
                r.end()?;
                vec![u8::from(self.mirror.object_chunk(o, offset, bytes, crc)?)]
            }
            (4, 2) => {
                let bytes = r.bytes(tickwire_recovery::MAX_MANIFEST_BYTES)?;
                r.end()?;
                self.mirror.publish_manifest(bytes)?.to_vec()
            }
            (5, 2) => {
                let batch = AppendBatch::decode(r.bytes(MAX_BATCH_BYTES)?)?;
                r.end()?;
                self.mirror.append(&batch, &self.signer)?.bytes()
            }
            (6, 2) => {
                let sequence = r.uint()?;
                r.end()?;
                self.mirror.ack(sequence, &self.signer)?.bytes()
            }
            _ => return Err(Error::Invalid("request operation")),
        };
        r.end()?;
        Ok(result)
    }
}
struct Copying {
    batch: AppendBatch,
    objects: VecDeque<ObjectRef>,
    offset: Option<u64>,
    manifests: VecDeque<Vec<u8>>,
}
pub struct SourceSender {
    client: PeerClient,
    copying: Option<Copying>,
    remote_sequence: u64,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkerProgress {
    pub requests: usize,
    pub bytes: usize,
    pub durable_sequence: u64,
}
impl SourceSender {
    pub fn new(client: PeerClient) -> Self {
        Self {
            client,
            copying: None,
            remote_sequence: 0,
        }
    }
    pub fn disconnect(&mut self) {
        self.client.hello = None;
        self.copying = None;
    }
    /// Management-thread transfer of the final closed input suffix and source seal.
    /// Normal account replication must reach the sealed position first.
    pub fn upload_final<S: BlobStore>(
        &mut self,
        source: &SourceWorker<S>,
        roots: &[ObjectRef],
        seal: &Signed,
    ) -> Result<Hash> {
        if self.remote_sequence != source.prefix().sequence {
            return Err(Error::Gap {
                next: self.remote_sequence + 1,
            });
        }
        let mut objects = BTreeMap::new();
        for root in roots.iter().chain(&source.stream().initial_objects) {
            objects.insert(root.hash, root.clone());
        }
        let entry = Entry {
            sequence: 1,
            writer_epoch: 1,
            kind: 1,
            payload: ObjectRef::new(&seal.bytes())?,
            manifest: None,
            extra_objects: roots.to_vec(),
        };
        for object in source.extra_dependencies(&entry)? {
            objects.insert(object.hash, object);
        }
        for object in objects.into_values() {
            if self.client.has_object(&object)? {
                continue;
            }
            let bytes = source.read_object(&object)?;
            for (index, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
                self.client
                    .chunk(&object, (index * CHUNK_BYTES) as u64, chunk)?;
            }
        }
        let bytes = seal.bytes();
        let object = ObjectRef::new(&bytes)?;
        if !self.client.has_object(&object)? {
            for (index, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
                self.client
                    .chunk(&object, (index * CHUNK_BYTES) as u64, chunk)?;
            }
        }
        Ok(object.hash)
    }
    pub fn reconnect<S: BlobStore>(&mut self, source: &mut SourceWorker<S>) -> Result<()> {
        let h = self.client.handshake(source)?;
        if h.prefix.sequence > source.prefix().sequence {
            return Err(Error::Frozen);
        }
        self.remote_sequence = h.prefix.sequence;
        self.copying = None;
        if h.prefix.sequence > 0 {
            source.acknowledge(self.client.ack(h.prefix.sequence)?)?;
        }
        Ok(())
    }
    /// Performs only the configured request/byte budget; no detached thread or
    /// unbounded queue. The owner chooses when this worker runs.
    pub fn poll<S: BlobStore>(&mut self, source: &mut SourceWorker<S>) -> Result<WorkerProgress> {
        let mut out = WorkerProgress::default();
        if self.client.hello.is_none() {
            self.reconnect(source)?;
            out.requests += if self.remote_sequence > 0 { 2 } else { 1 };
        }
        while out.requests < self.client.limits.max_requests_per_poll
            && out.bytes < self.client.limits.max_bytes_per_poll
        {
            if self.copying.is_none() {
                if self.remote_sequence >= source.prefix().sequence {
                    break;
                }
                let batch = source.batch_range(self.remote_sequence + 1, 1)?;
                let mut objects = BTreeMap::new();
                for o in &source.stream().initial_objects {
                    objects.insert(o.hash, o.clone());
                }
                let mut manifests = VecDeque::new();
                for e in &batch.entries {
                    objects.insert(e.payload.hash, e.payload.clone());
                    for o in &e.extra_objects {
                        objects.insert(o.hash, o.clone());
                    }
                    for o in source.extra_dependencies(e)? {
                        objects.insert(o.hash, o);
                    }
                    if let Some(m) = &e.manifest {
                        let manifest = source.manifest(m)?;
                        for o in &manifest.objects {
                            objects.insert(
                                o.content_hash,
                                ObjectRef {
                                    hash: o.content_hash,
                                    bytes: o.byte_length,
                                },
                            );
                        }
                        manifests.push_back(manifest.canonical_bytes()?);
                    }
                }
                if objects.len() > 24576 {
                    return Err(Error::Bounds);
                }
                self.copying = Some(Copying {
                    batch,
                    objects: objects.into_values().collect(),
                    offset: None,
                    manifests,
                });
            }
            let c = self.copying.as_mut().unwrap();
            if let Some(o) = c.objects.front() {
                if c.offset.is_none() {
                    let has = self.client.has_object(o)?;
                    out.requests += 1;
                    if has {
                        c.objects.pop_front();
                        continue;
                    }
                    c.offset = Some(0);
                    continue;
                }
                let offset = c.offset.unwrap();
                let bytes = source.read_object(o)?;
                let remaining = self.client.limits.max_bytes_per_poll - out.bytes;
                let n = CHUNK_BYTES
                    .min(bytes.len() - offset as usize)
                    .min(remaining);
                if n == 0 {
                    break;
                }
                self.client
                    .chunk(o, offset, &bytes[offset as usize..offset as usize + n])?;
                out.requests += 1;
                out.bytes += n;
                let end = offset + n as u64;
                if end == o.bytes {
                    c.objects.pop_front();
                    c.offset = None;
                } else {
                    c.offset = Some(end);
                }
                continue;
            }
            if let Some(m) = c.manifests.front() {
                if m.len() > self.client.limits.max_bytes_per_poll - out.bytes {
                    if out.bytes == 0 {
                        return Err(Error::Bounds);
                    }
                    break;
                }
                self.client.manifest(m)?;
                out.bytes += m.len();
                out.requests += 1;
                c.manifests.pop_front();
                continue;
            }
            let bytes = c.batch.bytes().len();
            if bytes > self.client.limits.max_bytes_per_poll - out.bytes {
                if out.bytes == 0 {
                    return Err(Error::Bounds);
                }
                break;
            }
            let ack = self.client.append(&c.batch)?;
            source.acknowledge(ack)?;
            self.remote_sequence = c.batch.end.sequence;
            self.copying = None;
            out.requests += 1;
            out.bytes += bytes;
        }
        out.durable_sequence = self.remote_sequence;
        Ok(out)
    }
}
