//! Actual file-backed replica evidence used by the WebSocket output test.
use super::*;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tickwire_account_gateway::{AdmissionRequest, AuthorityError, CommitState};
use tickwire_recovery::{BlobStore, Persistence};
use tickwire_replica as replica;

pub(super) struct Authority {
    pub inner: gateway_fixture::MockAuthority,
    pub evidence: Rc<RefCell<Option<replica::CommitEvidence>>>,
    required: bool,
}
impl Authority {
    pub fn new(required: bool) -> Self {
        Self {
            inner: gateway_fixture::MockAuthority::default(),
            evidence: Rc::new(RefCell::new(None)),
            required,
        }
    }
}
impl tickwire_account_gateway::Authority for Authority {
    fn prepare(
        &mut self,
        request: AdmissionRequest<'_>,
    ) -> std::result::Result<tickwire_recovery::VerifiedClosure, AuthorityError> {
        self.inner.prepare(request)
    }
    fn verify(
        &mut self,
        request: AdmissionRequest<'_>,
    ) -> std::result::Result<tickwire_recovery::VerifiedClosure, AuthorityError> {
        self.inner.verify(request)
    }
    fn current_gate(
        &mut self,
        command: &CanonicalCommand,
        admission: &tickwire_account::Admission,
    ) -> std::result::Result<Gate, AuthorityError> {
        self.inner.current_gate(command, admission)
    }
    fn commit_state(
        &mut self,
        _: &tickwire_account::Admission,
        receipt: &tickwire_account_journal::AppendReceipt,
    ) -> std::result::Result<CommitState, AuthorityError> {
        if !self.required {
            return Ok(CommitState::Ready);
        }
        let evidence = self.evidence.borrow();
        let Some(evidence) = evidence.as_ref() else {
            return Ok(CommitState::Pending);
        };
        if evidence.local().physical() != receipt.physical_frontier()
            || evidence.local().prefix().sequence != receipt.sequence()
        {
            return Err(AuthorityError::Denied);
        }
        Ok(CommitState::Ready)
    }
}
// The test policy delegates strategy authorization to the explicit existing mock;
// replication readiness itself requires the real private CommitEvidence.
#[allow(clippy::too_many_arguments)]
pub(super) fn transfer(
    directory: &Path,
    baseline: &tickwire_account_codec::CanonicalBaseline,
    bootstrap: &[u8],
    original: gateway_fixture::SharedBlobs,
    mode: replica::CommitMode,
    account: &tickwire_journal::DurableExport,
    input: &tickwire_journal::DurableExport,
    binding: &tickwire_binance_account::ws::BindingEvidence,
) -> replica::CommitEvidence {
    let mut store =
        tickwire_recovery::FileBlobStore::open(directory.join("source-objects")).unwrap();
    for (key, value) in original.0.lock().unwrap().iter() {
        store.put_sync(*key, value).unwrap();
    }
    let mut persistence = Persistence::new(store);
    let mut initial = Vec::new();
    for bytes in [baseline.bytes(), bootstrap] {
        persistence.put_object(bytes).unwrap();
        initial.push(replica::ObjectRef::new(bytes).unwrap());
    }
    initial.sort_by_key(|o| o.hash);
    let mut prefix = replica::INPUT_PREFIX_MAGIC.to_vec();
    prefix.extend_from_slice(&input.config().stream_epoch);
    prefix.extend_from_slice(&account.config().stream_epoch);
    prefix.extend_from_slice(&baseline.digest());
    prefix.extend_from_slice(&(input.records().len() as u64).to_le_bytes());
    prefix.extend_from_slice(&(input.records().len() as u32).to_le_bytes());
    for record in input.records() {
        let hash = persistence.put_object(&record.payload).unwrap();
        prefix.extend_from_slice(&record.seq.to_le_bytes());
        prefix.extend_from_slice(&record.exec_epoch.to_le_bytes());
        prefix.extend_from_slice(&(record.payload.len() as u64).to_le_bytes());
        prefix.extend_from_slice(&hash);
    }
    let decoded = replica::InputPrefix::decode(&prefix).unwrap();
    assert_eq!(
        decoded.entries[(binding.input.sequence - 1) as usize]
            .object
            .hash,
        binding.input_record_hash
    );
    persistence.put_object(&prefix).unwrap();
    let root = replica::ObjectRef::new(&prefix).unwrap();
    let stream = replica::Stream {
        domain: account.config().domain,
        journal_epoch: account.config().stream_epoch,
        source: tickwire_fencing::ExecutionIdentity {
            host_id: [72; 16],
            boot_id: [73; 16],
            exec_instance_id: [41; 16],
            exec_epoch: 4,
            network_identity: [74; 32],
            credential_access_identity: [42; 32],
        },
        source_storage_id: [75; 16],
        format: replica::Format::Account {
            baseline: baseline.digest(),
        },
        profile_version: 1,
        commit_mode: mode.clone(),
        initial_objects: initial,
    };
    let mut source = replica::SourceWorker::new(
        replica::SourceConfig {
            stream: stream.clone(),
            profile_version: 1,
            mode: mode.clone(),
            max_records: 32,
            max_pending_batches: 8,
            max_pending_bytes: 1 << 20,
        },
        persistence,
    )
    .unwrap();
    let proof = source
        .ingest(
            account,
            &BTreeMap::from([(binding.account_sequence, vec![root.clone()])]),
            100,
        )
        .unwrap();
    assert_eq!(
        proof.dependencies(binding.account_sequence).unwrap(),
        &[root]
    );
    let early = source.commit_evidence(&proof, 100);
    match mode {
        replica::CommitMode::SyncReplica { .. } => {
            assert!(matches!(early, Err(replica::Error::Missing)))
        }
        replica::CommitMode::LocalDurableAsync { .. } => {
            assert!(early.as_ref().unwrap().mirror_ack().is_none())
        }
    }
    let mirror = replica::FileMirror::create_files(
        replica::MirrorConfig {
            stream: stream.clone(),
            receiver_id: [71; 16],
            receiver_boot_id: [76; 16],
            mirror_assignment_epoch: 1,
            profile_version: 1,
            max_records: 32,
            physical: tickwire_journal::Config::new([77; 16], account.config().domain),
        },
        directory.join("mirror-wal"),
        directory.join("mirror-objects"),
    )
    .unwrap();
    let (client_tls, server_tls) = mutual_tls();
    let source_signer = replica::Signer::from_seed([78; 32]).unwrap();
    let mirror_signer = replica::Signer::from_seed([79; 32]).unwrap();
    let source_key = source_signer.public_key();
    let mirror_key = mirror_signer.public_key();
    let limits = replica::NetworkLimits {
        timeout: Duration::from_secs(2),
        max_requests_per_poll: 128,
        max_bytes_per_poll: 1 << 20,
    };
    let mut listener = replica::MirrorListener::bind(
        "127.0.0.1:0".parse().unwrap(),
        mirror,
        mirror_signer,
        source_key,
        server_tls,
        limits,
    )
    .unwrap();
    let address = listener.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let thread = std::thread::spawn(move || {
        while !thread_stop.load(Ordering::Acquire) {
            listener.poll().unwrap();
            std::thread::yield_now();
        }
        listener.mirror().prefix()
    });
    let peer = replica::PeerClient::new(
        address,
        &stream,
        source_signer,
        mirror_key,
        client_tls,
        limits,
    )
    .unwrap();
    let mut sender = replica::SourceSender::new(peer);
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let progress = sender.poll(&mut source).unwrap();
        if progress.durable_sequence == proof.prefix().sequence {
            break;
        }
        assert!(Instant::now() < end);
    }
    let evidence = if matches!(mode, replica::CommitMode::LocalDurableAsync { .. }) {
        early.unwrap()
    } else {
        source.commit_evidence(&proof, 100).unwrap()
    };
    if matches!(mode, replica::CommitMode::SyncReplica { .. }) {
        assert!(evidence.mirror_ack().is_some());
    }
    stop.store(true, Ordering::Release);
    assert_eq!(thread.join().unwrap(), proof.prefix());
    evidence
}
fn mutual_tls() -> (replica::ClientTls, replica::ServerTls) {
    let source = rcgen::generate_simple_self_signed(vec!["source.local".into()]).unwrap();
    let mirror = rcgen::generate_simple_self_signed(vec!["mirror.local".into()]).unwrap();
    let source_der = source.cert.der().to_vec();
    let mirror_der = mirror.cert.der().to_vec();
    let client = replica::ClientTls::new(
        replica::TlsIdentity {
            certificate_chain_der: vec![source_der.clone()],
            private_key_der: source.signing_key.serialize_der(),
            trust_anchors_der: vec![mirror_der.clone()],
            peer_certificate_sha256: tickwire_recovery::digest(&mirror_der),
        },
        "mirror.local".into(),
    )
    .unwrap();
    let server = replica::ServerTls::new(replica::TlsIdentity {
        certificate_chain_der: vec![mirror_der],
        private_key_der: mirror.signing_key.serialize_der(),
        trust_anchors_der: vec![source_der.clone()],
        peer_certificate_sha256: tickwire_recovery::digest(&source_der),
    })
    .unwrap();
    (client, server)
}
