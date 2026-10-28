//! Attach an independently executed rollback completion to the selected worker.
use super::{CaptureBootstrap, Request as InstallRequest, *};
pub const KIND: u16 = 10;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema: u16,
    pub installation: Object,
    pub completion: Object,
    pub objects: Vec<Object>,
    pub deadline_mono_ns: u64,
}
impl Request {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1
            || self.deadline_mono_ns == 0
            || self.objects.is_empty()
            || self.objects.len() > 512
            || self.objects.windows(2).any(|w| w[0].digest >= w[1].digest)
            || !self.objects.contains(&self.completion)
        {
            return Err(Error::Identity);
        }
        self.installation
            .validate(tickwire_release::replay::MAX_BYTES as u64)?;
        self.completion
            .validate(tickwire_release::replay::MAX_BYTES as u64)?;
        let mut total = 0u64;
        for object in &self.objects {
            object.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            total = total
                .checked_add(object.bytes)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(Error::Bounds)?;
        }
        let bytes = tickwire_release::replay::canonical(self)?;
        if bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let value: Self = serde_json::from_slice(bytes)?;
        if value.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(value)
    }
    pub fn validate(
        &self,
        original: &content::Request,
        installed: &content::Installation,
        now: MonoTime,
    ) -> Result<()> {
        self.bytes()?;
        original.bytes()?;
        installed.bytes()?;
        if self.installation != Object::of(&installed.bytes()?)
            || installed.request != Object::of(&original.bytes()?)
            || now.domain().boot_id() != original.member.clock_boot_id
            || now.domain().generation() != original.member.clock_generation
            || self.deadline_mono_ns > original.deadline_mono_ns
            || now.nanos() >= self.deadline_mono_ns
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub fn wire_request(
    config: &CaptureBootstrap,
    install: &InstallRequest,
    request: &Request,
    until: u64,
) -> Result<ReplayControlBody> {
    let object = Object::of(&request.bytes()?);
    let mut challenge = b"tickwire.rebuild.rollback.v1".to_vec();
    challenge.extend(object.digest);
    challenge.extend(config.identity.capture);
    Ok(ReplayControlBody {
        instance_id: config.identity.instance,
        strategy_generation: config.identity.generation,
        logical_strategy_id: config.identity.logical_strategy,
        challenge: tickwire_recovery::digest(&challenge)[..16]
            .try_into()
            .map_err(|_| Error::Bounds)?,
        valid_until_mono_ns: request.deadline_mono_ns.min(until),
        object_digest: object.digest,
        object_bytes: object.bytes,
        expected_sequence: 0,
        expected_cut_digest: request.installation.digest,
        operation_id: install.request.operation.operation_id,
        operation_revision: install.request.operation.operation_revision,
        schema: 1,
        replay_kind: KIND,
    })
}
pub fn verify_request(
    envelope: &ControlEnvelope,
    body: &ReplayControlBody,
    config: &CaptureBootstrap,
    install: &InstallRequest,
    request: &Request,
    now: MonoTime,
    until: u64,
) -> Result<()> {
    if envelope.opcode != 41
        || envelope.flags != 1
        || envelope.status != 0
        || envelope.expected_state_version != 0
        || envelope.request_id != body.challenge
        || now.domain() != config.market.domain()?
        || now.nanos() >= body.valid_until_mono_ns
        || *body != wire_request(config, install, request, until)?
    {
        return Err(Error::Identity);
    }
    Ok(())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema: u16,
    pub request: Object,
    pub installation: Object,
    pub completion: Object,
    pub pid: u32,
    pub observed_mono_ns: u64,
}
impl Receipt {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1 || self.pid == 0 || self.observed_mono_ns == 0 {
            return Err(Error::Identity);
        }
        for o in [self.request, self.installation, self.completion] {
            o.validate(tickwire_release::replay::MAX_BYTES as u64)?;
        }
        Ok(tickwire_release::replay::canonical(self)?)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let v: Self = serde_json::from_slice(bytes)?;
        if v.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(v)
    }
    pub fn verify(&self, request: &Request, pid: u32, now: MonoTime) -> Result<()> {
        self.bytes()?;
        if self.request != Object::of(&request.bytes()?)
            || self.installation != request.installation
            || self.completion != request.completion
            || self.pid != pid
            || self.observed_mono_ns > now.nanos()
            || now.nanos() >= request.deadline_mono_ns
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rollback_control_binds_original_installation_and_exact_probe() {
        let (capture, install, _) = super::super::tests::fixture();
        let completion = Object::of(b"actual-host-probe-completion");
        let request = Request {
            schema: 1,
            installation: Object::of(b"installation"),
            completion,
            objects: vec![completion],
            deadline_mono_ns: capture.deadline_mono_ns,
        };
        let now = MonoTime::new(capture.market.domain().unwrap(), 100);
        let body = wire_request(&capture, &install, &request, capture.deadline_mono_ns).unwrap();
        assert_eq!(body.replay_kind, 10);
        assert_eq!(body.expected_sequence, 0);
        assert_eq!(body.expected_cut_digest, request.installation.digest);
        let envelope = ControlEnvelope {
            opcode: 41,
            flags: 1,
            request_id: body.challenge,
            ..Default::default()
        };
        verify_request(
            &envelope,
            &body,
            &capture,
            &install,
            &request,
            now,
            capture.deadline_mono_ns,
        )
        .unwrap();
        let receipt = Receipt {
            schema: 1,
            request: Object::of(&request.bytes().unwrap()),
            installation: request.installation,
            completion,
            pid: 7,
            observed_mono_ns: 100,
        };
        receipt.verify(&request, 7, now).unwrap();
        assert!(receipt.verify(&request, 8, now).is_err());
        let mut changed = request.clone();
        changed.installation = Object::of(b"another-installation");
        assert!(
            verify_request(
                &envelope,
                &body,
                &capture,
                &install,
                &changed,
                now,
                capture.deadline_mono_ns
            )
            .is_err()
        );
        assert!(receipt.verify(&changed, 7, now).is_err());
        changed = request.clone();
        changed.completion = Object::of(b"another-probe");
        changed.objects = vec![changed.completion];
        assert!(receipt.verify(&changed, 7, now).is_err());
        let mut bytes = request.bytes().unwrap();
        bytes.push(b' ');
        assert!(Request::decode(&bytes).is_err());
        let expired = MonoTime::new(now.domain(), request.deadline_mono_ns);
        assert!(
            verify_request(
                &envelope,
                &body,
                &capture,
                &install,
                &request,
                expired,
                capture.deadline_mono_ns
            )
            .is_err()
        );
    }
}
