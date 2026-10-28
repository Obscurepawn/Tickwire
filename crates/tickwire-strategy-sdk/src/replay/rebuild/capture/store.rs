//! One-shot capture storage. Content never authenticates the producing process.
use super::{Captured, Contract, Identity, Inputs, VerifiedWindow};
use serde::{Deserialize, Serialize};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use tickwire_release::{Object, replay::MAX_BYTES};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub schema: u16,
    pub attempt: Object,
    pub identity: Identity,
    pub contract: Object,
    pub seed: Object,
    pub inputs: Object,
    pub seal: Object,
}
impl Completion {
    pub fn bytes(&self) -> crate::Result<Vec<u8>> {
        self.identity.validate()?;
        if self.schema != 1 {
            return Err(crate::Error::Identity);
        }
        for object in [
            self.attempt,
            self.contract,
            self.seed,
            self.inputs,
            self.seal,
        ] {
            object.validate(MAX_BYTES as u64)?;
        }
        Ok(tickwire_release::replay::canonical(self)?)
    }
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(crate::Error::Bounds);
        }
        let value: Self = serde_json::from_slice(bytes)?;
        if value.bytes()? != bytes {
            return Err(crate::Error::Identity);
        }
        Ok(value)
    }
}

pub struct Attempt {
    directory: PathBuf,
    directory_identity: (u64, u64),
    identity: Identity,
    contract: Contract,
    seed: Inputs,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    schema: u16,
    identity: Identity,
    contract: Object,
    seed: Object,
}
impl Intent {
    fn bytes(&self) -> crate::Result<Vec<u8>> {
        self.identity.validate()?;
        if self.schema != 1 {
            return Err(crate::Error::Identity);
        }
        self.contract.validate(MAX_BYTES as u64)?;
        self.seed.validate(MAX_BYTES as u64)?;
        Ok(tickwire_release::replay::canonical(self)?)
    }
}
fn intent(identity: &Identity, contract: &Contract, seed: &Inputs) -> crate::Result<Intent> {
    Ok(Intent {
        schema: 1,
        identity: identity.clone(),
        contract: Object::of(&contract.bytes()?),
        seed: Object::of(&seed.bytes()?),
    })
}
fn directory(path: &Path, private: bool) -> crate::Result<(u64, u64)> {
    if !path.is_absolute() || std::fs::canonicalize(path)? != path {
        return Err(crate::Error::Identity);
    }
    let metadata = std::fs::symlink_metadata(path)?;
    let permissions = if private { 0o077 } else { 0o022 };
    if !metadata.is_dir() || metadata.mode() & permissions != 0 {
        return Err(crate::Error::Identity);
    }
    Ok((metadata.dev(), metadata.ino()))
}
impl Attempt {
    pub fn begin(
        output: &Path,
        identity: &Identity,
        contract: &Contract,
        seed: &Inputs,
    ) -> crate::Result<Self> {
        if !output.is_absolute() {
            return Err(crate::Error::Identity);
        }
        let parent = output.parent().ok_or(crate::Error::Identity)?;
        directory(parent, false)?;
        let domain =
            tickwire_clock::ClockDomain::new(identity.clock.boot_id, identity.clock.generation)
                .map_err(|_| crate::Error::Identity)?;
        super::CaptureOnly::new(
            identity.clone(),
            contract.clone(),
            seed.clone(),
            tickwire_clock::MonoTime::new(domain, seed.start_ns),
        )?;
        let bytes = intent(identity, contract, seed)?.bytes()?;
        // create, not create_all: one directory means one attempt, even after failure.
        std::fs::DirBuilder::new().mode(0o700).create(output)?;
        std::fs::File::open(parent)?.sync_all()?;
        crate::recovery::immutable(&output.join("ATTEMPT.json"), &bytes)?;
        Ok(Self {
            directory_identity: directory(output, true)?,
            directory: output.to_path_buf(),
            identity: identity.clone(),
            contract: contract.clone(),
            seed: seed.clone(),
        })
    }
    pub fn finish(self, captured: Captured) -> crate::Result<Completion> {
        if directory(&self.directory, true)? != self.directory_identity {
            return Err(crate::Error::Identity);
        }
        let verified = VerifiedWindow::verify(
            &self.identity,
            &self.contract,
            &self.seed,
            captured.inputs(),
            captured.seal(),
        )?;
        let attempt_bytes = intent(&self.identity, &self.contract, &self.seed)?.bytes()?;
        if crate::recovery::read(&self.directory.join("ATTEMPT.json"), MAX_BYTES)? != attempt_bytes
        {
            return Err(crate::Error::Identity);
        }
        let completion = Completion {
            schema: 1,
            attempt: Object::of(&attempt_bytes),
            identity: self.identity,
            contract: verified.seal.contract,
            seed: verified.seal.seed,
            inputs: verified.seal.inputs,
            seal: Object::of(&verified.seal.bytes()?),
        };
        let bytes = completion.bytes()?;
        let objects = verified
            .objects()
            .chain([attempt_bytes.as_slice(), bytes.as_slice()]);
        let mut total = 0u64;
        for object in objects {
            total = total
                .checked_add(object.len() as u64)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(crate::Error::Bounds)?;
            if object.len() > MAX_BYTES {
                return Err(crate::Error::Bounds);
            }
            crate::recovery::object(&self.directory, object)?;
        }
        // The marker is last and every dependency has returned from file+directory sync.
        crate::recovery::immutable(&self.directory.join("CAPTURE_WINDOW.json"), &bytes)?;
        Ok(completion)
    }
}
pub fn read_back(output: &Path, expected: &Completion) -> crate::Result<VerifiedWindow> {
    directory(output, true)?;
    let completion_bytes = expected.bytes()?;
    if crate::recovery::read(&output.join("CAPTURE_WINDOW.json"), MAX_BYTES)? != completion_bytes {
        return Err(crate::Error::Identity);
    }
    let attempt_bytes = crate::recovery::read(&output.join("ATTEMPT.json"), MAX_BYTES)?;
    expected.attempt.verify(&attempt_bytes, MAX_BYTES as u64)?;
    let attempt: Intent = serde_json::from_slice(&attempt_bytes)?;
    if attempt.bytes()? != attempt_bytes
        || attempt.identity != expected.identity
        || attempt.contract != expected.contract
        || attempt.seed != expected.seed
    {
        return Err(crate::Error::Identity);
    }
    let mut total = completion_bytes.len() as u64 + attempt_bytes.len() as u64;
    let mut load = |object: Object| -> crate::Result<Vec<u8>> {
        object.validate(MAX_BYTES as u64)?;
        total = total
            .checked_add(object.bytes)
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
            .ok_or(crate::Error::Bounds)?;
        let name = object
            .digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        Ok(tickwire_release::files::read_object(
            &output.join(format!("{name}.bin")),
            object,
        )?)
    };
    let contract = Contract::decode(&load(expected.contract)?)?;
    let seed = Inputs::decode(&load(expected.seed)?)?;
    let inputs = Inputs::decode(&load(expected.inputs)?)?;
    let seal = tickwire_release::rebuild::window::Seal::decode(&load(expected.seal)?)?;
    // The content-addressed copies are also part of the declared output closure.
    load(expected.attempt)?;
    load(Object::of(&completion_bytes))?;
    let mut result = VerifiedWindow::verify(&expected.identity, &contract, &seed, &inputs, &seal)?;
    result.objects.extend([attempt_bytes, completion_bytes]);
    Ok(result)
}
