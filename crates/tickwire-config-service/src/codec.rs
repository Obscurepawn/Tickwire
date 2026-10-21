use crate::{
    CHUNK_BYTES, Config, Error, Identity, MAX_COMMAND_BYTES, ObjectCommand, ObjectLimits,
    ObjectSpec, Purpose, Result,
};
const MAGIC: &[u8; 8] = b"TWCF0001";
pub(crate) enum Decoded {
    Initialize(Config),
    InitializeTrusted(Config, crate::Hash),
    Object(ObjectCommand),
    Bootstrap(crate::bootstrap::Bootstrap),
}
fn prefix(cluster: Identity, tag: u8) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    out.extend_from_slice(&cluster);
    out.push(tag);
    out
}
pub(crate) fn initialize(config: Config) -> Vec<u8> {
    let mut out = prefix(config.cluster, 1);
    out.extend_from_slice(&config.objects.max_objects.to_le_bytes());
    out.extend_from_slice(&config.objects.max_inflight.to_le_bytes());
    out.extend_from_slice(&config.objects.max_payload_bytes.to_le_bytes());
    out
}
pub(crate) fn initialize_with_trust(config: Config, trust: Option<crate::Hash>) -> Vec<u8> {
    let mut out = initialize(config);
    if let Some(hash) = trust {
        out[24] = 5;
        out.extend_from_slice(&hash);
    }
    out
}
pub(crate) fn bootstrap(
    cluster: Identity,
    request: &crate::bootstrap::Bootstrap,
) -> Result<Vec<u8>> {
    identity(request.candidate, request.nonce)?;
    let mut out = prefix(cluster, 6);
    out.extend_from_slice(&request.candidate);
    out.extend_from_slice(&request.candidate_hash);
    out.extend_from_slice(&request.nonce);
    out.extend_from_slice(&request.signature);
    Ok(out)
}
pub(crate) fn encode(cluster: Identity, command: &ObjectCommand) -> Result<Vec<u8>> {
    let mut out = match command {
        ObjectCommand::Begin(spec) => {
            spec.validate()?;
            let mut out = prefix(cluster, 2);
            out.extend_from_slice(&spec.key);
            out.extend_from_slice(&spec.owner);
            out.push(spec.purpose as u8);
            out.extend_from_slice(&spec.length.to_le_bytes());
            out.extend_from_slice(&spec.hash);
            out
        }
        ObjectCommand::Chunk {
            key,
            owner,
            offset,
            bytes,
        } => {
            if bytes.is_empty() || bytes.len() > CHUNK_BYTES {
                return Err(Error::Bounds);
            }
            let mut out = prefix(cluster, 3);
            out.extend_from_slice(key);
            out.extend_from_slice(owner);
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
            out
        }
        ObjectCommand::Seal { key, owner } => {
            let mut out = prefix(cluster, 4);
            out.extend_from_slice(key);
            out.extend_from_slice(owner);
            out
        }
    };
    // Encoding and decoding share the same structural bounds.
    decode(cluster, &out)?;
    out.shrink_to_fit();
    Ok(out)
}
pub(crate) fn decode(cluster: Identity, bytes: &[u8]) -> Result<Decoded> {
    if bytes.len() < 25
        || bytes.len() > MAX_COMMAND_BYTES
        || &bytes[..8] != MAGIC
        || bytes[8..24] != cluster
    {
        return Err(Error::Protocol);
    }
    let mut reader = Reader(&bytes[25..]);
    let command = match bytes[24] {
        1 | 5 => {
            let config = Config {
                cluster,
                objects: ObjectLimits {
                    max_objects: reader.u32()?,
                    max_inflight: reader.u32()?,
                    max_payload_bytes: reader.u64()?,
                },
            };
            config.validate()?;
            if bytes[24] == 5 {
                Decoded::InitializeTrusted(config, reader.array()?)
            } else {
                Decoded::Initialize(config)
            }
        }
        2 => {
            let spec = ObjectSpec {
                key: reader.array()?,
                owner: reader.array()?,
                purpose: Purpose::from_byte(reader.byte()?)?,
                length: reader.u32()?,
                hash: reader.array()?,
            };
            spec.validate()?;
            Decoded::Object(ObjectCommand::Begin(spec))
        }
        3 => {
            let key = reader.array()?;
            let owner = reader.array()?;
            let offset = reader.u32()?;
            let length = reader.u32()? as usize;
            identity(key, owner)?;
            if !(1..=CHUNK_BYTES).contains(&length) {
                return Err(Error::Bounds);
            }
            let payload = reader.take(length)?.to_vec();
            Decoded::Object(ObjectCommand::Chunk {
                key,
                owner,
                offset,
                bytes: payload,
            })
        }
        4 => {
            let key = reader.array()?;
            let owner = reader.array()?;
            identity(key, owner)?;
            Decoded::Object(ObjectCommand::Seal { key, owner })
        }
        6 => {
            let request = crate::bootstrap::Bootstrap {
                candidate: reader.array()?,
                candidate_hash: reader.array()?,
                nonce: reader.array()?,
                signature: reader.array()?,
            };
            identity(request.candidate, request.nonce)?;
            Decoded::Bootstrap(request)
        }
        _ => return Err(Error::Protocol),
    };
    if !reader.0.is_empty() {
        return Err(Error::Protocol);
    }
    Ok(command)
}
fn identity(key: Identity, owner: Identity) -> Result<()> {
    if key == [0; 16] || owner == [0; 16] {
        Err(Error::Identity)
    } else {
        Ok(())
    }
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let out = self.0.get(..length).ok_or(Error::Protocol)?;
        self.0 = &self.0[length..];
        Ok(out)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| Error::Protocol)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn literal(text: &str) -> Vec<u8> {
        text.trim()
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
            .collect()
    }
    #[test]
    fn independent_openssl_bootstrap_and_trusted_initialization_vectors() {
        let public_key = literal(include_str!("../tests/fixtures/bootstrap-public.hex"))
            .try_into()
            .unwrap();
        let trust = crate::bootstrap::BootstrapTrust::new(
            [19; 16],
            [0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1],
            "tickwire-qualification".into(),
            public_key,
        )
        .unwrap();
        let init = literal(include_str!("../tests/fixtures/initialize-trusted.hex"));
        let Decoded::InitializeTrusted(config, trust_hash) = decode([19; 16], &init).unwrap()
        else {
            panic!("wrong initialization kind");
        };
        assert_eq!(trust_hash, trust.digest());
        assert_eq!(initialize_with_trust(config, Some(trust_hash)), init);
        let bytes = literal(include_str!("../tests/fixtures/bootstrap.hex"));
        let Decoded::Bootstrap(request) = decode([19; 16], &bytes).unwrap() else {
            panic!("wrong bootstrap kind");
        };
        assert_eq!(bootstrap([19; 16], &request).unwrap(), bytes);
        let message = trust
            .signing_bytes(request.candidate, request.candidate_hash, request.nonce)
            .unwrap();
        assert_eq!(
            message,
            literal(include_str!("../tests/fixtures/bootstrap-signing.hex"))
        );
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, public_key)
            .verify(&message, &request.signature)
            .unwrap();
        for value in [&init, &bytes] {
            for length in 0..value.len() {
                assert!(decode([19; 16], &value[..length]).is_err());
            }
            let mut extra = value.to_vec();
            extra.push(0);
            assert!(decode([19; 16], &extra).is_err());
            assert!(decode([20; 16], value).is_err());
        }
    }
    #[test]
    fn independently_encoded_initialization_and_object_vectors_are_stable() {
        // Python struct.pack('<IIQ', 16, 4, 32*1024*1024), plus fixed header.
        let init = literal(include_str!("../tests/fixtures/initialize.hex"));
        let Decoded::Initialize(config) = decode([19; 16], &init).unwrap() else {
            panic!("wrong command kind");
        };
        assert_eq!(config.objects.max_objects, 16);
        assert_eq!(config.objects.max_inflight, 4);
        assert_eq!(config.objects.max_payload_bytes, 32 * 1024 * 1024);
        assert_eq!(initialize(config), init);
        // Independent SHA256(b'candidate') and struct.pack('<I', 9).
        let begin = literal(include_str!("../tests/fixtures/begin.hex"));
        let Decoded::Object(ObjectCommand::Begin(spec)) = decode([19; 16], &begin).unwrap() else {
            panic!("wrong command kind");
        };
        assert_eq!(spec.key, [6; 16]);
        assert_eq!(spec.owner, [7; 16]);
        assert_eq!(spec.length, 9);
        assert_eq!(spec.purpose, Purpose::Candidate);
        assert_eq!(
            encode([19; 16], &ObjectCommand::Begin(spec)).unwrap(),
            begin
        );
    }
    #[test]
    fn truncation_unknown_tags_trailing_bytes_and_cross_cluster_input_fail() {
        for text in [
            include_str!("../tests/fixtures/initialize.hex"),
            include_str!("../tests/fixtures/begin.hex"),
        ] {
            let bytes = literal(text);
            for n in 0..bytes.len() {
                assert!(decode([19; 16], &bytes[..n]).is_err(), "accepted cut {n}");
            }
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert!(decode([19; 16], &trailing).is_err());
            assert!(decode([20; 16], &bytes).is_err());
            for tag in [0, 5, 255] {
                let mut mutated = bytes.clone();
                mutated[24] = tag;
                assert!(decode([19; 16], &mutated).is_err());
            }
        }
        let mut oversized = literal(include_str!("../tests/fixtures/begin.hex"));
        oversized[57] = 2; // Projection has a smaller per-purpose payload bound.
        oversized[58..62].copy_from_slice(&(4 * 1024 * 1024 + 1u32).to_le_bytes());
        assert!(matches!(decode([19; 16], &oversized), Err(Error::Bounds)));
    }
}
