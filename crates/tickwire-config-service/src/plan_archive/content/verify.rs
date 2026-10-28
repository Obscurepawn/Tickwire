use super::*;

impl ContentArchive {
    pub(super) fn validate(&self, manifest: &Manifest) -> Result<()> {
        if manifest.schema != 1
            || manifest.id == [0; 16]
            || !crate::auth::valid_subject(&manifest.subject)
            || manifest.leaf_sha256 == [0; 32]
            || manifest.request_digest == [0; 32]
            || manifest.rules_digest == [0; 32]
            || manifest.sources_digest == [0; 32]
            || manifest.checked_unix_ms == 0
            || manifest.checked_unix_ms >= manifest.expires_unix_ms
            || manifest.expires_unix_ms >= 1 << 53
        {
            return Err(Error::Identity);
        }
        manifest.limits.decode()?;
        let revision = decimal(&manifest.base_revision)?;
        let next = revision
            .checked_add(1)
            .filter(|v| *v < 1 << 63)
            .ok_or(Error::Bounds)?;
        decimal(&manifest.base_index)?;
        decimal(&manifest.base_term)?;
        for (reference, purpose) in [
            (&manifest.base, Purpose::Candidate),
            (&manifest.candidate, Purpose::Candidate),
            (&manifest.input, Purpose::Candidate),
            (&manifest.plan, Purpose::PlanMetadata),
            (&manifest.trust, Purpose::Qualification),
        ] {
            if reference.purpose != purpose as u8 {
                return Err(Error::Identity);
            }
        }
        for reference in manifest.references() {
            if reference.owner != manifest.id {
                return Err(Error::Identity);
            }
            let bytes = self.bytes(reference)?;
            if bytes.len() != reference.length as usize
                || <Hash>::from(Sha256::digest(bytes)) != reference.hash
            {
                return Err(Error::Digest);
            }
        }
        let base = canonical(self.bytes(&manifest.base)?, Purpose::Candidate)?;
        let candidate = canonical(self.bytes(&manifest.candidate)?, Purpose::Candidate)?;
        canonical(self.bytes(&manifest.input)?, Purpose::Candidate)?;
        canonical(self.bytes(&manifest.trust)?, Purpose::Qualification)?;
        let head =
            tickwire_config::patch::Head::new(revision, &base).map_err(Error::Configuration)?;
        if head.etag() != manifest.if_match {
            return Err(Error::Conflict);
        }
        let plan = canonical(self.bytes(&manifest.plan)?, Purpose::PlanMetadata)?;
        let mut digest = Sha256::new();
        digest.update(b"tickwire.config.computed-plan.v1\0");
        digest.update(plan.canonical_bytes());
        if <Hash>::from(digest.finalize()) != manifest.plan_digest {
            return Err(Error::Digest);
        }
        let p = plan.value();
        if p["base_revision"] != manifest.base_revision
            || decimal(p["next_revision"].as_str().ok_or(Error::Protocol)?)? != next
            || p["base_hash"] != hash_text(manifest.base.hash)
            || p["candidate_hash"] != hash_text(manifest.candidate.hash)
            || p["base_etag"] != manifest.if_match
        {
            return Err(Error::Conflict);
        }
        let old = processes(&base)?;
        let new = processes(&candidate)?;
        let all: BTreeSet<_> = old.union(&new).cloned().collect();
        if all.iter().ne(manifest.targets.keys())
            || p["targets"]
                .as_object()
                .ok_or(Error::Protocol)?
                .keys()
                .ne(manifest.targets.keys())
        {
            return Err(Error::Incomplete);
        }
        for (id, target) in &manifest.targets {
            if target.old.is_some() != old.contains(id) {
                return Err(Error::Identity);
            }
            let item = &p["targets"][id];
            if item["old_projection"].is_null() != !old.contains(id)
                || item["new_projection"].is_null() != !new.contains(id)
                || (old.contains(id)
                    && item["old_projection"]["global_hash"] != hash_text(manifest.base.hash))
                || (new.contains(id)
                    && item["new_projection"]["global_hash"] != hash_text(manifest.candidate.hash))
            {
                return Err(Error::Identity);
            }
            if let Some(reference) = &target.old {
                self.projection(
                    reference,
                    id,
                    revision,
                    manifest.base.hash,
                    &item["old_projection"],
                )?;
            }
            let binding = if new.contains(id) {
                &item["new_projection"]
            } else {
                &item["old_projection"]
            };
            self.projection(&target.next, id, next, manifest.candidate.hash, binding)?;
        }
        if manifest.bundle.is_empty()
            || manifest.bundle.len() > MAX_BYTES / (16 * 1024 * 1024) + 1
            || manifest.bundle_length == 0
            || manifest.bundle_length > MAX_BYTES as u64
        {
            return Err(Error::Bounds);
        }
        let mut length = 0u64;
        let mut digest = Sha256::new();
        for (i, reference) in manifest.bundle.iter().enumerate() {
            if reference.purpose != Purpose::Qualification as u8
                || (i + 1 < manifest.bundle.len()
                    && reference.length != Purpose::Qualification.max_bytes())
            {
                return Err(Error::Protocol);
            }
            length = length
                .checked_add(reference.length.into())
                .filter(|n| *n <= MAX_BYTES as u64)
                .ok_or(Error::Bounds)?;
            digest.update(self.bytes(reference)?);
        }
        if length != manifest.bundle_length
            || <Hash>::from(digest.finalize()) != manifest.bundle_hash
        {
            return Err(Error::Digest);
        }
        // A trust description is retained as historical configuration, never installed as current trust.
        let mut digest = Sha256::new();
        digest.update(b"tickwire.config.evidence-trust.v1\0");
        digest.update(self.bytes(&manifest.trust)?);
        if <Hash>::from(digest.finalize()) != manifest.trust_digest {
            return Err(Error::Digest);
        }
        Ok(())
    }
    fn projection(
        &self,
        reference: &ProjectionRef,
        id: &str,
        revision: u64,
        global: Hash,
        plan: &serde_json::Value,
    ) -> Result<()> {
        if reference.object.purpose != Purpose::Projection as u8
            || plan["policy_revision"] != reference.policy_revision
            || plan["projection_hash"] != hash_text(reference.projection_hash)
        {
            return Err(Error::Identity);
        }
        let policy_revision = decimal(&reference.policy_revision)?;
        if policy_revision >= 1 << 63 {
            return Err(Error::Bounds);
        }
        let object = canonical(self.bytes(&reference.object)?, Purpose::Projection)?;
        let value = object.value();
        let fields = [
            "global_config_hash",
            "global_revision",
            "payload",
            "projection_hash",
            "projection_policy_revision",
            "projection_schema_version",
            "target_process_id",
        ];
        if value
            .as_object()
            .ok_or(Error::Protocol)?
            .keys()
            .map(String::as_str)
            .ne(fields)
            || value["projection_schema_version"] != 1
            || decimal(value["global_revision"].as_str().ok_or(Error::Protocol)?)? != revision
            || value["global_config_hash"] != hash_text(global)
            || value["projection_policy_revision"] != reference.policy_revision
            || value["target_process_id"] != id
            || value["payload"]["process"]["id"] != id
            || value["projection_hash"] != hash_text(reference.projection_hash)
        {
            return Err(Error::Identity);
        }
        // Historical payload rules may differ. Preserve their canonical bytes and
        // hash here; only explicit reverify runs current projection semantics.
        let payload = parse(
            &serde_json::to_vec(&value["payload"]).map_err(|_| Error::Protocol)?,
            Purpose::Projection.max_bytes() as usize,
        )?;
        if payload.hash() != reference.projection_hash {
            return Err(Error::Digest);
        }
        Ok(())
    }
}
fn canonical(bytes: &[u8], purpose: Purpose) -> Result<Candidate> {
    let candidate = parse(bytes, purpose.max_bytes() as usize)?;
    if candidate.canonical_bytes() != bytes {
        return Err(Error::Protocol);
    }
    Ok(candidate)
}
fn processes(candidate: &Candidate) -> Result<BTreeSet<String>> {
    let rows = candidate.value()["processes"]
        .as_array()
        .ok_or(Error::Protocol)?;
    if rows.is_empty() || rows.len() > MAX_OBJECTS {
        return Err(Error::Bounds);
    }
    let mut ids = BTreeSet::new();
    for row in rows {
        let id = row["id"].as_str().ok_or(Error::Protocol)?;
        if id.is_empty() || id.len() > 128 || !ids.insert(id.to_owned()) {
            return Err(Error::Identity);
        }
    }
    Ok(ids)
}
