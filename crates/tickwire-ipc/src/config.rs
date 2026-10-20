use crate::*;

/// Context supplied by the authenticated connection and independently verified commit/receipts.
/// Boolean proof flags attest an external verification; this codec does not authenticate artifacts.
#[derive(Debug, Clone, Default)]
pub struct ConfigContext<'a> {
    pub negotiated_features: u64,
    pub authenticated_actor_kind: u16,
    pub authenticated_sender_instance: [u8; 16],
    pub authenticated_issuer_boot_id: [u8; 16],
    pub authenticated_host_id: &'a str,
    pub target_instance_id: [u8; 16],
    pub target_boot_id: [u8; 16],
    pub target_host_boot_id: [u8; 16],
    pub target_exec_generation: u64,
    pub current_runtime_revision: u64,
    pub committed_config_revision: u64,
    pub committed_base_revision: u64,
    pub committed_global_config_hash: [u8; 32],
    pub committed_control_commit_ref: [u8; 32],
    pub committed_target_set_hash: [u8; 32],
    pub proof_objects_prevalidated: bool,
    pub runtime_receipt_verified: bool,
    pub file_receipt_verified: bool,
    pub full_target_set_verified: bool,
    pub completion_commit_verified: bool,
}

pub fn validate_config_binding(
    frame: &ControlFrame<'_>,
    c: &ConfigContext<'_>,
) -> Result<ConfigControlBody> {
    if c.negotiated_features & 32 == 0 {
        return Err(Error::UnsupportedFeature);
    }
    let e = &frame.envelope;
    let b = ConfigControlBody::decode(frame.body)?;
    if !(80..=84).contains(&e.opcode) {
        return Err(Error::UnknownMessage);
    }
    if b.operation_id != e.operation_id || b.operation_revision != e.operation_revision {
        return Err(Error::Identity);
    }
    if b.actor_kind != c.authenticated_actor_kind
        || e.sender_instance != c.authenticated_sender_instance
        || b.issuer_boot_id != c.authenticated_issuer_boot_id
    {
        return Err(Error::Permission);
    }
    if e.fd_count != 0 || e.frame_bytes != 896 {
        return Err(Error::Malformed);
    }
    if b.apply_class > 5
        || b.config_status > 10
        || !(1..=3).contains(&b.actor_kind)
        || b.file_kind > 2
        || b.projection_transport > 2
        || b.projection_format > 2
        || b.config_flags & !3 != 0
    {
        return Err(Error::InvalidEnum);
    }
    if b.config_revision != c.committed_config_revision
        || b.base_revision != c.committed_base_revision
        || b.global_config_hash != c.committed_global_config_hash
        || b.control_commit_ref != c.committed_control_commit_ref
        || b.target_set_hash != c.committed_target_set_hash
    {
        return Err(Error::VersionConflict);
    }
    if !c.proof_objects_prevalidated {
        return Err(Error::RecoveryEvidence);
    }
    if e.opcode == 84 && b.config_status == 10 {
        if b.actor_kind != 1 || b.config_flags != 1 || !matches!(e.flags, 10 | 32) {
            return Err(Error::Permission);
        }
        if b.process_id_length != 0
            || b.target_host_id_length != 0
            || b.expected_exec_generation != 0
            || b.projection_policy_revision != 0
            || b.projection_object_bytes != 0
        {
            return Err(Error::Malformed);
        }
        for field in [
            &b.process_id[..],
            &b.target_host_id[..],
            &b.expected_instance_id[..],
            &b.expected_boot_id[..],
            &b.expected_host_boot_id[..],
            &b.instance_slot_id[..],
            &b.projection_hash[..],
            &b.projection_object_hash[..],
            &b.projection_resource_id[..],
            &b.runtime_receipt_hash[..],
            &b.file_receipt_hash[..],
            &b.journal_receipt_hash[..],
        ] {
            if !is_zero(field) {
                return Err(Error::Malformed);
            }
        }
        if is_zero(&b.completion_receipt_hash) {
            return Err(Error::RecoveryEvidence);
        }
        if !c.full_target_set_verified || !c.completion_commit_verified {
            return Err(Error::NotReady);
        }
        return Ok(b);
    }
    config_id(&b.process_id, b.process_id_length)?;
    if config_id(&b.target_host_id, b.target_host_id_length)? != c.authenticated_host_id.as_bytes()
    {
        return Err(Error::Identity);
    }
    if b.expected_instance_id != c.target_instance_id
        || b.expected_boot_id != c.target_boot_id
        || b.expected_host_boot_id != c.target_host_boot_id
        || b.expected_exec_generation != c.target_exec_generation
    {
        return Err(Error::Identity);
    }
    if !(1..=4194304).contains(&b.projection_object_bytes) || b.projection_policy_revision == 0 {
        return Err(Error::Bounds);
    }
    match b.projection_transport {
        1 if !is_zero(&b.projection_resource_id) => return Err(Error::Malformed),
        2 if is_zero(&b.projection_resource_id) => return Err(Error::MissingResource),
        0 => return Err(Error::Malformed),
        _ => (),
    }
    if b.apply_class == 5 {
        return Err(Error::UnsupportedOperation);
    }
    if b.apply_class == 3 && (is_zero(&b.successor_binding_hash) || b.config_flags & 2 == 0) {
        return Err(Error::Identity);
    }
    if !is_zero(&b.completion_receipt_hash) {
        return Err(Error::Permission);
    }
    match e.opcode {
        80 => {
            let request = e.flags == 1;
            if (request && (b.actor_kind != 1 || b.config_status != 1))
                || (!request && (e.flags != 10 || b.actor_kind != 2 || b.config_status != 3))
            {
                return Err(Error::Permission);
            }
            if !is_zero(&b.runtime_receipt_hash)
                || !is_zero(&b.file_receipt_hash)
                || (request && !is_zero(&b.journal_receipt_hash))
            {
                return Err(Error::RecoveryEvidence);
            }
            if !request && is_zero(&b.journal_receipt_hash) {
                return Err(Error::RecoveryEvidence);
            }
        }
        81 => {
            if b.actor_kind != 2 || e.flags != 1 || b.config_status != 3 || b.file_kind != 1 {
                return Err(Error::Permission);
            }
            if e.target_instance != b.expected_instance_id {
                return Err(Error::Identity);
            }
            if !is_zero(&b.runtime_receipt_hash)
                || is_zero(&b.file_receipt_hash)
                || is_zero(&b.journal_receipt_hash)
            {
                return Err(Error::RecoveryEvidence);
            }
            if c.current_runtime_revision != b.expected_runtime_revision
                || b.config_revision <= c.current_runtime_revision
            {
                return Err(Error::VersionConflict);
            }
        }
        82 => {
            if b.actor_kind != 3
                || b.config_status != 5
                || b.file_kind != 0
                || !is_zero(&b.file_receipt_hash)
                || !matches!(e.flags, 1 | 10)
            {
                return Err(Error::Permission);
            }
            if b.applied_event_seq == 0
                || is_zero(&b.runtime_receipt_hash)
                || is_zero(&b.journal_receipt_hash)
            {
                return Err(Error::RecoveryEvidence);
            }
        }
        83 => {
            if b.actor_kind != 2 || e.flags != 1 {
                return Err(Error::Permission);
            }
            if is_zero(&b.file_receipt_hash) || is_zero(&b.journal_receipt_hash) {
                return Err(Error::RecoveryEvidence);
            }
            match b.config_status {
                6 => {
                    if b.file_kind != 2
                        || b.applied_event_seq == 0
                        || is_zero(&b.runtime_receipt_hash)
                        || !c.runtime_receipt_verified
                        || !c.file_receipt_verified
                    {
                        return Err(Error::RecoveryEvidence);
                    }
                }
                2 => {
                    if b.file_kind != 1
                        || b.applied_event_seq != 0
                        || !is_zero(&b.runtime_receipt_hash)
                    {
                        return Err(Error::Malformed);
                    }
                }
                _ => return Err(Error::Malformed),
            }
        }
        _ => return Err(Error::UnsupportedOperation),
    }
    Ok(b)
}

fn config_id(bytes: &[u8], length: u16) -> Result<&[u8]> {
    if !(1..=63).contains(&length) {
        return Err(Error::Malformed);
    }
    let value = padded(bytes, length as usize).map_err(|_| Error::Malformed)?;
    if !value[0].is_ascii_lowercase()
        || value
            .iter()
            .any(|b| !b.is_ascii_lowercase() && !b.is_ascii_digit() && *b != b'-')
    {
        return Err(Error::Malformed);
    }
    Ok(value)
}

/// Validates all three independent deadlines; does not release a risk gate by itself.
pub fn validate_grant_deadline(
    grant: &GrantBody,
    lease: &ExecLeaseBody,
    now: u64,
    host_boot_id: &[u8; 16],
    exec_generation: u64,
    resource_deadline: u64,
) -> Result<()> {
    if lease.host_boot_id != *host_boot_id
        || lease.exec_generation != exec_generation
        || grant.exec_generation != exec_generation
    {
        return Err(Error::Identity);
    }
    if lease.deadline_mono_ns.checked_sub(lease.issued_mono_ns)
        != Some(u64::from(lease.lease_ms) * 1_000_000)
        || grant.control_lease_deadline_mono_ns != lease.deadline_mono_ns
    {
        return Err(Error::Malformed);
    }
    if !(lease.issued_mono_ns <= now
        && now < lease.deadline_mono_ns
        && now < resource_deadline
        && grant.valid_from_mono_ns <= now
        && now < grant.valid_until_mono_ns)
    {
        return Err(Error::Deadline);
    }
    Ok(())
}
