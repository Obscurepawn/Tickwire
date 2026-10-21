use super::*;
use tickwire_account::IncomeObservationsAssociated;
/// Exact immutable roots needed to transport an association closure. This checks
/// canonical shape and bindings; signature authority and actual WAL provenance
/// remain the responsibility of AccountIngress during replay.
pub fn dependency_hashes(
    binding: &IncomeObservationsAssociated,
    manifest: &[u8],
    approval: &[u8],
) -> Result<[v::Hash; 6]> {
    let proposal = AssociationProposal::decode(manifest)?;
    let signed = SignedAssociationApproval::decode(approval)?;
    let claims = authority::ApprovalClaims::decode(signed.payload())?;
    let m = &proposal.manifest;
    if m.binding(proposal.digest(), signed.digest())? != *binding
        || claims.manifest != proposal.digest()
        || claims.request != m.request
        || claims.authority != m.certificate
        || signed.authority().digest() != m.certificate
    {
        return Err(Error::Identity);
    }
    Ok([
        proposal.digest(),
        signed.digest(),
        m.certificate,
        m.policy.digest,
        claims.audit,
        signed.authority().claims().authorization_digest,
    ])
}
