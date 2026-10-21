//! Explicit, purpose-bound association of persisted account observations with
//! complete funding-income identities. No signing key is held by this module.
use super::*;
mod authority;
mod dependencies;
pub use dependencies::dependency_hashes;
mod history;
mod owner;
pub use owner::AssociationRequest;
pub(crate) mod encoding;
pub use encoding::AssociationProposal;
pub(crate) mod policy;
pub use authority::{
    ActiveAssociationPolicy, AssociationAuthority, AuthorityClaims, SignedAssociationApproval,
    SignedAssociationAuthority,
};

#[cfg(test)]
mod tests;
