use super::*;
pub fn bind_order(
    _message: &PrivateEvent<'_>,
    _command: &v::CanonicalCommand,
    _identity: &v::OrderIdentity,
    _assets: &[v::AssetRef],
) -> Result<v::CanonicalEvent> {
    Err(Error::InvalidResponse)
}
