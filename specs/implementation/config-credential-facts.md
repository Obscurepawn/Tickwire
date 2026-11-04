# Credential metadata used by a configuration

`tickwire-config-service::credential_facts` takes the process, host, OS identity, account, purpose and pinned secret reference from a qualified `ProjectionSet`. The result retains the global candidate digest.

The service obtains the credential catalog independently. For each entry it verifies the configured issuer's `CredentialMetadata` signature, scope, version, nonzero fingerprint, enabled status, observation age and exact field set. It takes the UID from the issuer's authorized mapping and checks uniqueness in both directions across objects. Execution and guardian references that alias the same key invalidate the catalog. An additional valid grant contributes no usage unless the projection actually uses it.

The result contains the matched uses and signed metadata. Secret retrieval and deployment authorization belong to their respective owners. Tests cover absent or duplicate records, aliases, UID conflicts, stale observations, signature expiry and exact subject matching.
