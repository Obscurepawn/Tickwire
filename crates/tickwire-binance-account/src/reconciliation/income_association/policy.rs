use super::authority::{ActiveAssociationPolicy, AssociationAuthority};
use super::*;
use crate::reconciliation::income_closure::encoding::{Reader, Writer};
const DOMAIN: &[u8] = b"tickwire.usdm.income-association-policy.v1";

/// Written only from the deployment configuration captured by this input owner.
/// Approval files cannot install a policy or nominate the active certificate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PolicyRecord {
    pub writer: u64,
    pub execution_instance: [u8; 16],
    pub controller_id: [u8; 16],
    pub root: [u8; 32],
    pub revision: u64,
    pub active: Option<ActiveAssociationPolicy>,
}
impl PolicyRecord {
    pub(crate) fn bytes(&self) -> Vec<u8> {
        let mut w = Writer(Vec::new());
        w.array(8);
        w.blob(DOMAIN);
        w.uint(1);
        w.uint(self.writer);
        w.blob(&self.execution_instance);
        w.blob(&self.controller_id);
        w.blob(&self.root);
        w.uint(self.revision);
        match &self.active {
            None => w.array(0),
            Some(a) => {
                w.array(2);
                w.blob(&a.certificate_digest);
                w.uint(a.authorization_revision)
            }
        }
        w.0
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 512 {
            return Err(Error::Capacity);
        }
        let mut r = Reader { bytes, pos: 0 };
        r.array(8)?;
        if r.blob(64)? != DOMAIN || r.uint()? != 1 {
            return Err(Error::Encoding);
        }
        let writer = r.uint()?;
        let execution_instance = r.fixed()?;
        let controller_id = r.fixed()?;
        let root = r.fixed()?;
        let revision = r.uint()?;
        let active = match r.count(2)? {
            0 => None,
            2 => Some(ActiveAssociationPolicy {
                certificate_digest: r.fixed()?,
                authorization_revision: r.uint()?,
            }),
            _ => return Err(Error::Encoding),
        };
        let out = Self {
            writer,
            execution_instance,
            controller_id,
            root,
            revision,
            active,
        };
        if r.pos != bytes.len()
            || out.bytes() != bytes
            || writer == 0
            || execution_instance == [0; 16]
            || controller_id == [0; 16]
            || root == [0; 32]
            || out.active.as_ref().is_some_and(|a| {
                a.authorization_revision != revision
                    || revision == 0
                    || a.certificate_digest == [0; 32]
            })
        {
            return Err(Error::Identity);
        }
        Ok(out)
    }
    pub(super) fn authority(&self) -> AssociationAuthority {
        AssociationAuthority {
            controller_id: self.controller_id,
            public_key: self.root,
            execution_instance: self.execution_instance,
            active_policy: self.active.clone(),
        }
    }
}
impl<S: Store> AccountIngress<S> {
    pub(super) fn last_association_policy(&self, through: usize) -> Option<(usize, &PolicyRecord)> {
        self.records
            .get(..through)?
            .iter()
            .enumerate()
            .rev()
            .find_map(|(i, r)| match r {
                Record::AssociationPolicy(p) => Some((i, p)),
                _ => None,
            })
    }
    pub(crate) fn validate_association_policy(&self, p: &PolicyRecord, writer: u64) -> Result<()> {
        let authority = self
            .config
            .income_association_authority
            .as_ref()
            .ok_or(Error::Identity)?;
        if p.writer != writer
            || p.controller_id != authority.controller_id
            || p.root != authority.public_key
        {
            return Err(Error::Identity);
        }
        PolicyRecord::decode(&p.bytes())?;
        if let Some((_, old)) = self.last_association_policy(self.records.len())
            && (p.writer < old.writer
                || p.revision < old.revision
                || (p.active.is_some() && p.active != old.active && p.revision <= old.revision)
                || p == old)
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub(super) fn sync_association_policy(&mut self) -> Result<u64> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        if self.cursor != self.records.len() {
            return Err(Error::ReplayPending);
        }
        let a = self
            .config
            .income_association_authority
            .as_ref()
            .ok_or(Error::Identity)?;
        a.validate()?;
        let old = self.last_association_policy(self.records.len());
        let revision = a.active_policy.as_ref().map_or_else(
            || old.map_or(0, |(_, p)| p.revision),
            |p| p.authorization_revision,
        );
        let policy = PolicyRecord {
            writer: self.config.writer_epoch,
            execution_instance: a.execution_instance,
            controller_id: a.controller_id,
            root: a.public_key,
            revision,
            active: a.active_policy.clone(),
        };
        if let Some((index, previous)) = old
            && previous == &policy
        {
            return Ok(index as u64 + 1);
        }
        self.validate_association_policy(&policy, self.config.writer_epoch)?;
        let key = self.append(Record::AssociationPolicy(policy))?;
        self.cursor = self.records.len();
        Ok(key.sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconciliation::income_import::tests::{Memory, fixture};
    #[test]
    fn policy_is_durable_and_revoked_leaf_cannot_be_reinstalled_at_same_revision() {
        let (input, _, _, _) = fixture();
        let mut config = input.config.clone();
        config.income_association_authority = Some(AssociationAuthority {
            controller_id: [10; 16],
            public_key: [11; 32],
            execution_instance: [12; 16],
            active_policy: Some(ActiveAssociationPolicy {
                certificate_digest: [13; 32],
                authorization_revision: 3,
            }),
        });
        let mut input = AccountIngress::create(Memory::default(), config.clone()).unwrap();
        assert_eq!(input.sync_association_policy().unwrap(), 2);
        assert_eq!(input.sync_association_policy().unwrap(), 2);
        config
            .income_association_authority
            .as_mut()
            .unwrap()
            .active_policy = None;
        input.config = config.clone();
        assert_eq!(input.sync_association_policy().unwrap(), 3);
        let store = input.journal.into_store();
        let mut reopened = AccountIngress::recover(store, config.clone(), None).unwrap();
        reopened.cursor = reopened.records.len();
        assert_eq!(reopened.sync_association_policy().unwrap(), 3);
        reopened
            .config
            .income_association_authority
            .as_mut()
            .unwrap()
            .active_policy = Some(ActiveAssociationPolicy {
            certificate_digest: [13; 32],
            authorization_revision: 3,
        });
        assert!(reopened.sync_association_policy().is_err());
        reopened
            .config
            .income_association_authority
            .as_mut()
            .unwrap()
            .active_policy = Some(ActiveAssociationPolicy {
            certificate_digest: [14; 32],
            authorization_revision: 4,
        });
        assert_eq!(reopened.sync_association_policy().unwrap(), 4);
        let Record::AssociationPolicy(old) = &reopened.records[1] else {
            panic!("policy")
        };
        assert_eq!(old.active.as_ref().unwrap().certificate_digest, [13; 32]);
        assert_eq!(old.revision, 3);
    }
}
