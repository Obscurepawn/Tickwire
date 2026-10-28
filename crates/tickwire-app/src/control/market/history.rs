use super::*;
#[derive(Clone, Default)]
pub(in crate::control) struct History {
    operations: BTreeMap<(Consumer, [u8; 16]), (Decision, Option<Decision>)>,
}
impl History {
    pub fn len(&self) -> usize {
        self.operations.len()
    }
    pub fn contains(&self, decision: &Decision) -> Result<bool> {
        let Some((arm, commit)) = self.operations.get(&(decision.consumer, decision.id())) else {
            return Ok(false);
        };
        let old = match decision.action {
            DecisionAction::Arm { .. } => Some(arm),
            DecisionAction::Commit { .. } => commit.as_ref(),
        };
        let Some(old) = old else { return Ok(false) };
        if serde_json::to_vec(old)? != serde_json::to_vec(decision)? {
            return Err(invalid("market decision conflict"));
        }
        Ok(true)
    }
    pub fn check(&self, decision: &Decision, now: MonoTime, maximum: usize) -> Result<()> {
        let bytes = serde_json::to_vec(decision)?;
        if bytes.len() > MAX_CONTROL_BYTES + 2048
            || decision.consumer.pid == 0
            || decision.consumer.instance == [0; 16]
            || decision.consumer.generation == 0
        {
            return Err(invalid("market decision size or consumer"));
        }
        match &decision.action {
            DecisionAction::Arm { approval } => {
                approval.validate(&approval.previous, now)?;
                if !decision.consumer.matches(&approval.previous)
                    || approval.previous.domain()?
                        != decision
                            .binding
                            .domain
                            .build()
                            .map_err(super::super::debug_error)?
                    || (approval.previous.endpoint.local.role == 4
                        && (decision.consumer.instance != decision.binding.executor_id
                            || decision.consumer.generation != decision.binding.owner_generation))
                {
                    return Err(invalid("market decision actual identity"));
                }
                if !self
                    .operations
                    .contains_key(&(decision.consumer, decision.id()))
                    && self.len() >= maximum
                {
                    return Err(invalid("market decision capacity"));
                }
            }
            DecisionAction::Commit {
                commit,
                previous_applied,
            } => {
                let (arm, _) = self
                    .operations
                    .get(&(decision.consumer, decision.id()))
                    .ok_or_else(|| invalid("market commit before approved arm"))?;
                let DecisionAction::Arm { approval } = &arm.action else {
                    return Err(invalid("market arm history"));
                };
                if serde_json::to_vec(&arm.binding)? != serde_json::to_vec(&decision.binding)? {
                    return Err(invalid("market decision owner changed"));
                }
                commit.validate(approval, *previous_applied, now)?;
            }
        }
        self.contains(decision)?;
        Ok(())
    }
    pub fn record(&mut self, decision: Decision, now: MonoTime, maximum: usize) -> Result<()> {
        self.check(&decision, now, maximum)?;
        let key = (decision.consumer, decision.id());
        match decision.action {
            DecisionAction::Arm { .. } => {
                self.operations.entry(key).or_insert((decision, None));
            }
            DecisionAction::Commit { .. } => {
                self.operations
                    .get_mut(&key)
                    .ok_or_else(|| invalid("market arm history"))?
                    .1 = Some(decision);
            }
        }
        Ok(())
    }
}
