//! One approved transition; old consumer ownership remains in the worker until durability succeeds.
use super::*;
use tickwire_market_source::transition::{Approval, Commit, Phase, Receipt};
use tickwire_release::Object;
pub(super) struct Expansion {
    pub approval: Approval,
    pub arm: tickwire_ipc::ReplayControlBody,
    pub armed: Object,
    pub receipt: Receipt,
    pub quiesced: Option<Object>,
    pub commit: Option<(Commit, tickwire_ipc::ReplayControlBody)>,
    pub activated: Option<Object>,
    pub pending: Option<tickwire_market_source::PendingJoin>,
    pub candidate: Option<tickwire_market_source::Client>,
    pub market_reply: Option<Vec<u8>>,
}
impl Expansion {
    pub fn active(&self) -> bool {
        self.receipt.phase != Phase::Activated
    }
    pub fn paused(&self) -> bool {
        self.receipt.phase == Phase::Quiesced
    }
    pub fn previous_applied(&self) -> Result<u64> {
        self.receipt.previous_applied.ok_or(Error::NotReady)
    }
}
pub(super) fn queue(queue: &mut std::collections::VecDeque<Vec<u8>>, bytes: Vec<u8>) -> Result<()> {
    if queue.len() >= 8 {
        return Err(Error::Bounds);
    }
    queue.push_back(bytes);
    Ok(())
}

pub(super) fn arm_rejection(
    approval: &Approval,
    current: Option<&tickwire_market_source::ConnectConfig>,
    supports_replay: bool,
    busy: bool,
    now: tickwire_clock::MonoTime,
) -> Option<u16> {
    let Some(current) = current else {
        return Some(transition_control::UNSUPPORTED);
    };
    if !supports_replay {
        Some(transition_control::UNSUPPORTED)
    } else if busy {
        Some(transition_control::BUSY)
    } else if approval.validate(current, now).is_err() {
        Some(transition_control::INVALID)
    } else {
        None
    }
}
#[cfg(test)]
use crate::transition_fixture as fixture;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arm_rejects_legacy_busy_and_changed_subscription_before_touching_the_cursor() {
        let approval = fixture::approval();
        let now = tickwire_clock::MonoTime::new(approval.previous.domain().unwrap(), fixture::NOW);
        assert_eq!(
            arm_rejection(&approval, Some(&approval.previous), true, false, now),
            None
        );
        assert_eq!(
            arm_rejection(&approval, Some(&approval.previous), false, false, now),
            Some(transition_control::UNSUPPORTED)
        );
        assert_eq!(
            arm_rejection(&approval, None, true, false, now),
            Some(transition_control::UNSUPPORTED)
        );
        assert_eq!(
            arm_rejection(&approval, Some(&approval.previous), true, true, now),
            Some(transition_control::BUSY)
        );
        let mut current = approval.previous.clone();
        current.cursor_id = [99; 16];
        assert_eq!(
            arm_rejection(&approval, Some(&current), true, false, now),
            Some(transition_control::INVALID)
        );
    }
}

pub(super) fn poll_frontier(
    client: &mut tickwire_market_source::Client,
    now: tickwire_clock::MonoTime,
    deadline: u64,
    maximum_age: u64,
) -> Result<bool> {
    if now.domain() != client.domain
        || now.nanos() < client.baseline_now
        || now.nanos() >= deadline
        || now.nanos() >= client.session.deadline()
    {
        return Err(Error::Deadline);
    }
    if client.consumer.watermarks().applied != client.replica.sequence() {
        return Err(Error::Identity);
    }
    match client.connection.try_recv() {
        Ok(Some(packet)) => {
            if !packet.fds().is_empty() {
                return Err(Error::Identity);
            }
            client.session.validate(packet.bytes(), now.nanos())?;
            return Err(Error::NotReady);
        }
        Ok(None) => {}
        Err(tickwire_control_sys::Error::System { source, .. })
            if source.kind() == std::io::ErrorKind::Interrupted =>
        {
            return Ok(false);
        }
        Err(error) => return Err(error.into()),
    }
    if !client.catchup.is_empty() {
        healthy_frontier(client, now.nanos(), maximum_age)?;
        return Ok(true);
    }
    let Some(guard) = client.consumer.borrow()? else {
        return Ok(false);
    };
    let header = tickwire_ipc::RecordHeader::decode(&guard.bytes()[..128])?;
    if header.ingress_mono_ns > now.nanos() {
        return Err(Error::Stale);
    }
    let payload = if header.message_type == 259 {
        guard.depth_payload()?
    } else {
        &[]
    };
    client
        .baseline_bytes
        .len()
        .checked_add(512)
        .and_then(|n| n.checked_add(payload.len()))
        .filter(|n| *n <= tickwire_release::replay::MAX_BYTES)
        .ok_or(Error::Bounds)?;
    let raw = guard.bytes().to_vec();
    let payload = payload.to_vec();
    let sequence = guard.sequence();
    drop(guard);
    client.catchup.try_reserve(1).map_err(|_| Error::Bounds)?;
    let instrument = client.replica.apply(&raw, &payload)?;
    client.consumer.apply(sequence)?;
    client
        .catchup
        .push(tickwire_market_source::shm::CatchupRecord {
            record: raw,
            payload,
            instrument,
            applied_mono_ns: now.nanos(),
        });
    healthy_frontier(client, now.nanos(), maximum_age)?;
    Ok(true)
}
fn healthy_frontier(
    client: &tickwire_market_source::Client,
    now: u64,
    maximum_age: u64,
) -> Result<()> {
    if maximum_age == 0
        || !client.replica.instruments().all(|id| {
            client.replica.view(id).is_some_and(|v| {
                v.continuous()
                    && v.live()
                    && v.published_mono_ns() <= now
                    && now
                        .checked_sub(v.received_mono_ns())
                        .is_some_and(|age| age <= maximum_age)
            })
        })
    {
        return Err(Error::NotReady);
    }
    Ok(())
}
#[cfg(test)]
mod frontier_tests;
