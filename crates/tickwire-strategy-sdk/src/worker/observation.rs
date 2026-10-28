//! One continuous-state query. It owns no trading gates and never pauses inputs.
use crate::{Error, Result, checkpoint::OBSERVE_CURRENT};
use tickwire_ipc::{CheckpointSealBody, ControlEnvelope};

pub(super) struct Pending {
    request: ControlEnvelope,
    seal: CheckpointSealBody,
    response: Option<Vec<u8>>,
}

impl Pending {
    pub(super) fn expired(&self, now: u64) -> bool {
        now >= self.seal.valid_until_mono_ns
    }
    pub(super) fn new(request: ControlEnvelope, seal: CheckpointSealBody) -> Result<Self> {
        if seal.seal_mode != OBSERVE_CURRENT {
            return Err(Error::Identity);
        }
        Ok(Self {
            request,
            seal,
            response: None,
        })
    }

    /// Returns true when the response was sent or its deadline expired. A false
    /// result leaves ordinary input processing free to continue on this iteration.
    pub(super) fn drive(
        &mut self,
        position: Option<u64>,
        mut now: impl FnMut() -> Result<u64>,
        checkpoint: impl FnOnce(&ControlEnvelope, &CheckpointSealBody) -> Result<Vec<u8>>,
        send: impl FnOnce(&[u8]) -> Result<bool>,
    ) -> Result<bool> {
        if self.expired(now()?) {
            return Ok(true);
        }
        if self.response.is_none() {
            if position.is_none_or(|position| position < self.seal.report_sequence) {
                return Ok(false);
            }
            self.response = Some(checkpoint(&self.request, &self.seal)?);
        }
        // Durable checkpoint work can cross the original deadline. Never send
        // its now-stale response, and never recalculate it merely for backpressure.
        if self.expired(now()?) {
            return Ok(true);
        }
        send(self.response.as_ref().ok_or(Error::NotReady)?)
    }
}

/// A latest-state announcement may be coalesced, but cannot overtake a queued
/// request response or the READY bound to an installation/rollback response.
pub(super) fn send_announcement(
    pending: &mut Option<Vec<u8>>,
    ordered_response_pending: bool,
    send: impl FnOnce(&[u8]) -> Result<bool>,
) -> Result<()> {
    if ordered_response_pending {
        return Ok(());
    }
    if let Some(frame) = pending
        && send(frame)?
    {
        *pending = None;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn periodic_announcement_waits_for_ordered_responses_and_preserves_latest_on_backpressure() {
        let mut latest = Some(vec![10]);
        send_announcement(&mut latest, true, |_| {
            panic!("request response is still queued")
        })
        .unwrap();
        // Inputs can continue while control responses are blocked.
        latest = Some(vec![11]);
        send_announcement(&mut latest, true, |_| {
            panic!("installation READY is still queued")
        })
        .unwrap();
        send_announcement(&mut latest, false, |bytes| {
            assert_eq!(bytes, &[11]);
            Ok(false)
        })
        .unwrap();
        send_announcement(&mut latest, false, |bytes| {
            assert_eq!(bytes, &[11]);
            Ok(true)
        })
        .unwrap();
        send_announcement(&mut latest, false, |_| panic!("already delivered")).unwrap();
    }

    fn pending() -> Pending {
        Pending::new(
            ControlEnvelope::default(),
            CheckpointSealBody {
                report_sequence: 4,
                valid_until_mono_ns: 100,
                seal_mode: OBSERVE_CURRENT,
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn query_waits_for_a_real_prefix_and_keeps_one_response_under_backpressure() {
        let mut query = pending();
        for position in [None, Some(0), Some(3)] {
            assert!(
                !query
                    .drive(
                        position,
                        || Ok(1),
                        |_, _| panic!("no complete report prefix"),
                        |_| panic!("no response exists"),
                    )
                    .unwrap()
            );
        }
        assert!(
            !query
                .drive(
                    Some(5),
                    || Ok(2),
                    |_, body| {
                        assert_eq!(body.report_sequence, 4);
                        Ok(vec![5])
                    },
                    |bytes| {
                        assert_eq!(bytes, &[5]);
                        Ok(false)
                    },
                )
                .unwrap()
        );
        assert!(
            query
                .drive(
                    Some(20),
                    || Ok(3),
                    |_, _| panic!("must not execute another checkpoint"),
                    |bytes| {
                        assert_eq!(bytes, &[5]);
                        Ok(true)
                    },
                )
                .unwrap()
        );
    }

    #[test]
    fn query_expiry_before_or_during_checkpoint_or_send_drops_only_the_query() {
        let mut query = pending();
        assert!(
            query
                .drive(
                    None,
                    || Ok(100),
                    |_, _| panic!("expired"),
                    |_| panic!("expired"),
                )
                .unwrap()
        );
        let now = Cell::new(10);
        let mut query = pending();
        assert!(
            query
                .drive(
                    Some(4),
                    || Ok(now.get()),
                    |_, _| {
                        now.set(100);
                        Ok(vec![1])
                    },
                    |_| panic!("checkpoint crossed deadline"),
                )
                .unwrap()
        );
        let mut query = pending();
        assert!(
            !query
                .drive(Some(4), || Ok(1), |_, _| Ok(vec![1]), |_| Ok(false))
                .unwrap()
        );
        assert!(
            query
                .drive(
                    Some(8),
                    || Ok(100),
                    |_, _| panic!("already has a checkpoint"),
                    |_| panic!("expired while backpressured"),
                )
                .unwrap()
        );
    }
}
