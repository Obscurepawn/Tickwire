use super::*;
use tickwire_account::Account;
use tickwire_account_codec::CanonicalBaseline;
use tickwire_spot_execution::private::stream::{EventKind, StreamReader};

pub(super) fn evidence(
    r: &StreamReader,
    baseline: &CanonicalBaseline,
    account: &Account,
    b: &Reactivation,
    previous: Option<&Reactivation>,
) -> Result<[u8; 32]> {
    let after = previous.map_or(0, |p| p.through_sequence);
    let mut ready_epoch = previous.map(|p| p.private_connection_epoch);
    if r.config().scope != b.scope
        || r.config().stream_id != b.input_stream
        || b.gap_generation != b.private_connection_epoch
        || after > b.through_sequence
        || b.through_sequence - after > 250_000
    {
        return Err(Error::Origin("private coverage scope or bound invalid"));
    }
    let mut h = ring::digest::Context::new(&ring::digest::SHA256);
    let mut cursor = after.min(
        b.through_sequence
            .checked_sub(1)
            .ok_or(Error::Origin("invalid private frontier"))?,
    );
    while cursor < b.through_sequence {
        let n = (b.through_sequence - cursor).min(256) as usize;
        let chunk = r
            .drain(cursor, n)
            .map_err(|_| Error::Origin("private input archive incomplete"))?;
        if chunk.is_empty() {
            return Err(Error::Origin(
                "private input archive ends before claimed cut",
            ));
        }
        for event in chunk {
            cursor += 1;
            if event.sequence != cursor {
                return Err(Error::Origin("private input archive skipped a row"));
            }
            if event.epoch == b.private_connection_epoch && event.received_ns > b.evaluated_at {
                return Err(Error::Origin(
                    "private input arrived after the proof evaluation time",
                ));
            }
            h.update(&event.epoch.to_be_bytes());
            h.update(&event.sequence.to_be_bytes());
            h.update(&event.received_ns.to_be_bytes());
            match event.kind {
                EventKind::Started { boot_id } => {
                    ready_epoch = None;
                    h.update(b"S");
                    h.update(&boot_id);
                    if event.epoch == b.private_connection_epoch && boot_id != b.clock_boot_id {
                        return Err(Error::Origin("private boot identity differs"));
                    }
                }
                EventKind::Gap { reason } => {
                    ready_epoch = None;
                    h.update(b"G");
                    h.update(&tickwire_recovery::digest(reason.as_bytes()));
                }
                EventKind::Ready => {
                    ready_epoch = Some(event.epoch);
                    h.update(b"R");
                }
                EventKind::Data { channel, row } => {
                    compare::private_row(baseline, account, channel, &row)?;
                    h.update(b"D");
                    h.update(&[channel as u8]);
                    h.update(&tickwire_recovery::digest(
                        &serde_json::to_vec(&row)
                            .map_err(|_| Error::Origin("invalid private row"))?,
                    ));
                }
            }
        }
    }
    if ready_epoch != Some(b.private_connection_epoch) {
        return Err(Error::Origin(
            "private cut precedes subscription readiness or follows a gap",
        ));
    }
    Ok(h.finish().as_ref().try_into().unwrap())
}
