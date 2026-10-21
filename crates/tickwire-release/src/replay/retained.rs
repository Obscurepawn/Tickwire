//! Complete merged inputs retained in the existing synchronized recovery closure.
use super::*;
pub struct RecordedCut {
    anchor: VerifiedAnchor,
    rows: Vec<MergedInput>,
    current: Checkpoint,
}
impl RecordedCut {
    pub fn current(&self) -> &Checkpoint {
        &self.current
    }
    pub fn anchor(&self) -> &VerifiedAnchor {
        &self.anchor
    }
    pub fn tail_from(&self, base: &Checkpoint) -> Result<Vec<u8>> {
        if base.source != self.current.source
            || base.cut.sequence < self.anchor.checkpoint().cut.sequence
            || base.cut.sequence > self.current.cut.sequence
        {
            return Err(Error::Identity);
        }
        let index = usize::try_from(base.cut.sequence - self.anchor.checkpoint().cut.sequence)
            .map_err(|_| Error::Bounds)?;
        let models = base
            .runtime
            .models
            .iter()
            .map(|m| (m.id, Object::of(&m.value.bytes)))
            .collect::<Vec<_>>();
        let anchor = verify_anchor(
            &base.bytes()?,
            &base.source,
            Object::of(&base.runtime.config.bytes),
            &models,
        )?;
        let bytes = encode_tail(&anchor, self.rows.get(index..).ok_or(Error::Identity)?)?;
        verify_tail(&anchor, &bytes, &self.current.cut)?;
        Ok(bytes)
    }
}
/// Requires an explicit v2 completion mark after the last merged input. Legacy
/// inputs without that mark do not gain complete-tail replay capability.
pub fn decode(bytes: &[u8]) -> Result<RecordedCut> {
    if bytes.len() > MAX_BYTES {
        return Err(Error::Bounds);
    }
    tickwire_recovery::EncodedInputs::decode(bytes.to_vec()).map_err(|_| Error::Identity)?;
    let mut position = 68;
    let mut anchor = None;
    let mut rows = vec![];
    let mut current = None;
    while position < bytes.len() {
        let header = bytes.get(position..position + 13).ok_or(Error::Identity)?;
        let kind = header[8];
        let n = u32::from_be_bytes(header[9..13].try_into().map_err(|_| Error::Identity)?) as usize;
        position += 13;
        let payload = bytes
            .get(position..position.checked_add(n).ok_or(Error::Bounds)?)
            .ok_or(Error::Identity)?;
        position += n;
        if kind != 5 {
            continue;
        }
        let tag = payload.get(..8).ok_or(Error::Identity)?;
        let value = &payload[8..];
        match tag {
            b"TWM2A001" => {
                if anchor.is_some() {
                    return Err(Error::Identity);
                }
                let c = Checkpoint::decode(value)?;
                let models = c
                    .runtime
                    .models
                    .iter()
                    .map(|m| (m.id, Object::of(&m.value.bytes)))
                    .collect::<Vec<_>>();
                anchor = Some(verify_anchor(
                    value,
                    &c.source,
                    Object::of(&c.runtime.config.bytes),
                    &models,
                )?);
            }
            b"TWM2R001" => {
                if anchor.is_none() || rows.len() == MAX_ROWS {
                    return Err(Error::Bounds);
                }
                let row: MergedInput =
                    serde_json::from_slice(value).map_err(|_| Error::Identity)?;
                if canonical(&row)? != value {
                    return Err(Error::Identity);
                }
                row.validate()?;
                rows.push(row);
                current = None;
            }
            b"TWM2C001" => {
                let a = anchor.as_ref().ok_or(Error::Identity)?;
                let c = Checkpoint::decode(value)?;
                if c.source != a.checkpoint().source {
                    return Err(Error::Identity);
                }
                verify_tail(a, &encode_tail(a, &rows)?, &c.cut)?;
                current = Some(c);
            }
            _ => return Err(Error::Unsupported),
        }
    }
    Ok(RecordedCut {
        anchor: anchor.ok_or(Error::Unsupported)?,
        rows,
        current: current.ok_or(Error::Unsupported)?,
    })
}
