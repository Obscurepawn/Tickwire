//! Structural selection of retained market inputs after a source cut. The SDK
//! additionally compares materialized views; these bytes do not authorize JOIN.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// One flag per merged row. Transitions replace one stream, retaining its
/// lineage; a snapshot that jumps past the source cut cannot prove coverage.
pub fn after_cut(
    source: &[Cursor],
    initial: &[Cursor],
    rows: &[MergedInput],
    end: &[Cursor],
) -> Result<Vec<bool>> {
    if source.len() != initial.len()
        || end.len() != initial.len()
        || source.len() > MAX_STREAMS
        || rows.len() > MAX_ROWS
    {
        return Err(Error::Identity);
    }
    let mut seen = BTreeSet::new();
    let mut known = BTreeSet::new();
    let mut active = BTreeMap::new();
    let mut reached = |cursor: &Cursor, inherited: bool| -> Result<bool> {
        if let Some(cut) = source.iter().find(|c| c.stream == cursor.stream) {
            if inherited || cursor.sequence > cut.sequence {
                return Err(Error::Identity);
            }
            if cursor.sequence == cut.sequence {
                if cursor != cut || !seen.insert(cursor.stream.clone()) {
                    return Err(Error::Identity);
                }
                return Ok(true);
            }
        }
        Ok(inherited)
    };
    for cursor in initial {
        if !known.insert(cursor.stream.clone()) {
            return Err(Error::Identity);
        }
        active.insert(
            cursor.stream.clone(),
            (cursor.clone(), reached(cursor, false)?),
        );
    }
    let mut selected = Vec::with_capacity(rows.len());
    for row in rows {
        row.validate()?;
        let include = match &row.input {
            Input::MarketApplied {
                stream, sequence, ..
            } => {
                let state = active.get_mut(stream).ok_or(Error::Identity)?;
                if state.0.sequence.checked_add(1) != Some(*sequence) {
                    return Err(Error::Identity);
                }
                let include = state.1;
                state.0.sequence = *sequence;
                state.0.content_digest = market_cursor_digest(&row.input)?;
                if !state.1 {
                    state.1 = reached(&state.0, false)?;
                }
                include
            }
            Input::MarketTransition {
                previous,
                next,
                activated_sequence,
                ..
            } => {
                let (cursor, inherited) = active.remove(&previous.stream).ok_or(Error::Identity)?;
                if cursor != *previous || !known.insert(next.clone()) {
                    return Err(Error::Identity);
                }
                let cursor = Cursor {
                    stream: next.clone(),
                    sequence: *activated_sequence,
                    content_digest: market_cursor_digest(&row.input)?,
                };
                let matched = reached(&cursor, inherited)?;
                active.insert(next.clone(), (cursor, matched));
                inherited
            }
            Input::MarketCallback { stream, .. } => {
                if !active.contains_key(stream) {
                    return Err(Error::Identity);
                }
                false
            }
            _ => false,
        };
        selected.push(include);
    }
    if seen.len() != source.len()
        || active.len() != end.len()
        || end.iter().any(|c| {
            active
                .get(&c.stream)
                .is_none_or(|(actual, matched)| actual != c || !matched)
        })
    {
        return Err(Error::Identity);
    }
    Ok(selected)
}
