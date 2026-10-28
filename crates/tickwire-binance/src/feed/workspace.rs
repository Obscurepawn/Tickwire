use super::*;

/// Reusable bounded storage for the combined bids and asks of one depth update.
#[derive(Debug)]
pub struct DepthWorkspace {
    levels: Vec<Level>,
    maximum: usize,
}
impl DepthWorkspace {
    pub fn capacity(&self) -> usize {
        self.maximum
    }
    pub fn reserved_bytes(&self) -> usize {
        self.levels.capacity() * std::mem::size_of::<Level>()
    }
    pub fn new(max_total_levels: usize) -> Result<Self> {
        if max_total_levels == 0 || max_total_levels > MAX_DEPTH_LEVELS {
            return Err(Error::Capacity);
        }
        let mut levels = Vec::new();
        levels
            .try_reserve_exact(max_total_levels)
            .map_err(|_| Error::Capacity)?;
        Ok(Self {
            levels,
            maximum: max_total_levels,
        })
    }
}

/// An update borrowed from a workspace until the next exclusive decode.
#[derive(Debug)]
pub struct DepthUpdateRef<'a> {
    event: DepthEvent<'a>,
    event_time_ns: Option<u64>,
    transaction_time_ns: Option<u64>,
}
impl DepthUpdateRef<'_> {
    pub fn event(&self) -> DepthEvent<'_> {
        self.event
    }
    pub fn event_time_ns(&self) -> Option<u64> {
        self.event_time_ns
    }
    pub fn transaction_time_ns(&self) -> Option<u64> {
        self.transaction_time_ns
    }
}

pub fn decode_diff_into<'a>(
    bytes: &[u8],
    context: &DepthContext,
    received: u64,
    max_bytes: usize,
    workspace: &'a mut DepthWorkspace,
) -> Result<DepthUpdateRef<'a>> {
    check_input_limit(workspace.maximum, bytes, max_bytes)?;
    decode_into(context, received, workspace, |output| {
        decode_json_with(bytes, context, |side, level| output.push(side, level))
    })
}

pub fn decode_sbe_diff_into<'a>(
    bytes: &[u8],
    context: &DepthContext,
    received: u64,
    max_bytes: usize,
    workspace: &'a mut DepthWorkspace,
) -> Result<DepthUpdateRef<'a>> {
    check_input_limit(workspace.maximum, bytes, max_bytes)?;
    decode_into(context, received, workspace, |output| {
        sbe::decode_with(bytes, context, max_bytes, output.maximum, |side, level| {
            output.push(side, level)
        })
    })
}
struct Output<'a> {
    levels: &'a mut Vec<Level>,
    maximum: usize,
    bids: usize,
}
impl Output<'_> {
    fn push(&mut self, side: Side, level: Level) -> Result<()> {
        if self.levels.len() == self.maximum {
            return Err(Error::Capacity);
        }
        if side == Side::Bid {
            self.bids += 1;
        }
        self.levels.push(level);
        Ok(())
    }
}
fn decode_into<'a>(
    context: &DepthContext,
    received: u64,
    workspace: &'a mut DepthWorkspace,
    decode: impl FnOnce(&mut Output<'_>) -> Result<DepthFields>,
) -> Result<DepthUpdateRef<'a>> {
    workspace.levels.clear();
    let mut output = Output {
        levels: &mut workspace.levels,
        maximum: workspace.maximum,
        bids: 0,
    };
    let fields = decode(&mut output)?;
    let bids = output.bids;
    let (bids, asks) = workspace.levels.split_at(bids);
    Ok(DepthUpdateRef {
        event: DepthEvent {
            key: context.key,
            generation: context.generation,
            first_update_id: fields.first,
            last_update_id: fields.last,
            previous_update_id: fields.previous,
            received_at_ns: received,
            bids,
            asks,
        },
        event_time_ns: fields.event_time_ns,
        transaction_time_ns: fields.transaction_time_ns,
    })
}
