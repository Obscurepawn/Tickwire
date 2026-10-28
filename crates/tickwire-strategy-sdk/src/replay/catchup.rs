//! Exact retained raw input continuation after source C. Old Shadow callback
//! outputs are never replayed as economic outcomes or queued for publication.
use super::*;
pub struct Continuation {
    inputs: Vec<Input>,
    markets: States,
    market_cursors: Vec<Cursor>,
    accounts: States,
    external: Vec<Cursor>,
}
impl Continuation {
    pub fn inputs(&self) -> &[Input] {
        &self.inputs
    }
    /// Executes the replacement's real callbacks at current local time. The
    /// synchronized trace keeps generated effects; this function has no writer
    /// and consumes every returned command before allowing normal processing.
    pub fn apply<S: crate::Strategy, B: tickwire_recovery::BlobStore>(
        &self,
        engine: &mut durable::DurableEngine<application::Application<S>, B>,
        mut now: impl FnMut() -> Result<u64>,
    ) -> Result<()> {
        let result = (|| {
            for input in &self.inputs {
                engine.apply(input.clone(), now()?)?;
                if let Input::MarketApplied { stream, record, .. } = input {
                    use tickwire_ipc::Wire;
                    let h = tickwire_ipc::RecordHeader::decode(&record[..128])
                        .map_err(|_| Error::Identity)?;
                    let at = now()?;
                    let age = engine.strategy().maximum_age();
                    let view = engine
                        .strategy()
                        .market_view(stream, h.instrument_id)
                        .ok_or(Error::Identity)?;
                    let invoked = view.live()
                        && at
                            .checked_sub(view.received_mono_ns())
                            .is_some_and(|n| n <= age);
                    engine.apply(
                        Input::MarketCallback {
                            stream: stream.clone(),
                            instrument: h.instrument_id,
                            invoked,
                            maximum_age_ns: age,
                        },
                        at,
                    )?;
                }
                // Dropping a committed batch is deliberate suppression, never
                // delivery. No historical effect is retained for later output.
                drop(engine.flush()?);
            }
            let at = now()?;
            engine.poll_timers(at, MAX_TIMERS)?;
            drop(engine.flush()?);
            if engine.next_deadline().is_some_and(|t| t <= at) {
                return Err(Error::Capacity);
            }
            if engine.strategy().market_states()? != self.markets
                || engine.durable_checkpoint().runtime.markets != self.market_cursors
                || engine.strategy().account_states()? != self.accounts
                || engine.durable_checkpoint().runtime.external != self.external
            {
                return Err(Error::StateMismatch);
            }
            Ok(())
        })();
        if result.is_err() {
            engine.stop();
        }
        result
    }
}
struct Opaque(Vec<u8>);
impl crate::Strategy for Opaque {
    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        Ok(self.0.clone())
    }
    fn on_market(
        &mut self,
        _view: crate::MarketView<'_>,
        _now: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        Err(crate::Error::NotReady)
    }
}
fn projection(checkpoint: &Checkpoint) -> Result<application::Application<Opaque>> {
    application::Application::restore(&checkpoint.business, |bytes| Ok(Opaque(bytes.to_vec())))
}
type States = std::collections::BTreeMap<StreamKey, (u64, Vec<u8>)>;
fn check_at_cut(
    actual: &States,
    cut: &States,
    seen: &mut std::collections::BTreeSet<StreamKey>,
) -> Result<()> {
    for (key, (seq, bytes)) in cut {
        if seen.contains(key) {
            continue;
        }
        if let Some((at, state)) = actual.get(key) {
            if at > seq {
                return Err(Error::Sequence);
            }
            if at == seq {
                if state != bytes {
                    return Err(Error::StateMismatch);
                }
                seen.insert(key.clone());
            }
        }
    }
    Ok(())
}
fn check_cursors(states: &States, cursors: &[Cursor]) -> Result<()> {
    if states.len() != cursors.len()
        || cursors.iter().any(|c| {
            states
                .get(&c.stream)
                .is_none_or(|(seq, _)| *seq != c.sequence)
        })
    {
        return Err(Error::Identity);
    }
    Ok(())
}
/// Retention must cover every source-C stream through the current Shadow cut.
/// Account and market bytes at C are compared through their real reducers. A
/// later JOIN with a matching number is not sufficient to establish coverage.
pub fn plan(
    source: &Checkpoint,
    shadow_anchor: &Checkpoint,
    rows: &[MergedInput],
    shadow_current: &Checkpoint,
) -> Result<Continuation> {
    if source.source.clock != shadow_current.source.clock
        || shadow_anchor.source != shadow_current.source
        || rows.len() > MAX_ROWS
    {
        return Err(Error::Identity);
    }
    let models = shadow_anchor
        .runtime
        .models
        .iter()
        .map(|m| (m.id, Object::of(&m.value.bytes)))
        .collect::<Vec<_>>();
    let anchor = verify_anchor(
        &shadow_anchor.bytes()?,
        &shadow_anchor.source,
        Object::of(&shadow_anchor.runtime.config.bytes),
        &models,
    )?;
    verify_tail(&anchor, &encode_tail(&anchor, rows)?, &shadow_current.cut)?;
    let source_views = projection(source)?;
    let end_views = projection(shadow_current)?;
    let mut views = projection(shadow_anchor)?;
    let markets = source_views.market_states()?;
    let accounts = source_views.account_states()?;
    let market_end = end_views.market_states()?;
    let account_end = end_views.account_states()?;
    check_cursors(&markets, &source.runtime.markets)?;
    check_cursors(&accounts, &source.runtime.accounts)?;
    check_cursors(&market_end, &shadow_current.runtime.markets)?;
    check_cursors(&account_end, &shadow_current.runtime.accounts)?;
    let initial_markets = views.market_states()?;
    let initial_accounts = views.account_states()?;
    check_cursors(&initial_markets, &shadow_anchor.runtime.markets)?;
    check_cursors(&initial_accounts, &shadow_anchor.runtime.accounts)?;
    if initial_markets.len() != markets.len()
        || market_end.len() != markets.len()
        || accounts.keys().any(|k| !account_end.contains_key(k))
    {
        return Err(Error::Identity);
    }
    let mut seen_markets = std::collections::BTreeSet::new();
    let mut seen_accounts = std::collections::BTreeSet::new();
    check_at_cut(&initial_markets, &markets, &mut seen_markets)?;
    check_at_cut(&initial_accounts, &accounts, &mut seen_accounts)?;
    let mut external = shadow_anchor.runtime.external.clone();
    let mut seen_external = std::collections::BTreeSet::new();
    for c in &source.runtime.external {
        if let Some(at) = external.iter().find(|a| a.stream == c.stream) {
            if at.sequence > c.sequence {
                return Err(Error::Sequence);
            }
            if at.sequence == c.sequence {
                if at.content_digest != c.content_digest {
                    return Err(Error::StateMismatch);
                }
                seen_external.insert(c.stream.clone());
            }
        }
    }
    let mut inputs = Vec::new();
    let mut context_runtime = shadow_anchor.runtime.clone();
    let market_rows = market_lineage::after_cut(
        &source.runtime.markets,
        &shadow_anchor.runtime.markets,
        rows,
        &shadow_current.runtime.markets,
    )?;
    for (row, market_selected) in rows.iter().zip(market_rows) {
        context_runtime.now_ns = row.now_ns;
        let include = match &row.input {
            Input::MarketApplied { .. } | Input::MarketTransition { .. } => {
                views.apply(
                    &row.input,
                    &mut ReplayContext {
                        views: None,
                        runtime: &mut context_runtime,
                    },
                    &mut ReplayEffects {
                        allowed: false,
                        commands: vec![],
                    },
                )?;
                check_at_cut(&views.market_states()?, &markets, &mut seen_markets)?;
                market_selected
            }
            Input::AccountCallback {
                stream, position, ..
            } => {
                views.apply(
                    &row.input,
                    &mut ReplayContext {
                        views: None,
                        runtime: &mut context_runtime,
                    },
                    &mut ReplayEffects {
                        allowed: false,
                        commands: vec![],
                    },
                )?;
                check_at_cut(&views.account_states()?, &accounts, &mut seen_accounts)?;
                accounts.get(stream).is_none_or(|(c, _)| position > c)
            }
            Input::External {
                stream, sequence, ..
            }
            | Input::Entropy {
                stream, sequence, ..
            } => {
                advance_cursor(
                    &mut external,
                    stream,
                    *sequence,
                    tickwire_recovery::digest(&canonical(&row.input)?),
                    Some(1),
                )?;
                match source.runtime.external.iter().find(|c| c.stream == *stream) {
                    Some(c) => {
                        if *sequence == c.sequence {
                            let at = external
                                .iter()
                                .find(|v| v.stream == *stream)
                                .ok_or(Error::Identity)?;
                            if at.content_digest != c.content_digest {
                                return Err(Error::StateMismatch);
                            }
                            seen_external.insert(stream.clone());
                        }
                        *sequence > c.sequence
                    }
                    None => true,
                }
            }
            // These are effects of the discarded Shadow computation. The new
            // instance regenerates callbacks and due inherited timers at its
            // actual installation time, with all publication suppressed.
            Input::MarketCallback { .. } | Input::TimerFired { .. } => false,
            // A separate mutable Shadow configuration is not silently merged
            // into the release's qualified ConfigApply contract.
            Input::ConfigApply { .. } | Input::ModelApply { .. } => return Err(Error::Unsupported),
        };
        if include {
            inputs.push(row.input.clone());
        }
    }
    if seen_markets.len() != markets.len()
        || seen_accounts.len() != accounts.len()
        || seen_external.len() != source.runtime.external.len()
        || views.market_states()? != market_end
        || views.account_states()? != account_end
        || external != shadow_current.runtime.external
    {
        return Err(Error::StateMismatch);
    }
    Ok(Continuation {
        inputs,
        markets: market_end,
        market_cursors: shadow_current.runtime.markets.clone(),
        accounts: account_end,
        external,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (Checkpoint, Checkpoint, Vec<MergedInput>, Checkpoint) {
        let (bundle, op, member, proof, target, source, config) =
            super::super::install::tests::fixture_with_action(false);
        let release = bundle.verify_operation(&op, &member).unwrap();
        let candidate = super::super::install::prepare(
            &crate::restore::FixedDemoFactory,
            &release,
            target,
            &proof,
            source,
            config,
        )
        .unwrap();
        let (app, start) = candidate.into_parts();
        let anchor = verify_anchor(
            &start.bytes().unwrap(),
            &start.source,
            Object::of(&start.runtime.config.bytes),
            &[],
        )
        .unwrap();
        let mut engine = Engine::new(&anchor, app).unwrap();
        let (_, _, raw) = application::tests::market_fixture();
        engine
            .apply_live(
                Input::MarketApplied {
                    stream: start.runtime.markets[0].stream.clone(),
                    sequence: 1001,
                    received_ns: 1_600_000_000,
                    record: raw,
                    payload: vec![],
                },
                1_600_000_001,
            )
            .unwrap();
        let effect = engine
            .apply_live(
                Input::MarketCallback {
                    stream: start.runtime.markets[0].stream.clone(),
                    instrument: 1001,
                    invoked: true,
                    maximum_age_ns: 1_000_000_000,
                },
                1_600_000_002,
            )
            .unwrap();
        assert_eq!(effect.commands.len(), 1);
        (
            proof.current().clone(),
            start,
            engine.recorded().to_vec(),
            engine.checkpoint().unwrap(),
        )
    }
    #[test]
    fn continuation_retains_raw_post_c_records_but_not_shadow_commands_or_callback_flags() {
        let (c, a, rows, d) = fixture();
        let continuation = plan(&c, &a, &rows, &d).unwrap();
        assert_eq!(continuation.inputs(), &[rows[0].input.clone()]);
    }
    #[test]
    fn latest_join_cannot_replace_missing_retained_prefix() {
        let (c, _, _, d) = fixture();
        assert!(plan(&c, &d, &[], &d).is_err());
    }
    #[test]
    fn exact_source_and_shadow_cursor_digests_cannot_be_replaced_by_equal_views() {
        let (c, a, rows, d) = fixture();
        for source_changed in [true, false] {
            let mut changed = if source_changed { c.clone() } else { d.clone() };
            changed.runtime.markets[0].content_digest[0] ^= 1;
            let changed = Checkpoint::new(
                changed.source,
                changed.cut.sequence,
                changed.cut.chain,
                changed.business,
                changed.runtime,
            )
            .unwrap();
            let (source, end) = if source_changed {
                (&changed, &d)
            } else {
                (&c, &changed)
            };
            assert!(
                plan(source, &a, &rows, end).is_err(),
                "source_changed={source_changed}"
            );
        }
    }
    #[test]
    fn continuation_runs_changed_target_state_and_suppresses_output_before_future_callbacks() {
        let (c, a, rows, d) = fixture();
        let plan = plan(&c, &a, &rows, &d).unwrap();
        let (bundle, op, member, proof, target, source, config) =
            super::super::install::tests::fixture_with_action(false);
        let release = bundle.verify_operation(&op, &member).unwrap();
        let candidate = super::super::install::prepare(
            &crate::restore::FixedDemoFactory,
            &release,
            target,
            &proof,
            source,
            config,
        )
        .unwrap();
        let (app, checkpoint) = candidate.into_parts();
        assert_eq!(app.business_state().unwrap()[8], 0);
        let anchor = verify_anchor(
            &checkpoint.bytes().unwrap(),
            &checkpoint.source,
            Object::of(&checkpoint.runtime.config.bytes),
            &[],
        )
        .unwrap();
        let root = std::env::temp_dir().join(format!(
            "tickwire-catchup-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = tickwire_recovery::FileBlobStore::open(&root).unwrap();
        let mut engine = durable::DurableEngine::new(anchor, app, store).unwrap();
        plan.apply(&mut engine, || Ok(1_700_000_000)).unwrap();
        assert_eq!(engine.strategy().business_state().unwrap()[8], 1);
        assert_eq!(engine.durable_checkpoint().runtime.effects.count, 1);
        assert_eq!(
            engine
                .strategy()
                .market_view(&checkpoint.runtime.markets[0].stream, 1001)
                .unwrap()
                .received_mono_ns(),
            1_600_000_000
        );
        let mut published = 0;
        engine
            .flush()
            .unwrap()
            .publish(|_| {
                published += 1;
                Ok(())
            })
            .unwrap();
        engine
            .apply(
                Input::MarketCallback {
                    stream: checkpoint.runtime.markets[0].stream.clone(),
                    instrument: 1001,
                    invoked: true,
                    maximum_age_ns: 1_000_000_000,
                },
                1_700_000_001,
            )
            .unwrap();
        engine
            .flush()
            .unwrap()
            .publish(|_| {
                published += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(published, 0);
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }
}
