use crate::*;

#[derive(Clone)]
pub(crate) enum Record {
    Config(Vec<u8>),
    Income(crate::reconciliation::income::IncomeRecord),
    IncomeImport(crate::reconciliation::income_import::ImportRecord),
    IncomeEffect {
        response: u64,
        index: u8,
    },
    Register(CanonicalCommand, Vec<[u8; 16]>),
    Event(SourceMeta, Vec<u8>, Decision),
    Rest(crate::rest::RestRecord),
    Snapshot(crate::reconciliation::bootstrap::SnapshotRecord),
    RestEffect {
        response: u64,
        index: u8,
    },
    BeginReconciliation {
        epoch: u64,
        generation: u64,
    },
    Gap {
        epoch: u64,
        generation: u64,
        reason: u8,
    },
}
pub(crate) fn config(config: &Config) -> Vec<u8> {
    let mut bytes = b"tickwire.account-input.config.v1".to_vec();
    bytes.extend(config.baseline.digest());
    bytes.extend(config.account_stream);
    bytes.extend(config.physical.stream_epoch);
    bytes.extend(config.physical.domain);
    bytes.extend(config.physical.segment_max_bytes.to_le_bytes());
    for value in [
        config.max_records,
        config.max_registrations,
        config.max_raw_bytes,
        config.max_retained_bytes,
    ] {
        bytes.extend((value as u64).to_le_bytes());
    }
    if let Some(authority) = &config.income_baseline_authority {
        bytes.extend(b"income-risk-admin.v1");
        bytes.extend(authority.controller_id);
        bytes.extend(authority.public_key);
    }
    bytes
}
fn blob(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if out
        .len()
        .checked_add(bytes.len())
        .and_then(|n| n.checked_add(4))
        .is_none_or(|n| n > MAX_RECORD_BYTES)
    {
        return Err(Error::Capacity);
    }
    out.extend((bytes.len() as u32).to_le_bytes());
    out.extend(bytes);
    Ok(())
}
pub(crate) fn encode(record: &Record) -> Result<Vec<u8>> {
    let mut out = b"TWAI0001".to_vec();
    match record {
        Record::IncomeImport(record) => {
            out.push(13);
            out.extend(record.writer.to_le_bytes());
            out.extend(record.accepted.to_le_bytes());
            blob(&mut out, record.proposal.bytes())?;
            blob(&mut out, record.approval.payload())?;
            out.extend(record.approval.signature());
        }
        Record::Income(record) => {
            out.push(11);
            for n in [
                record.query.start_time_ms,
                record.query.end_time_ms,
                record.query.private_connection_epoch,
                record.query.gap_generation,
                record.epoch,
                record.received,
                record.clock_generation,
            ] {
                out.extend(n.to_le_bytes());
            }
            out.extend(record.query.limit.to_le_bytes());
            out.push(match record.query.purpose {
                crate::reconciliation::income::IncomeUse::BaselineCoverage => 1,
                crate::reconciliation::income::IncomeUse::Account => 2,
            });
            out.extend(record.clock_boot_id);
            out.extend(record.status.to_le_bytes());
            out.push(u8::from(record.accepted));
            blob(&mut out, &record.raw)?;
            out.push(record.rows.len() as u8);
            for row in &record.rows {
                blob(&mut out, row.bytes())?;
            }
        }
        Record::IncomeEffect { response, index } => {
            out.push(12);
            out.extend(response.to_le_bytes());
            out.push(*index);
        }
        Record::Config(bytes) => {
            out.push(1);
            blob(&mut out, bytes)?;
        }
        Record::Register(command, ids) => {
            out.push(2);
            blob(&mut out, command.bytes())?;
            out.push(ids.len() as u8);
            for id in ids {
                out.extend(id);
            }
        }
        Record::Event(meta, raw, decision) => {
            out.push(3);
            out.extend(meta.epoch.to_le_bytes());
            out.push(u8::from(meta.subscription.is_some()));
            out.extend(meta.subscription.unwrap_or(0).to_le_bytes());
            out.push(u8::from(meta.micros));
            out.extend(meta.received.to_le_bytes());
            blob(&mut out, raw)?;
            match decision {
                Decision::Bound(event) => {
                    out.push(1);
                    blob(&mut out, event.bytes())?;
                }
                Decision::AccountBound(event) => {
                    out.push(3);
                    blob(&mut out, event.bytes())?;
                }
                Decision::Quarantine(reason) => {
                    out.push(2);
                    out.push(reason_code(*reason));
                }
            }
        }
        Record::Snapshot(record) => {
            out.push(8);
            out.push(match record.query.kind {
                crate::reconciliation::bootstrap::SnapshotKind::Account => 1,
                crate::reconciliation::bootstrap::SnapshotKind::OpenOrders => 2,
                crate::reconciliation::bootstrap::SnapshotKind::ConditionalOrders => 3,
            });
            out.extend(record.query.private_connection_epoch.to_le_bytes());
            out.extend(record.query.gap_generation.to_le_bytes());
            out.extend(record.epoch.to_le_bytes());
            out.extend(record.received.to_le_bytes());
            out.extend(record.clock_boot_id);
            out.extend(record.clock_generation.to_le_bytes());
            out.extend(record.ledger.to_le_bytes());
            out.extend(record.status.to_le_bytes());
            out.push(u8::from(record.complete));
            blob(&mut out, &record.raw)?;
        }
        Record::BeginReconciliation { epoch, generation } => {
            out.push(10);
            out.extend(epoch.to_le_bytes());
            out.extend(generation.to_le_bytes());
        }
        Record::Rest(record) => {
            if record.query.is_some() && record.order_query.is_some() {
                return Err(Error::Encoding);
            }
            if let Some(query) = record.order_query {
                out.push(9);
                out.extend(query.order_id);
                out.extend(query.private_connection_epoch.to_le_bytes());
                out.extend(query.gap_generation.to_le_bytes());
            } else if let Some(query) = record.query {
                out.push(7);
                out.extend(query.order_id);
                out.extend(query.from_id.to_le_bytes());
                out.extend(query.limit.to_le_bytes());
                out.extend(query.private_connection_epoch.to_le_bytes());
                out.extend(query.gap_generation.to_le_bytes());
            } else {
                out.push(5);
            }
            blob(&mut out, record.command.bytes())?;
            out.extend(record.epoch.to_le_bytes());
            out.extend(record.received.to_le_bytes());
            out.extend(record.status.to_le_bytes());
            blob(&mut out, &record.raw)?;
            out.push(match record.disposition {
                crate::rest::RestDisposition::Observed => 1,
                crate::rest::RestDisposition::NeedsReconciliation => 2,
                crate::rest::RestDisposition::Rejected => 3,
                crate::rest::RestDisposition::Unknown => 4,
                crate::rest::RestDisposition::NoBytes => return Err(Error::Encoding),
            });
            out.push(record.events.len() as u8);
            for event in &record.events {
                blob(&mut out, event.bytes())?;
            }
        }
        Record::RestEffect { response, index } => {
            out.push(6);
            out.extend(response.to_le_bytes());
            out.push(*index);
        }
        Record::Gap {
            epoch,
            generation,
            reason,
        } => {
            out.push(4);
            out.extend(epoch.to_le_bytes());
            out.extend(generation.to_le_bytes());
            out.push(*reason);
        }
    }
    if out.len() > MAX_RECORD_BYTES {
        return Err(Error::Capacity);
    }
    Ok(out)
}
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.position.checked_add(n).ok_or(Error::Encoding)?;
        let out = self.bytes.get(self.position..end).ok_or(Error::Encoding)?;
        self.position = end;
        Ok(out)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn bool(&mut self) -> Result<bool> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Encoding),
        }
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn blob(&mut self) -> Result<&'a [u8]> {
        let n = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        self.take(n)
    }
}
pub(crate) fn decode(bytes: &[u8]) -> Result<Record> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::Capacity);
    }
    let mut r = Reader { bytes, position: 0 };
    if r.take(8)? != b"TWAI0001" {
        return Err(Error::Encoding);
    }
    let tag = r.byte()?;
    let record = match tag {
        13 => {
            use crate::reconciliation::income_import::{
                ImportRecord, IncomeImportProposal, SignedIncomeApproval,
            };
            let writer = r.u64()?;
            let accepted = r.u64()?;
            let proposal = IncomeImportProposal::decode(r.blob()?)?;
            let payload = r.blob()?;
            let signature = r.take(64)?;
            let approval = SignedIncomeApproval::new(payload, signature)?;
            Record::IncomeImport(ImportRecord {
                proposal,
                approval,
                accepted,
                writer,
            })
        }
        11 => {
            use crate::reconciliation::income::{IncomeQuery, IncomeRecord, IncomeUse};
            let start_time_ms = r.u64()?;
            let end_time_ms = r.u64()?;
            let private_connection_epoch = r.u64()?;
            let gap_generation = r.u64()?;
            let epoch = r.u64()?;
            let received = r.u64()?;
            let clock_generation = r.u64()?;
            let limit = u16::from_le_bytes(r.take(2)?.try_into().unwrap());
            let purpose = match r.byte()? {
                1 => IncomeUse::BaselineCoverage,
                2 => IncomeUse::Account,
                _ => return Err(Error::Encoding),
            };
            let query = IncomeQuery {
                start_time_ms,
                end_time_ms,
                limit,
                private_connection_epoch,
                gap_generation,
                purpose,
            };
            query.validate()?;
            let clock_boot_id = r.take(16)?.try_into().unwrap();
            let status = u16::from_le_bytes(r.take(2)?.try_into().unwrap());
            let accepted = r.bool()?;
            let raw = r.blob()?.to_vec();
            let count = r.byte()?;
            if count > 16
                || u16::from(count) > limit
                || epoch == 0
                || clock_generation == 0
                || clock_boot_id == [0; 16]
                || (!accepted && count != 0)
            {
                return Err(Error::Encoding);
            }
            let mut rows = Vec::with_capacity(count as usize);
            for _ in 0..count {
                rows.push(
                    v::CanonicalAccountInput::decode(r.blob()?).map_err(|_| Error::Encoding)?,
                );
            }
            Record::Income(IncomeRecord {
                query,
                epoch,
                received,
                clock_boot_id,
                clock_generation,
                status,
                accepted,
                raw,
                rows,
            })
        }
        12 => {
            let response = r.u64()?;
            let index = r.byte()?;
            if response == 0 || index >= 16 {
                return Err(Error::Encoding);
            }
            Record::IncomeEffect { response, index }
        }
        1 => Record::Config(r.blob()?.to_vec()),
        2 => {
            let command = CanonicalCommand::decode(r.blob()?).map_err(|_| Error::Encoding)?;
            let count = r.byte()?;
            if count == 0 || count > 3 {
                return Err(Error::Encoding);
            }
            let mut ids = Vec::with_capacity(count as usize);
            for _ in 0..count {
                ids.push(r.take(16)?.try_into().unwrap());
            }
            Record::Register(command, ids)
        }
        3 => {
            let epoch = r.u64()?;
            let present = r.bool()?;
            let subscription = r.u64()?;
            if !present && subscription != 0 {
                return Err(Error::Encoding);
            }
            let micros = r.bool()?;
            let received = r.u64()?;
            let raw = r.blob()?.to_vec();
            if raw.is_empty() || raw.len() > 32 * 1024 {
                return Err(Error::Encoding);
            }
            std::str::from_utf8(&raw).map_err(|_| Error::Encoding)?;
            let decision = match r.byte()? {
                1 => Decision::Bound(Box::new(
                    CanonicalEvent::decode(r.blob()?).map_err(|_| Error::Encoding)?,
                )),
                2 => Decision::Quarantine(reason(r.byte()?)?),
                3 => Decision::AccountBound(Box::new(
                    v::CanonicalAccountInput::decode(r.blob()?).map_err(|_| Error::Encoding)?,
                )),
                _ => return Err(Error::Encoding),
            };
            Record::Event(
                SourceMeta {
                    epoch,
                    subscription: present.then_some(subscription),
                    micros,
                    received,
                },
                raw,
                decision,
            )
        }
        4 => {
            let epoch = r.u64()?;
            let generation = r.u64()?;
            let reason = r.byte()?;
            if !(1..=14).contains(&reason) {
                return Err(Error::Encoding);
            }
            Record::Gap {
                epoch,
                generation,
                reason,
            }
        }
        5 | 7 | 9 => {
            let order_query = if tag == 9 {
                Some((r.take(16)?.try_into().unwrap(), r.u64()?, r.u64()?))
            } else {
                None
            };
            let query = if tag == 7 {
                Some((
                    r.take(16)?.try_into().unwrap(),
                    r.u64()?,
                    u16::from_le_bytes(r.take(2)?.try_into().unwrap()),
                    r.u64()?,
                    r.u64()?,
                ))
            } else {
                None
            };
            let command = CanonicalCommand::decode(r.blob()?).map_err(|_| Error::Encoding)?;
            let epoch = r.u64()?;
            let received = r.u64()?;
            let status = u16::from_le_bytes(r.take(2)?.try_into().unwrap());
            if status != 0 && !(100..600).contains(&status) {
                return Err(Error::Encoding);
            }
            let raw = r.blob()?.to_vec();
            let disposition = match r.byte()? {
                1 => crate::rest::RestDisposition::Observed,
                2 => crate::rest::RestDisposition::NeedsReconciliation,
                3 => crate::rest::RestDisposition::Rejected,
                4 => crate::rest::RestDisposition::Unknown,
                _ => return Err(Error::Encoding),
            };
            let count = r.byte()?;
            if count > if tag == 7 { 16 } else { 3 } {
                return Err(Error::Encoding);
            }
            let mut events = Vec::new();
            for _ in 0..count {
                events.push(CanonicalEvent::decode(r.blob()?).map_err(|_| Error::Encoding)?);
            }
            let query = query.map(
                |(order_id, from_id, limit, private_connection_epoch, gap_generation)| {
                    crate::reconciliation::TradeQuery {
                        intent: intent(&command),
                        order_id,
                        from_id,
                        limit,
                        private_connection_epoch,
                        gap_generation,
                    }
                },
            );
            if let Some(query) = query {
                query.validate()?;
            }
            let order_query =
                order_query.map(|(order_id, private_connection_epoch, gap_generation)| {
                    crate::reconciliation::orders::OrderQuery {
                        intent: intent(&command),
                        order_id,
                        private_connection_epoch,
                        gap_generation,
                    }
                });
            if let Some(q) = order_query {
                q.validate()?;
            }
            Record::Rest(crate::rest::RestRecord {
                command,
                query,
                order_query,
                epoch,
                received,
                status,
                raw,
                disposition,
                events,
            })
        }
        6 => {
            let response = r.u64()?;
            let index = r.byte()?;
            if response == 0 || index >= 16 {
                return Err(Error::Encoding);
            }
            Record::RestEffect { response, index }
        }
        8 => {
            use crate::reconciliation::bootstrap::{SnapshotKind, SnapshotQuery, SnapshotRecord};
            let kind = match r.byte()? {
                1 => SnapshotKind::Account,
                2 => SnapshotKind::OpenOrders,
                3 => SnapshotKind::ConditionalOrders,
                _ => return Err(Error::Encoding),
            };
            let query = SnapshotQuery {
                kind,
                private_connection_epoch: r.u64()?,
                gap_generation: r.u64()?,
            };
            query.validate()?;
            let epoch = r.u64()?;
            let received = r.u64()?;
            let clock_boot_id = r.take(16)?.try_into().unwrap();
            let clock_generation = r.u64()?;
            let ledger = r.u64()?;
            let status = u16::from_le_bytes(r.take(2)?.try_into().unwrap());
            let complete = r.bool()?;
            let raw = r.blob()?.to_vec();
            if epoch == 0
                || clock_boot_id == [0; 16]
                || clock_generation == 0
                || complete && status != 200
            {
                return Err(Error::Encoding);
            }
            Record::Snapshot(SnapshotRecord {
                query,
                epoch,
                received,
                clock_boot_id,
                clock_generation,
                ledger,
                status,
                complete,
                raw,
            })
        }
        10 => {
            let epoch = r.u64()?;
            let generation = r.u64()?;
            if epoch == 0 || generation == 0 {
                return Err(Error::Encoding);
            }
            Record::BeginReconciliation { epoch, generation }
        }
        _ => return Err(Error::Encoding),
    };
    if r.position != bytes.len() || encode(&record)? != bytes {
        return Err(Error::Encoding);
    }
    Ok(record)
}
fn reason_code(reason: QuarantineReason) -> u8 {
    match reason {
        QuarantineReason::UnknownOrder => 1,
        QuarantineReason::AmbiguousOrder => 2,
        QuarantineReason::UnadmittedOrder => 3,
        QuarantineReason::BindingConflict => 4,
        QuarantineReason::AccountRejected => 5,
        QuarantineReason::AccountReconciliation => 6,
    }
}
fn reason(code: u8) -> Result<QuarantineReason> {
    Ok(match code {
        1 => QuarantineReason::UnknownOrder,
        2 => QuarantineReason::AmbiguousOrder,
        3 => QuarantineReason::UnadmittedOrder,
        4 => QuarantineReason::BindingConflict,
        5 => QuarantineReason::AccountRejected,
        6 => QuarantineReason::AccountReconciliation,
        _ => return Err(Error::Encoding),
    })
}
