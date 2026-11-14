use crate::{Error, Result};
use tickwire_ipc::*;
use tickwire_recovery::{BlobStore, StoreKey};
use tickwire_shm::Publisher;
use tickwire_venue::CanonicalCommand;
use tickwire_venue::{
    ClientId, ClientIdDomain, Command, CommandBody, CommandContext, Decimal, ModifyOrder,
    OrderTarget, PositionSide, SelfDescribingClientId, Side, SpotAdvancedCommand,
};

/// Ergonomic alias for [`IntentWriter`] on the strategy outbound SPSC command lane.
pub type OutboundWriter = IntentWriter;

/// Builder for [`CommandBody::Modify`] (`command_kind = 5`, `message_type = 517`) that validates
/// positive `price` and `quantity` plus non-zero `OrderTarget` identity before queuing to SPSC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyRequest {
    pub target: OrderTarget,
    pub new_client_id: Option<ClientId>,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
    pub position_side: PositionSide,
    pub reduce_only: bool,
}

impl ModifyRequest {
    #[inline]
    pub fn new(target: OrderTarget, side: Side, quantity: Decimal, price: Decimal) -> Self {
        Self {
            target,
            new_client_id: None,
            side,
            quantity,
            price,
            position_side: PositionSide::Both,
            reduce_only: false,
        }
    }

    #[inline]
    pub fn with_new_client_id(mut self, client_id: ClientId) -> Self {
        self.new_client_id = Some(client_id);
        self
    }

    /// Mints a deterministic `SelfDescribingClientId` for venues whose in-place modification
    /// wire assigns a fresh `newClientOrderId` (for example, `REPLACEMENT_ORDER_LEG`).
    pub fn with_minted_client_id(mut self, ctx: &CommandContext, leg: u8) -> Result<Self> {
        let (client_id, _) = mint_self_describing_client_id(ctx, leg)?;
        self.new_client_id = Some(client_id);
        Ok(self)
    }

    #[inline]
    pub fn with_position_side(mut self, position_side: PositionSide) -> Self {
        self.position_side = position_side;
        self
    }

    #[inline]
    pub fn with_reduce_only(mut self, reduce_only: bool) -> Self {
        self.reduce_only = reduce_only;
        self
    }

    /// Validates strictly positive `quantity` and `price` and non-empty `OrderTarget` fields.
    pub fn validate(&self) -> Result<()> {
        if !self.quantity.is_positive() {
            return Err(Error::Command(tickwire_venue::Error::Quantity));
        }
        if !self.price.is_positive() {
            return Err(Error::Command(tickwire_venue::Error::Price));
        }
        if self.target.order_id == [0; 16]
            || self.target.expected_owner_revision == 0
            || self.target.original_intent_exec_epoch == 0
            || (self.target.client_id.is_none() && self.target.venue_order_id.is_none())
        {
            return Err(Error::Command(tickwire_venue::Error::Identity));
        }
        Ok(())
    }

    pub fn into_modify_order(self) -> Result<ModifyOrder> {
        self.validate()?;
        Ok(ModifyOrder {
            target: self.target,
            new_client_id: self.new_client_id,
            side: self.side,
            quantity: self.quantity,
            price: self.price,
            position_side: self.position_side,
            reduce_only: self.reduce_only,
        })
    }

    /// Validates `self` and constructs a canonical CBOR [`CanonicalCommand`] checked against
    /// `context` instrument tick/step bounds.
    pub fn build_command(self, context: CommandContext) -> Result<CanonicalCommand> {
        let modify = self.into_modify_order()?;
        build_modify_command(context, modify)
    }
}

/// Validates a [`ModifyOrder`] (`price > 0`, `quantity > 0`, valid `target`, and catalog
/// tick/step alignment) and returns its [`CanonicalCommand`].
pub fn build_modify_command(
    context: CommandContext,
    modify: ModifyOrder,
) -> Result<CanonicalCommand> {
    if !modify.quantity.is_positive() {
        return Err(Error::Command(tickwire_venue::Error::Quantity));
    }
    if !modify.price.is_positive() {
        return Err(Error::Command(tickwire_venue::Error::Price));
    }
    if modify.target.order_id == [0; 16]
        || modify.target.expected_owner_revision == 0
        || modify.target.original_intent_exec_epoch == 0
        || (modify.target.client_id.is_none() && modify.target.venue_order_id.is_none())
    {
        return Err(Error::Command(tickwire_venue::Error::Identity));
    }
    Ok(CanonicalCommand::new(Command {
        context,
        body: CommandBody::Modify(modify),
    })?)
}

/// Mint a canonical 32-character `[0-9a-z]` `SelfDescribingClientId` and its deterministic
/// 16-byte internal order UUID from `CommandContext`.
pub fn mint_self_describing_client_id(
    ctx: &CommandContext,
    leg: u8,
) -> Result<(ClientId, [u8; 16])> {
    let domain = ClientIdDomain::from_command_context(ctx);
    let sd = SelfDescribingClientId::from_command_context(ctx, leg)?;
    let client_id = sd.encode(&domain)?;
    Ok((client_id, sd.deterministic_order_id(&domain)))
}
pub struct StoredCommand {
    digest: [u8; 32],
    bytes: u32,
}
impl StoredCommand {
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn bytes(&self) -> u32 {
        self.bytes
    }
}
pub struct CommandStore<B> {
    inner: B,
}
impl<B: BlobStore> CommandStore<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
    pub fn put(&mut self, command: &CanonicalCommand) -> Result<StoredCommand> {
        self.inner
            .put_sync(StoreKey::Object(command.digest()), command.bytes())?;
        Ok(StoredCommand {
            digest: command.digest(),
            bytes: command.bytes().len() as u32,
        })
    }
    pub fn resolve(&self, digest: [u8; 32], bytes: u32) -> Result<CanonicalCommand> {
        if bytes == 0 || bytes as usize > tickwire_venue::MAX_CANONICAL_BYTES {
            return Err(Error::Bounds);
        }
        let value = self.inner.get(StoreKey::Object(digest), bytes as usize)?;
        let command = CanonicalCommand::decode(&value)?;
        if command.digest() != digest || value.len() != bytes as usize {
            return Err(Error::Identity);
        }
        Ok(command)
    }
}
pub fn resolve_file(
    reader: &tickwire_recovery::FileBlobReader,
    digest: [u8; 32],
    bytes: u32,
) -> Result<CanonicalCommand> {
    if bytes == 0 || bytes as usize > tickwire_venue::MAX_CANONICAL_BYTES {
        return Err(Error::Bounds);
    }
    let value = reader.get_object(digest, bytes as usize)?;
    let command = CanonicalCommand::decode(&value)?;
    if command.digest() != digest || value.len() != bytes as usize {
        return Err(Error::Identity);
    }
    Ok(command)
}
pub fn resolve_blob<B: BlobStore>(
    store: &B,
    digest: [u8; 32],
    bytes: u32,
) -> Result<CanonicalCommand> {
    if bytes == 0 || bytes as usize > tickwire_venue::MAX_CANONICAL_BYTES {
        return Err(Error::Bounds);
    }
    let value = store.get(StoreKey::Object(digest), bytes as usize)?;
    let command = CanonicalCommand::decode(&value)?;
    if command.digest() != digest || value.len() != bytes as usize {
        return Err(Error::Identity);
    }
    Ok(command)
}
pub struct IntentWriter {
    publisher: Publisher<1>,
    strategy: u64,
    instance: [u8; 16],
    generation: u64,
    last_intent: u64,
    risk_domain: [u8; 16],
}
impl IntentWriter {
    pub fn new(
        publisher: Publisher<1>,
        strategy: u64,
        instance: [u8; 16],
        generation: u64,
        risk_domain: [u8; 16],
    ) -> Result<Self> {
        let h = publisher.header();
        if risk_domain == [0; 16]
            || h.risk_scope == 0
            || h.resource_kind != 2
            || h.required_features & 128 == 0
            || strategy == 0
            || instance != h.producer_instance
            || generation != h.producer_generation
        {
            return Err(Error::Identity);
        }
        Ok(Self {
            publisher,
            strategy,
            instance,
            generation,
            last_intent: 0,
            risk_domain,
        })
    }
    pub fn publisher(&mut self) -> &mut Publisher<1> {
        &mut self.publisher
    }
    pub fn publish<B: BlobStore>(
        &mut self,
        command: &CanonicalCommand,
        store: &mut CommandStore<B>,
        now: u64,
    ) -> Result<u64> {
        let c = &command.command().context;
        if c.request.risk_domain_id != self.risk_domain
            || c.strategy.logical_strategy_id != self.strategy
            || c.strategy.instance_id != self.instance
            || c.request.strategy_generation != self.generation
            || c.strategy.strategy_intent_seq <= self.last_intent
            || now < c.strategy.created_mono_ns
            || now >= c.request.deadline
        {
            return Err(Error::Identity);
        }
        let receipt = store.put(command)?;
        let mut record = encode_intent(
            command,
            &receipt,
            self.publisher
                .published()
                .checked_add(1)
                .ok_or(Error::Bounds)?,
            now,
            self.publisher.header().risk_scope,
        )?;
        record.header.record_crc32c = 0;
        let mut image = [0u8; 1024];
        record.encode(&mut image)?;
        record.header.record_crc32c = crc32c(&image);
        record.encode(&mut image)?;
        decode_record(&image)?;
        let seq = self.publisher.publish(&[&image])?;
        self.last_intent = c.strategy.strategy_intent_seq;
        Ok(seq)
    }

    /// Validates `request`, builds the canonical [`CommandBody::Modify`] command (`command_kind = 5`,
    /// `message_type = 517`), persists it in `store`, and publishes it onto the SPSC intent ring.
    pub fn publish_modify<B: BlobStore>(
        &mut self,
        context: CommandContext,
        request: ModifyRequest,
        store: &mut CommandStore<B>,
        now: u64,
    ) -> Result<(CanonicalCommand, u64)> {
        let command = request.build_command(context)?;
        let seq = self.publish(&command, store, now)?;
        Ok((command, seq))
    }
}
pub fn encode_intent(
    command: &CanonicalCommand,
    receipt: &StoredCommand,
    sequence: u64,
    now: u64,
    risk_scope: u64,
) -> Result<IntentRecord> {
    if risk_scope == 0
        || receipt.digest != command.digest()
        || receipt.bytes as usize != command.bytes().len()
    {
        return Err(Error::Identity);
    }
    let c = &command.command().context;
    let kind = match &command.command().body {
        CommandBody::New(_) | CommandBody::SpotList(_) | CommandBody::Algo(_) => 1,
        CommandBody::Cancel(_) => 2,
        CommandBody::CancelReplace { .. } => 3,
        CommandBody::AmendKeepPriority { .. } => 4,
        CommandBody::Modify(_) => 5,
        CommandBody::Query(_) => 6,
        CommandBody::CancelList(_) => 7,
        CommandBody::CancelAlgo(_) => 8,
        CommandBody::SpotAdvanced(SpotAdvancedCommand::CancelReplace { .. }) => 3,
        CommandBody::SpotAdvanced(_) => 1,
    };
    let mut body = IntentBody {
        logical_strategy_id: c.strategy.logical_strategy_id,
        instance_id: c.strategy.instance_id,
        strategy_generation: c.request.strategy_generation,
        exec_generation: c.request.intent_exec_epoch,
        authorization_epoch: c.strategy.authorization_epoch,
        config_version: c.request.config_revision,
        intent_seq: c.strategy.strategy_intent_seq,
        economic_intent_id: c.strategy.economic_intent_id,
        created_mono_ns: c.strategy.created_mono_ns,
        valid_until_mono_ns: c.request.deadline,
        hedge_group_id: c.strategy.hedge_group_id,
        leg_id: c.strategy.leg_id,
        trigger_count: c.triggers.as_slice().len() as u16,
        command_kind: kind,
        recovery_manifest_hash: c.strategy.recovery_manifest_hash,
        catalog_hash: c.instrument.catalog_hash,
        risk_policy_version: c.strategy.risk_policy_version,
        order_config_version: c.strategy.order_config_version,
        command_digest: receipt.digest,
        command_bytes: receipt.bytes,
        ..Default::default()
    };
    let product = match c.request.product {
        tickwire_venue::Product::Spot => 1,
        tickwire_venue::Product::LinearPerpetual | tickwire_venue::Product::LinearFuture => 2,
        _ => return Err(Error::Identity),
    };
    for (out, t) in body.triggers.iter_mut().zip(c.triggers.as_slice()) {
        *out = TriggerRef {
            topic_id: t.topic_id,
            feed_generation: t.feed_generation,
            ring_epoch: t.ring_epoch,
            sequence: t.sequence,
            instrument_id: t.instrument_id,
            instrument_version: t.instrument_version,
            observed_mono_ns: t.observed_mono_ns,
            required_feed_state: 2,
            product,
        };
    }
    Ok(IntentRecord {
        header: RecordHeader {
            sequence,
            message_type: 512 + kind,
            schema_major: 1,
            header_bytes: 128,
            body_bytes: 896,
            flags: 4 | 8,
            topic_or_scope_id: risk_scope,
            producer_generation: c.request.strategy_generation,
            instrument_id: c.instrument.instrument_id,
            instrument_version: c.instrument.instrument_version,
            publish_mono_ns: now,
            ..Default::default()
        },
        body,
    })
}
/// Resolve the immutable command and compare every canonical-reference wire field.
/// The full 128-bit risk domain is supplied by the trusted channel registry.
pub fn resolve_record(
    reader: &tickwire_recovery::FileBlobReader,
    raw: &[u8],
    bank: &ResourceHeader,
    risk_domain: [u8; 16],
) -> Result<CanonicalCommand> {
    if bank.resource_kind != 2 || bank.required_features & 128 == 0 || risk_domain == [0; 16] {
        return Err(Error::Identity);
    }
    let Record::Intent(record) = decode_record(raw)? else {
        return Err(Error::Identity);
    };
    let command = resolve_file(
        reader,
        record.body.command_digest,
        record.body.command_bytes,
    )?;
    let c = &command.command().context;
    if c.request.risk_domain_id != risk_domain
        || c.strategy.instance_id != bank.producer_instance
        || c.request.strategy_generation != bank.producer_generation
    {
        return Err(Error::Identity);
    }
    let receipt = StoredCommand {
        digest: command.digest(),
        bytes: command.bytes().len() as u32,
    };
    let mut expected = encode_intent(
        &command,
        &receipt,
        record.header.sequence,
        record.header.publish_mono_ns,
        bank.risk_scope,
    )?;
    expected.header.record_crc32c = record.header.record_crc32c;
    if wire_image(&expected)?.as_slice() != raw {
        return Err(Error::Identity);
    }
    Ok(command)
}
pub fn resolve_record_from_store<B: BlobStore>(
    store: &B,
    raw: &[u8],
    bank: &ResourceHeader,
    risk_domain: [u8; 16],
) -> Result<CanonicalCommand> {
    if bank.resource_kind != 2 || bank.required_features & 128 == 0 || risk_domain == [0; 16] {
        return Err(Error::Identity);
    }
    let Record::Intent(record) = decode_record(raw)? else {
        return Err(Error::Identity);
    };
    let command = resolve_blob(store, record.body.command_digest, record.body.command_bytes)?;
    let c = &command.command().context;
    if c.request.risk_domain_id != risk_domain
        || c.strategy.instance_id != bank.producer_instance
        || c.request.strategy_generation != bank.producer_generation
    {
        return Err(Error::Identity);
    }
    let receipt = StoredCommand {
        digest: command.digest(),
        bytes: command.bytes().len() as u32,
    };
    let mut expected = encode_intent(
        &command,
        &receipt,
        record.header.sequence,
        record.header.publish_mono_ns,
        bank.risk_scope,
    )?;
    expected.header.record_crc32c = record.header.record_crc32c;
    if wire_image(&expected)?.as_slice() != raw {
        return Err(Error::Identity);
    }
    Ok(command)
}
fn wire_image(record: &IntentRecord) -> Result<Vec<u8>> {
    let mut bytes = vec![0; IntentRecord::SIZE];
    record.encode(&mut bytes)?;
    Ok(bytes)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn immutable_command_is_stored_before_a_reference_exists() {
        let command = CanonicalCommand::new(crate::test_command::command()).unwrap();
        let mut mock = tickwire_recovery::MockBlobStore::default();
        mock.fail_next_put = true;
        let mut store = CommandStore::new(mock);
        assert!(store.put(&command).is_err());
        let receipt = store.put(&command).unwrap();
        assert_eq!(
            store
                .resolve(receipt.digest(), receipt.bytes())
                .unwrap()
                .bytes(),
            command.bytes()
        );
    }
    #[test]
    fn full_reference_keeps_digest_triggers_and_explicit_scope() {
        let command = CanonicalCommand::new(crate::test_command::command()).unwrap();
        let mut store = CommandStore::new(tickwire_recovery::MockBlobStore::default());
        let receipt = store.put(&command).unwrap();
        let mut record = encode_intent(&command, &receipt, 7, 60, 900).unwrap();
        assert_eq!(record.header.topic_or_scope_id, 900);
        assert_eq!(record.body.command_digest, command.digest());
        assert_eq!(record.body.triggers[0].sequence, 1);
        let mut bytes = wire_image(&record).unwrap();
        record.header.record_crc32c = crc32c(&bytes);
        record.encode(&mut bytes).unwrap();
        let Record::Intent(decoded) = decode_record(&bytes).unwrap() else {
            panic!("intent");
        };
        assert_eq!(decoded.body.command_bytes as usize, command.bytes().len());
        assert!(encode_intent(&command, &receipt, 7, 60, 0).is_err());
    }
    #[test]
    fn mints_self_describing_client_id_and_round_trips_through_in_memory_command_store() {
        let mut raw_cmd = crate::test_command::command();
        let (cid, det_id) = mint_self_describing_client_id(&raw_cmd.context, 0).unwrap();
        assert_eq!(cid.as_str().len(), 32);
        assert_ne!(det_id, [0; 16]);
        if let CommandBody::New(ref mut spec) = raw_cmd.body {
            spec.client_id = cid.clone();
        }
        let domain = ClientIdDomain::from_command_context(&raw_cmd.context);
        assert_eq!(
            SelfDescribingClientId::decode(&cid, &domain),
            tickwire_venue::DecodedClientId::Managed(SelfDescribingClientId {
                logical_strategy_id: 8,
                leg: 0,
                epoch: 4,
                intent_id: 3,
            })
        );
        let command = CanonicalCommand::new(raw_cmd).unwrap();
        let blob_store = tickwire_recovery::RuntimeBlobStore::in_memory();
        let mut store = CommandStore::new(blob_store);
        let receipt = store.put(&command).unwrap();
        assert_eq!(
            store
                .resolve(receipt.digest(), receipt.bytes())
                .unwrap()
                .digest(),
            command.digest()
        );
    }
}
