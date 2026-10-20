// Generated fixed-layout boundary tests.
use tickwire_ipc::*;
#[test]
fn layout_arenaclass() {
    let value = ArenaClass::default();
    let mut raw = vec![0xCC; 64];
    value.encode(&mut raw).unwrap();
    assert_eq!(ArenaClass::decode(&raw).unwrap(), value);
    for length in [0, 63, 65] {
        assert_eq!(ArenaClass::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 2..4 {
        raw[i] = 1;
        assert_eq!(ArenaClass::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 32..64 {
        raw[i] = 1;
        assert_eq!(ArenaClass::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_resourceheader() {
    let value = ResourceHeader::default();
    let mut raw = vec![0xCC; 4096];
    value.encode(&mut raw).unwrap();
    assert_eq!(ResourceHeader::decode(&raw).unwrap(), value);
    for length in [0, 4095, 4097] {
        assert_eq!(ResourceHeader::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 368..512 {
        raw[i] = 1;
        assert_eq!(ResourceHeader::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 768..4096 {
        raw[i] = 1;
        assert_eq!(ResourceHeader::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_producerpage() {
    let value = ProducerPage::default();
    let mut raw = vec![0xCC; 4096];
    value.encode(&mut raw).unwrap();
    assert_eq!(ProducerPage::decode(&raw).unwrap(), value);
    for length in [0, 4095, 4097] {
        assert_eq!(ProducerPage::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 8..128 {
        raw[i] = 1;
        assert_eq!(ProducerPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 136..256 {
        raw[i] = 1;
        assert_eq!(ProducerPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 264..384 {
        raw[i] = 1;
        assert_eq!(ProducerPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 392..512 {
        raw[i] = 1;
        assert_eq!(ProducerPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 520..640 {
        raw[i] = 1;
        assert_eq!(ProducerPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 648..768 {
        raw[i] = 1;
        assert_eq!(ProducerPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 776..896 {
        raw[i] = 1;
        assert_eq!(ProducerPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 904..4096 {
        raw[i] = 1;
        assert_eq!(ProducerPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_cursorpage() {
    let value = CursorPage::default();
    let mut raw = vec![0xCC; 4096];
    value.encode(&mut raw).unwrap();
    assert_eq!(CursorPage::decode(&raw).unwrap(), value);
    for length in [0, 4095, 4097] {
        assert_eq!(CursorPage::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 8..128 {
        raw[i] = 1;
        assert_eq!(CursorPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 136..256 {
        raw[i] = 1;
        assert_eq!(CursorPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 264..384 {
        raw[i] = 1;
        assert_eq!(CursorPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 392..512 {
        raw[i] = 1;
        assert_eq!(CursorPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 520..640 {
        raw[i] = 1;
        assert_eq!(CursorPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 648..768 {
        raw[i] = 1;
        assert_eq!(CursorPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 776..896 {
        raw[i] = 1;
        assert_eq!(CursorPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 904..4096 {
        raw[i] = 1;
        assert_eq!(CursorPage::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_recordheader() {
    let value = RecordHeader::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(RecordHeader::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(RecordHeader::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 38..40 {
        raw[i] = 1;
        assert_eq!(RecordHeader::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 120..128 {
        raw[i] = 1;
        assert_eq!(RecordHeader::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_arenablockheader() {
    let value = ArenaBlockHeader::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(ArenaBlockHeader::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(
            ArenaBlockHeader::decode(&vec![0; length]),
            Err(Error::Bounds)
        );
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 14..16 {
        raw[i] = 1;
        assert_eq!(ArenaBlockHeader::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 44..128 {
        raw[i] = 1;
        assert_eq!(ArenaBlockHeader::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_depthlevel() {
    let value = DepthLevel::default();
    let mut raw = vec![0xCC; 16];
    value.encode(&mut raw).unwrap();
    assert_eq!(DepthLevel::decode(&raw).unwrap(), value);
    for length in [0, 15, 17] {
        assert_eq!(DepthLevel::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
}
#[test]
fn layout_triggerref() {
    let value = TriggerRef::default();
    let mut raw = vec![0xCC; 64];
    value.encode(&mut raw).unwrap();
    assert_eq!(TriggerRef::decode(&raw).unwrap(), value);
    for length in [0, 63, 65] {
        assert_eq!(TriggerRef::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 60..64 {
        raw[i] = 1;
        assert_eq!(TriggerRef::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_marketbody() {
    let value = MarketBody::default();
    let mut raw = vec![0xCC; 384];
    value.encode(&mut raw).unwrap();
    assert_eq!(MarketBody::decode(&raw).unwrap(), value);
    for length in [0, 383, 385] {
        assert_eq!(MarketBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 180..184 {
        raw[i] = 1;
        assert_eq!(MarketBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_intentbody() {
    let value = IntentBody::default();
    let mut raw = vec![0xCC; 896];
    value.encode(&mut raw).unwrap();
    assert_eq!(IntentBody::decode(&raw).unwrap(), value);
    for length in [0, 895, 897] {
        assert_eq!(IntentBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 304..384 {
        raw[i] = 1;
        assert_eq!(IntentBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_reportbody() {
    let value = ReportBody::default();
    let mut raw = vec![0xCC; 384];
    value.encode(&mut raw).unwrap();
    assert_eq!(ReportBody::decode(&raw).unwrap(), value);
    for length in [0, 383, 385] {
        assert_eq!(ReportBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 376..384 {
        raw[i] = 1;
        assert_eq!(ReportBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_ownershipbody() {
    let value = OwnershipBody::default();
    let mut raw = vec![0xCC; 384];
    value.encode(&mut raw).unwrap();
    assert_eq!(OwnershipBody::decode(&raw).unwrap(), value);
    for length in [0, 383, 385] {
        assert_eq!(OwnershipBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 256..384 {
        raw[i] = 1;
        assert_eq!(OwnershipBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_ownershiprecord() {
    let value = OwnershipRecord::default();
    let mut raw = vec![0xCC; 512];
    value.encode(&mut raw).unwrap();
    assert_eq!(OwnershipRecord::decode(&raw).unwrap(), value);
    for length in [0, 511, 513] {
        assert_eq!(
            OwnershipRecord::decode(&vec![0; length]),
            Err(Error::Bounds)
        );
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
}
#[test]
fn layout_marketrecord() {
    let value = MarketRecord::default();
    let mut raw = vec![0xCC; 512];
    value.encode(&mut raw).unwrap();
    assert_eq!(MarketRecord::decode(&raw).unwrap(), value);
    for length in [0, 511, 513] {
        assert_eq!(MarketRecord::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
}
#[test]
fn layout_intentrecord() {
    let value = IntentRecord::default();
    let mut raw = vec![0xCC; 1024];
    value.encode(&mut raw).unwrap();
    assert_eq!(IntentRecord::decode(&raw).unwrap(), value);
    for length in [0, 1023, 1025] {
        assert_eq!(IntentRecord::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
}
#[test]
fn layout_reportrecord() {
    let value = ReportRecord::default();
    let mut raw = vec![0xCC; 512];
    value.encode(&mut raw).unwrap();
    assert_eq!(ReportRecord::decode(&raw).unwrap(), value);
    for length in [0, 511, 513] {
        assert_eq!(ReportRecord::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
}
#[test]
fn layout_snapshotheader() {
    let value = SnapshotHeader::default();
    let mut raw = vec![0xCC; 256];
    value.encode(&mut raw).unwrap();
    assert_eq!(SnapshotHeader::decode(&raw).unwrap(), value);
    for length in [0, 255, 257] {
        assert_eq!(SnapshotHeader::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
}
#[test]
fn layout_snapshotsection() {
    let value = SnapshotSection::default();
    let mut raw = vec![0xCC; 64];
    value.encode(&mut raw).unwrap();
    assert_eq!(SnapshotSection::decode(&raw).unwrap(), value);
    for length in [0, 63, 65] {
        assert_eq!(
            SnapshotSection::decode(&vec![0; length]),
            Err(Error::Bounds)
        );
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 56..64 {
        raw[i] = 1;
        assert_eq!(SnapshotSection::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_topicsnapshotentry() {
    let value = TopicSnapshotEntry::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(TopicSnapshotEntry::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(
            TopicSnapshotEntry::decode(&vec![0; length]),
            Err(Error::Bounds)
        );
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 120..128 {
        raw[i] = 1;
        assert_eq!(
            TopicSnapshotEntry::decode(&raw),
            Err(Error::ReservedNonzero)
        );
        raw[i] = 0;
    }
}
#[test]
fn layout_instrumententry() {
    let value = InstrumentEntry::default();
    let mut raw = vec![0xCC; 256];
    value.encode(&mut raw).unwrap();
    assert_eq!(InstrumentEntry::decode(&raw).unwrap(), value);
    for length in [0, 255, 257] {
        assert_eq!(
            InstrumentEntry::decode(&vec![0; length]),
            Err(Error::Bounds)
        );
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 100..104 {
        raw[i] = 1;
        assert_eq!(InstrumentEntry::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 224..256 {
        raw[i] = 1;
        assert_eq!(InstrumentEntry::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_controlenvelope() {
    let value = ControlEnvelope::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(ControlEnvelope::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(
            ControlEnvelope::decode(&vec![0; length]),
            Err(Error::Bounds)
        );
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 124..128 {
        raw[i] = 1;
        assert_eq!(ControlEnvelope::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_hellobody() {
    let value = HelloBody::default();
    let mut raw = vec![0xCC; 192];
    value.encode(&mut raw).unwrap();
    assert_eq!(HelloBody::decode(&raw).unwrap(), value);
    for length in [0, 191, 193] {
        assert_eq!(HelloBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 180..192 {
        raw[i] = 1;
        assert_eq!(HelloBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_welcomebody() {
    let value = WelcomeBody::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(WelcomeBody::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(WelcomeBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 112..128 {
        raw[i] = 1;
        assert_eq!(WelcomeBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_resourcedescriptor() {
    let value = ResourceDescriptor::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(ResourceDescriptor::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(
            ResourceDescriptor::decode(&vec![0; length]),
            Err(Error::Bounds)
        );
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
}
#[test]
fn layout_offerprefix() {
    let value = OfferPrefix::default();
    let mut raw = vec![0xCC; 64];
    value.encode(&mut raw).unwrap();
    assert_eq!(OfferPrefix::decode(&raw).unwrap(), value);
    for length in [0, 63, 65] {
        assert_eq!(OfferPrefix::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 40..64 {
        raw[i] = 1;
        assert_eq!(OfferPrefix::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_resourceackbody() {
    let value = ResourceAckBody::default();
    let mut raw = vec![0xCC; 96];
    value.encode(&mut raw).unwrap();
    assert_eq!(ResourceAckBody::decode(&raw).unwrap(), value);
    for length in [0, 95, 97] {
        assert_eq!(
            ResourceAckBody::decode(&vec![0; length]),
            Err(Error::Bounds)
        );
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 88..96 {
        raw[i] = 1;
        assert_eq!(ResourceAckBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_joinbody() {
    let value = JoinBody::default();
    let mut raw = vec![0xCC; 256];
    value.encode(&mut raw).unwrap();
    assert_eq!(JoinBody::decode(&raw).unwrap(), value);
    for length in [0, 255, 257] {
        assert_eq!(JoinBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 248..256 {
        raw[i] = 1;
        assert_eq!(JoinBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_freezebody() {
    let value = FreezeBody::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(FreezeBody::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(FreezeBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 106..128 {
        raw[i] = 1;
        assert_eq!(FreezeBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_switchbody() {
    let value = SwitchBody::default();
    let mut raw = vec![0xCC; 192];
    value.encode(&mut raw).unwrap();
    assert_eq!(SwitchBody::decode(&raw).unwrap(), value);
    for length in [0, 191, 193] {
        assert_eq!(SwitchBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 162..192 {
        raw[i] = 1;
        assert_eq!(SwitchBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_quiescebody() {
    let value = QuiesceBody::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(QuiesceBody::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(QuiesceBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 116..128 {
        raw[i] = 1;
        assert_eq!(QuiesceBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_grantbody() {
    let value = GrantBody::default();
    let mut raw = vec![0xCC; 384];
    value.encode(&mut raw).unwrap();
    assert_eq!(GrantBody::decode(&raw).unwrap(), value);
    for length in [0, 383, 385] {
        assert_eq!(GrantBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 356..384 {
        raw[i] = 1;
        assert_eq!(GrantBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_execleasebody() {
    let value = ExecLeaseBody::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(ExecLeaseBody::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(ExecLeaseBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 104..128 {
        raw[i] = 1;
        assert_eq!(ExecLeaseBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_statusbody() {
    let value = StatusBody::default();
    let mut raw = vec![0xCC; 192];
    value.encode(&mut raw).unwrap();
    assert_eq!(StatusBody::decode(&raw).unwrap(), value);
    for length in [0, 191, 193] {
        assert_eq!(StatusBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 184..192 {
        raw[i] = 1;
        assert_eq!(StatusBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_querybody() {
    let value = QueryBody::default();
    let mut raw = vec![0xCC; 96];
    value.encode(&mut raw).unwrap();
    assert_eq!(QueryBody::decode(&raw).unwrap(), value);
    for length in [0, 95, 97] {
        assert_eq!(QueryBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 42..48 {
        raw[i] = 1;
        assert_eq!(QueryBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
    for i in 80..96 {
        raw[i] = 1;
        assert_eq!(QueryBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_configcontrolbody() {
    let value = ConfigControlBody::default();
    let mut raw = vec![0xCC; 768];
    value.encode(&mut raw).unwrap();
    assert_eq!(ConfigControlBody::decode(&raw).unwrap(), value);
    for length in [0, 767, 769] {
        assert_eq!(
            ConfigControlBody::decode(&vec![0; length]),
            Err(Error::Bounds)
        );
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 610..616 {
        raw[i] = 1;
        assert_eq!(ConfigControlBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
#[test]
fn layout_resyncbody() {
    let value = ResyncBody::default();
    let mut raw = vec![0xCC; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(ResyncBody::decode(&raw).unwrap(), value);
    for length in [0, 127, 129] {
        assert_eq!(ResyncBody::decode(&vec![0; length]), Err(Error::Bounds));
        assert_eq!(value.encode(&mut vec![0; length]), Err(Error::Bounds));
    }
    for i in 112..128 {
        raw[i] = 1;
        assert_eq!(ResyncBody::decode(&raw), Err(Error::ReservedNonzero));
        raw[i] = 0;
    }
}
