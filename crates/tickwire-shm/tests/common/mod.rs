use tickwire_ipc::{MarketBody, MarketRecord, RecordHeader, Wire};

/// Valid fixed ABI bytes whose sequence and timestamp vary across ring reuse.
pub fn record(sequence: u64) -> [u8; 512] {
    let record = MarketRecord {
        header: RecordHeader {
            sequence,
            message_type: 261,
            schema_major: 1,
            header_bytes: 128,
            body_bytes: 384,
            flags: 7,
            topic_or_scope_id: 1,
            feed_generation: 1,
            producer_generation: 1,
            instrument_id: 7,
            instrument_version: 1,
            source_timestamp_ns: sequence * 1000,
            ingress_mono_ns: sequence * 1000 + 1,
            publish_mono_ns: sequence * 1000 + 2,
            ..Default::default()
        },
        body: MarketBody {
            exchange_event_ns: sequence * 1000,
            feed_state: 2,
            ..Default::default()
        },
    };
    let mut bytes = [0; 512];
    record.encode(&mut bytes).unwrap();
    tickwire_ipc::decode_record(&bytes).unwrap();
    bytes
}
