use crate::config::Deployment;
use serde_json::json;

pub fn deployment(instrument: u64) -> Deployment {
    serde_json::from_value(json!({
        "baseline":"/unused/baseline.cbor", "directories":{"account":"/unused/account","input":"/unused/input","recovery":"/unused/recovery"},
        "recover":false,"product":"spot","boot_id":([71u8;16]),"clock_generation":1,"owner_id":([72u8;16]),"writer_epoch":4,"initial_writer_epoch":4,
        "account_stream":([21u8;16]),"input_stream":([22u8;16]),"first_connection_epoch":1,"instrument_id":instrument,"book_source":9,
        "credential":{"api_key_file":"/unused/api","signing_key_file":"/unused/key","algorithm":"hmac","generation":1,"spot_logon":false},
        "network":{"rest":"127.0.0.1:1","time":"127.0.0.1:1","public_stream":"127.0.0.1:1","snapshot":"127.0.0.1:1","private_stream":"127.0.0.1:1","private_management":null,"trust_anchors":[]},
        "clock":{"max_round_trip_ns":1000000000,"max_width_ns":1000000000,"max_age_ns":10000000000u64,"quantization_error_ns":1000000,"drift_ppb":1,"max_adjustment_ns":1000000000,"calibration_interval_ns":1000000000,"calibration_retry_ns":100000000},
        "tuning":{"connect_timeout_ns":1000000000,"request_timeout_ns":1000000000,"delivery_budget_ns":1000000,"recv_window_micros":5000000,"maximum_book_age_ns":1000000000,"maximum_idle_wait_ns":1000000,"snapshot_limit":5,"maximum_intents":8,"maximum_input_records":64,"maximum_input_bytes":1048576,"wal_segment_bytes":1048576},
        "policy":{"scope":{"risk_domain_id":([1u8;16]),"venue":1,"environment":"test","account":2},"product":"spot","executor_id":([72u8;16]),"owner_generation":1,"writer_epoch":4,"domain":{"process_instance":([71u8;16]),"generation":1},"versions":{"config_revision":6,"risk_policy_version":10,"order_config_version":11,"catalog_version":14,"catalog_hash":([5u8;32])},"max_strategies":4,"max_feeds":4,"marks_per_feed":2,"control_revision":1,"mode":"stopped","owner_lease":null,"strategy_grants":[],"management_grant":null},
        "quota":{"supervisor_generation":1,"consumer_instance":([72u8;16]),"consumer_generation":1,"consumer_lease_until_ns":1000,"producer_id":([73u8;16]),"policy_generation":1,"policy_starts_ns":0,"policy_ends_ns":1000,"windows":[],"grants":[],"rules":[]},
        "strategy":{"mode":"observe"}, "reconciliation":{"enabled":false,"reader_id":([74u8;16]),"first_request_epoch":1,"first_execution_connection_epoch":2,"retry_ns":1000000000,"valid_for_ns":1000000000},
        "stop_file":"/unused/STOP","maximum_runtime_ns":1000000000
    })).unwrap()
}
