use std::{fs, process::Command};
#[test]
fn two_real_strategy_queues_share_the_native_wire_and_reopening_never_resends() {
    for venue in ["bybit", "okx"] {
        let path = std::env::temp_dir().join(format!(
            "tickwire-spot-route-{}-{venue}",
            std::process::id()
        ));
        assert!(!path.exists());
        let run = || {
            Command::new(env!("CARGO_BIN_EXE_tickwire-spot-runner"))
                .args(["paper", venue])
                .arg(&path)
                .output()
                .unwrap()
        };
        let first = run();
        assert!(
            first.status.success(),
            "{}",
            String::from_utf8_lossy(&first.stderr)
        );
        let replies: Vec<_> = std::str::from_utf8(&first.stderr)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|v| v["event"] == "request_reply")
            .collect();
        assert_eq!(replies.len(), 2);
        for (reply, member) in replies.iter().zip([8_u64, 9]) {
            assert_eq!(reply["member"], member);
            assert_eq!(reply["intent"], member);
            assert!(
                reply["reply"]
                    .as_str()
                    .unwrap()
                    .contains(&format!("paper{member}"))
            );
        }
        let summary: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
        assert_eq!(summary["profile"], "local-paper");
        assert_eq!(summary["strategies"], serde_json::json!([8, 9]));
        assert_eq!(summary["native_acknowledgements"], 2);
        assert_eq!(summary["open_orders"], 2);
        assert_eq!(summary["reserved_quote_atoms"], "12000000000");
        assert_eq!(summary["quote_exponent"], -8);
        assert!(path.join("account/ACCOUNT-BASELINE.cbor").exists());
        assert!(!run().status.success());
        fs::remove_dir_all(path).unwrap();
    }
}
