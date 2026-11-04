//! Exercise the deployed CLI, catalog qualification and restart boundary without venue access.
#![cfg(target_os = "linux")]
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tickwire_ipc::{EXACT_MARKET_EVENTS, ResourceHeader, Wire};

struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn write(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

#[test]
fn spot_market_cli_qualifies_catalog_and_preserves_the_bank_generation_across_restart() {
    for (id, example) in [
        (2, include_str!("../examples/bybit-spot-quote.json")),
        (3, include_str!("../examples/okx-spot-quote.json")),
    ] {
        let temp = tickwire_control_sys::PrivateDirectory::create_in(Path::new("/tmp"), "tw-spot")
            .unwrap();
        let root = temp.path();
        let admissions = root.join("admissions");
        fs::create_dir(&admissions).unwrap();
        fs::set_permissions(&admissions, fs::Permissions::from_mode(0o700)).unwrap();
        let mut source: Value = serde_json::from_str(example).unwrap();
        // A loopback proxy stalls before TLS; subscriber setup must still run on the owner thread.
        let proxy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        source["proxy_addr"] = json!(proxy.local_addr().unwrap());
        let row = json!({"id":source["instrument_id"].to_string(),"version":"1","product":"Spot",
            "symbol":source["symbol"],"base":"1","quote":"2","settlement":"2","price_tick":source["price_tick"],
            "quantity_step":source["quantity_step"],"contract":{"kind":"Cash"}});
        let catalog = json!({"schema":1,"venue":id,"environment":"Production","market":"Spot","catalog_version":"1",
            "assets":[{"id":"1","version":"1","symbol":"BTC","exponent":-8},{"id":"2","version":"1","symbol":"USDT","exponent":-8}],
            "instruments":[row]});
        let catalog_path = root.join("catalog.json");
        write(&catalog_path, &catalog);
        let hash = Sha256::digest(fs::read(&catalog_path).unwrap());
        let resource = root.join("topic.json");
        let mut spec: Value =
            serde_json::from_str(include_str!("../examples/topic-101.json")).unwrap();
        spec["identity"]["arch"] = if cfg!(target_arch = "aarch64") { 2 } else { 1 }.into();
        write(&resource, &spec);
        let cfg = root.join("md.json");
        let mut config = json!({"source":source,"resource":resource,"catalog":{"path":catalog_path,
            "reference":format!("artifact://lab/spot@sha256:{hash:x}")},"clock_generation":1,"source_id":1,"instrument_set_version":1,
            "server":{"socket":root.join("md.sock"),"admission_directory":admissions,"control_directory":root.join("control"),
            "control_stream":([1u8;16]),"control_domain":([2u8;16]),"control_writer_epoch":1}});
        let mut command = Command::new(env!("CARGO_BIN_EXE_tickwire-md"));
        command.arg("--spot-market").arg(&cfg);
        config["source"]["instrument_version"] = 2.into();
        write(&cfg, &config);
        let rejected = command.output().unwrap();
        assert!(!rejected.status.success());
        assert!(String::from_utf8_lossy(&rejected.stderr).contains("catalog"));
        assert!(!root.join("control").exists());

        config["source"]["instrument_version"] = 1.into();
        write(&cfg, &config);
        let mut owner = Running(command.stderr(Stdio::piped()).spawn().unwrap());
        let header = root.join("control/spot-market.active");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !header.exists() {
            assert!(
                owner.0.try_wait().unwrap().is_none(),
                "MD exited before exporting its resource"
            );
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        let image = ResourceHeader::decode(&fs::read(&header).unwrap()).unwrap();
        assert_eq!(image.topic_id, 101);
        assert_ne!(image.required_features & EXACT_MARKET_EVENTS, 0);
        drop(owner);
        config["server"]["socket"] = json!(root.join("restart.sock"));
        write(&cfg, &config);
        let rejected = command.output().unwrap();
        assert!(!rejected.status.success());
        assert!(String::from_utf8_lossy(&rejected.stderr).contains("fresh ring epoch"));
    }
}
