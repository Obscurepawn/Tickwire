use std::process::Command;

#[test]
fn help_names_the_live_account_deployment_entrypoint() {
    let output = Command::new(env!("CARGO_BIN_EXE_tickwire-spot-runner"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("live <CONFIG.json>")
    );
}
