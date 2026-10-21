//! Actual retained Linux installation; requires an explicit read-only fixture.
#[test]
#[ignore = "requires TICKWIRE_INSTALL_DELTA_FIXTURE"]
fn actual_installed_delta_history_is_verified_in_full() {
    use std::{fs, path::PathBuf};
    use tickwire_release::replay::install;
    let directory = PathBuf::from(std::env::var_os("TICKWIRE_INSTALL_DELTA_FIXTURE").unwrap());
    let root = [
        0x9c, 0x13, 0x4f, 0xfd, 0x07, 0x4c, 0x92, 0xb1, 0xa3, 0xfc, 0x34, 0x12, 0xa9, 0x13, 0x98,
        0xc3, 0x25, 0xa5, 0x05, 0x89, 0xc8, 0x36, 0x5c, 0x12, 0x65, 0xd2, 0x9a, 0xb8, 0x2e, 0xa1,
        0xdf, 0xdd,
    ];
    let value = install::verify(root, |hash| {
        let hex = hash.iter().map(|b| format!("{b:02x}")).collect::<String>();
        fs::read(directory.join(format!("object-{hex}")))
            .map_err(|_| tickwire_release::Error::Missing)
    });
    assert!(
        value.is_ok(),
        "actual completed installation: {:?}",
        value.as_ref().err()
    );
    let value = value.unwrap();
    assert_eq!(value.checkpoint.cut.sequence, 6);
    assert_eq!(value.initial_checkpoint().unwrap().cut.sequence, 0);
    assert_eq!(value.checkpoint.runtime.markets[0].sequence, 12);
    assert_eq!(value.checkpoint.runtime.accounts[0].sequence, 3);
    assert_ne!(
        value.request.target.input_stream,
        value.request.member.new_instance
    );
    eprintln!(
        "actual installed delta verified: objects={} final_seq={} market_seq={} account_seq={}",
        value.objects.len(),
        value.checkpoint.cut.sequence,
        value.checkpoint.runtime.markets[0].sequence,
        value.checkpoint.runtime.accounts[0].sequence
    );
}
