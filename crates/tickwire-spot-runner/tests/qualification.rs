use tickwire_spot_runner::reconcile::PrivateCut;

#[test]
fn any_private_event_or_reconnect_during_the_two_rest_reads_invalidates_the_cut() {
    let cut = PrivateCut {
        stream: [3; 16],
        epoch: 2,
        gap_generation: 1,
        through: 4,
    };
    assert!(PrivateCut::stable([cut, cut]).is_ok());
    for moved in [
        PrivateCut { through: 5, ..cut },
        PrivateCut { epoch: 3, ..cut },
        PrivateCut {
            gap_generation: 2,
            ..cut
        },
        PrivateCut {
            stream: [4; 16],
            ..cut
        },
    ] {
        assert!(PrivateCut::stable([cut, moved]).is_err());
    }
    assert!(PrivateCut::stable([PrivateCut { through: 0, ..cut }; 2]).is_err());
}
