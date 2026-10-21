//! The common retained-input verifier is also used by Host and target install.
#[cfg(test)]
use super::*;
pub use tickwire_release::replay::retained::{RecordedCut, decode};
#[cfg(test)]
mod tests {
    use super::*;
    fn tag(prefix: &[u8; 8], bytes: &[u8]) -> Vec<u8> {
        [prefix.as_slice(), bytes].concat()
    }
    fn wrap(rows: &[Vec<u8>]) -> Vec<u8> {
        tickwire_recovery::encode_inputs(
            [7; 16],
            1,
            [0; 32],
            &rows
                .iter()
                .map(|r| tickwire_recovery::InputRecord {
                    kind: 5,
                    payload: r,
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .bytes
    }
    #[test]
    fn retained_closure_reconstructs_the_exact_s_to_c_input_tail() {
        let state = super::super::tests::Stateful::initial();
        let a = super::super::tests::anchor_with(state.clone());
        let mut e = Engine::new(&a, state).unwrap();
        e.apply_live(
            Input::ConfigApply {
                previous: Object::of(&[1]),
                next: VersionedBytes {
                    revision: 2,
                    bytes: vec![9],
                },
            },
            11,
        )
        .unwrap();
        let c = e.checkpoint().unwrap();
        let rows = vec![
            tag(b"TWM2A001", &a.checkpoint().bytes().unwrap()),
            tag(b"TWM2R001", &canonical(&e.recorded()[0]).unwrap()),
            tag(b"TWM2C001", &c.bytes().unwrap()),
        ];
        let read = decode(&wrap(&rows)).unwrap();
        assert_eq!(read.current(), &c);
        assert_eq!(
            read.tail_from(a.checkpoint()).unwrap(),
            encode_tail(&a, e.recorded()).unwrap()
        );
        assert!(
            decode(&wrap(&rows[..2])).is_err(),
            "a callback without a completed checkpoint is not a cut"
        );
        let mut reordered = rows.clone();
        reordered.swap(1, 2);
        assert!(
            decode(&wrap(&reordered)).is_err(),
            "an older completion cannot seal a newer callback"
        );
        let mut duplicate = rows.clone();
        duplicate.insert(1, rows[0].clone());
        assert!(decode(&wrap(&duplicate)).is_err());
        let mut wrong = c.clone();
        wrong.source.generation += 1;
        let mut changed = rows;
        changed[2] = tag(b"TWM2C001", &wrong.bytes().unwrap());
        assert!(decode(&wrap(&changed)).is_err());
    }
}
