use super::*;
pub(crate) const LIMIT: usize = 4096;
pub(crate) struct Prefixes {
    rows: Vec<(u64, [u8; 32])>,
}
impl Prefixes {
    pub(crate) fn new(position: u64, digest: [u8; 32]) -> Self {
        {
            let mut rows = Vec::with_capacity(LIMIT);
            rows.push((position, digest));
            Self { rows }
        }
    }
    pub(crate) fn record(&mut self, position: u64, digest: [u8; 32]) -> Result<()> {
        let &(last, hash) = self.rows.last().ok_or(Error::Invalid)?;
        if position == last {
            return if digest == hash {
                Ok(())
            } else {
                Err(Error::Invalid)
            };
        }
        if last.checked_add(1) != Some(position) {
            return Err(Error::Invalid);
        }
        if self.rows.len() == LIMIT {
            return Err(Error::ObservationCapacity);
        }
        self.rows.push((position, digest));
        Ok(())
    }
    pub(crate) fn get(&self, position: u64) -> Option<[u8; 32]> {
        self.rows.iter().find(|r| r.0 == position).map(|r| r.1)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ongoing_reports_preserve_the_exact_earlier_host_prefix() {
        let mut p = Prefixes::new(3, [3; 32]);
        p.record(4, [4; 32]).unwrap();
        p.record(5, [5; 32]).unwrap();
        assert_eq!(p.get(4), Some([4; 32]));
        assert_eq!(p.get(3), Some([3; 32]));
        assert_eq!(p.get(6), None);
    }
    #[test]
    fn duplicate_is_idempotent_but_gap_substitution_and_capacity_fail() {
        let mut p = Prefixes::new(0, [1; 32]);
        p.record(0, [1; 32]).unwrap();
        assert!(p.record(0, [2; 32]).is_err());
        assert!(p.record(2, [2; 32]).is_err());
        for n in 1..LIMIT as u64 {
            p.record(n, [1; 32]).unwrap();
        }
        assert!(p.record(LIMIT as u64, [1; 32]).is_err());
        assert_eq!(p.get(0), Some([1; 32]));
    }
}
