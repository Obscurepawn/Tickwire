use super::*;

/// Process-local readiness identities. Cancelled owners never return tokens to the pool.
#[derive(Default)]
pub(super) struct Tokens {
    next: usize,
}
impl Tokens {
    fn reserve(&mut self, count: usize) -> Result<usize> {
        let next = self
            .next
            .checked_add(count)
            .ok_or_else(|| invalid("MD token capacity"))?;
        let first = self.next;
        self.next = next;
        Ok(first)
    }
    pub(super) fn depth(&mut self) -> Result<public::IoTokens> {
        let first = self.reserve(2)?;
        Ok(public::IoTokens {
            stream: public::mio::Token(first),
            snapshot: public::mio::Token(first + 1),
        })
    }
    pub(super) fn event(&mut self) -> Result<public::mio::Token> {
        Ok(public::mio::Token(self.reserve(1)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn appended_depth_never_overlaps_existing_events_or_cancelled_owners() {
        let mut tokens = Tokens::default();
        let old = tokens.depth().unwrap();
        let event = tokens.event().unwrap();
        let cancelled = tokens.depth().unwrap();
        let next = tokens.depth().unwrap();
        let later_event = tokens.event().unwrap();
        let ids = [
            old.stream,
            old.snapshot,
            event,
            cancelled.stream,
            cancelled.snapshot,
            next.stream,
            next.snapshot,
            later_event,
        ];
        let unique = ids
            .into_iter()
            .map(|token| token.0)
            .collect::<BTreeSet<_>>();
        assert_eq!(unique.len(), ids.len());
        assert!(event.0 < next.stream.0);
        assert!(cancelled.snapshot.0 < next.stream.0);
    }
    #[test]
    fn exhausted_tokens_do_not_wrap_or_partially_advance() {
        let mut tokens = Tokens {
            next: usize::MAX - 1,
        };
        assert!(tokens.depth().is_err());
        assert_eq!(tokens.event().unwrap().0, usize::MAX - 1);
        assert!(tokens.event().is_err());
    }
}
