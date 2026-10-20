//! Borrowed calibrated-clock adapter for the private network owner.
#![forbid(unsafe_code)]
use tickwire_binance_clock::{BinanceTimeClient, MonotonicClock};
pub use tickwire_binance_private::{AccountScope, Clock, ClockSample, Error, Result, SignedTime};
use tickwire_binance_private::{Environment, Market};
use tickwire_clock::{ClockDomain, Source};

pub trait DeliveryBudget {
    /// Upper bound from the final send check to remote timestamp validation.
    fn remaining_ns(
        &mut self,
        scope: &AccountScope,
        stamp: &SignedTime,
        now: &ClockSample,
    ) -> Result<u64>;
}
impl<F: FnMut(&AccountScope, &SignedTime, &ClockSample) -> Result<u64>> DeliveryBudget for F {
    fn remaining_ns(&mut self, s: &AccountScope, t: &SignedTime, n: &ClockSample) -> Result<u64> {
        self(s, t, n)
    }
}
pub struct ClockAdapter<'a, C: MonotonicClock, B: DeliveryBudget> {
    time: &'a mut BinanceTimeClient<C>,
    scope: AccountScope,
    budget: &'a mut B,
    domain: ClockDomain,
    source: Source,
}
impl<C: MonotonicClock, B: DeliveryBudget> std::fmt::Debug for ClockAdapter<'_, C, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClockAdapter")
            .field("scope", &self.scope)
            .field("source", &self.source)
            .field("domain", &self.domain)
            .finish_non_exhaustive()
    }
}
impl<'a, C: MonotonicClock, B: DeliveryBudget> ClockAdapter<'a, C, B> {
    pub fn new(
        time: &'a mut BinanceTimeClient<C>,
        scope: AccountScope,
        budget: &'a mut B,
    ) -> Result<Self> {
        scope.validate()?;
        let source = match (scope.market, scope.environment) {
            (Market::Spot, Environment::Production) => Source::SpotProduction,
            (Market::Spot, Environment::Test) => Source::SpotTest,
            (Market::Usdm, Environment::Production) => Source::UsdmProduction,
            (Market::Usdm, Environment::Test) => Source::UsdmTest,
        };
        if time.source() != source {
            return Err(Error::Identity);
        }
        let domain = time.domain();
        Ok(Self {
            time,
            scope,
            budget,
            domain,
            source,
        })
    }
}
impl<C: MonotonicClock, B: DeliveryBudget> Clock for ClockAdapter<'_, C, B> {
    fn sample(&mut self) -> Result<ClockSample> {
        let now = self.time.sample().map_err(clock_error)?;
        if now.local.monotonic.domain() != self.domain {
            return Err(Error::ClockDomain);
        }
        if now.signed.is_some_and(|t| t.domain() != self.domain) {
            return Err(Error::ClockDomain);
        }
        if now.signed.is_some_and(|t| t.source() != self.source) {
            return Err(Error::Identity);
        }
        Ok(ClockSample {
            instant: now.local.instant,
            monotonic: now.local.monotonic,
            signing_time: now.signed,
        })
    }
    fn check_signed(
        &mut self,
        scope: &AccountScope,
        stamp: &SignedTime,
        now: &ClockSample,
        window_us: u64,
    ) -> Result<()> {
        if *scope != self.scope || stamp.source() != self.source {
            return Err(Error::Identity);
        }
        if stamp.domain() != self.domain || now.monotonic.domain() != self.domain {
            return Err(Error::ClockDomain);
        }
        let window_ns = window_us
            .checked_mul(1000)
            .filter(|n| *n > 0)
            .ok_or(Error::Bounds)?;
        let fresh = self.sample()?;
        if fresh.monotonic.nanos() < now.monotonic.nanos() || fresh.instant < now.instant {
            return Err(Error::ClockDomain);
        }
        let budget_ns = self.budget.remaining_ns(scope, stamp, &fresh)?;
        // check_send obtains another local sample after the provider returns.
        self.time
            .check_send(stamp, window_ns, budget_ns)
            .map_err(clock_error)
    }
}
fn clock_error(error: tickwire_binance_clock::Error) -> Error {
    use tickwire_binance_clock::Error as Client;
    use tickwire_clock::Error as Core;
    match error {
        Client::Clock(
            Core::Domain | Core::TimeBackwards | Core::Faulted | Core::ClockJump | Core::Overflow,
        ) => Error::ClockDomain,
        Client::Exhausted => Error::ClockDomain,
        _ => Error::Clock,
    }
}
