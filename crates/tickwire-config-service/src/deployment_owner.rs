//! Bounded asynchronous source validation with service-owned time.
use crate::{
    Error, Result,
    bootstrap::CommittedHead,
    deployment::{ContentValidated, Limits},
    evidence::Trust,
    source_bundle::Bundle,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver, TryRecvError},
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
use tickwire_config::{
    Candidate,
    delivery::ProjectionDelivery,
    plan::{ComputedPlan, PlanOptions},
    prepared::PreparedCandidate,
};

struct Context {
    bundle: Bundle,
    trust: Trust,
    limits: Limits,
}
pub struct Owner {
    context: Arc<Context>,
    in_flight: Arc<AtomicUsize>,
    max_jobs: usize,
    last_unix_ms: u64,
}
struct Lease {
    in_flight: Arc<AtomicUsize>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}
struct JobCore<T> {
    context: Arc<Context>,
    receiver: Option<Receiver<Result<Completed<T>>>>,
    lease: Option<Arc<Lease>>,
    finished: Arc<AtomicBool>,
    accepted_unix_ms: u64,
}
pub struct Job(JobCore<()>);
/// A completed calculation bound to the exact owner context that produced it.
pub struct Assessment {
    context: Arc<Context>,
    content: ContentValidated,
}
pub struct PlanJob(JobCore<PlanResult>);
impl PlanJob {
    pub fn computation_finished(&self) -> bool {
        self.0.finished.load(Ordering::Acquire)
    }
}
struct PlanResult {
    base: CommittedHead,
    prepared: PreparedCandidate,
    delivery: ProjectionDelivery,
    plan: ComputedPlan,
}
/// Retained content calculation; runtime acceptance and commit authority are separate.
pub struct PlanAssessment {
    context: Arc<Context>,
    result: PlanResult,
    content: ContentValidated,
}
impl PlanAssessment {
    pub fn base(&self) -> &CommittedHead {
        &self.result.base
    }
    pub fn prepared(&self) -> &PreparedCandidate {
        &self.result.prepared
    }
    pub fn content(&self) -> &ContentValidated {
        &self.content
    }
    pub fn delivery(&self) -> &ProjectionDelivery {
        &self.result.delivery
    }
    pub fn plan(&self) -> &ComputedPlan {
        &self.result.plan
    }
}
impl Assessment {
    pub fn content(&self) -> &ContentValidated {
        &self.content
    }
}
struct Completed<T> {
    value: T,
    checked: ContentValidated,
    finished_unix_ms: u64,
}
struct Finished(Arc<AtomicBool>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
impl Job {
    pub fn computation_finished(&self) -> bool {
        self.0.finished.load(Ordering::Acquire)
    }
}
impl Owner {
    pub fn start_plan(
        &mut self,
        base: CommittedHead,
        prepared: PreparedCandidate,
        options: PlanOptions,
    ) -> Result<PlanJob> {
        if base.candidate().hash() != prepared.base_hash() {
            return Err(Error::Conflict);
        }
        self.start_task(move |context, start| {
            let checked = ContentValidated::check(
                prepared.candidate(),
                context.bundle.sources(),
                &context.trust,
                start,
                context.limits,
            )?;
            // Both results remain retained, so include the content checker’s copy
            // of the new projection set in the same total storage budget.
            let remaining = context
                .limits
                .projections
                .max_total_bytes
                .checked_sub(checked.projections().stored_bytes())
                .filter(|remaining| *remaining != 0)
                .ok_or(Error::Capacity)?;
            let delivery = ProjectionDelivery::prepare(
                base.candidate(),
                &prepared,
                context.bundle.sources().artifacts,
                tickwire_config::delivery::ProjectionLimits {
                    max_total_bytes: remaining,
                    ..context.limits.projections
                },
            )
            .map_err(Error::Configuration)?;
            let plan = ComputedPlan::compute(
                base.revision(),
                base.candidate(),
                &prepared,
                &delivery,
                options,
            )
            .map_err(Error::Configuration)?;
            Ok((
                PlanResult {
                    base,
                    prepared,
                    delivery,
                    plan,
                },
                checked,
            ))
        })
        .map(PlanJob)
    }
    pub fn poll_plan(&mut self, job: &mut PlanJob) -> Result<Option<PlanAssessment>> {
        Ok(self
            .poll_task(&mut job.0)?
            .map(|(result, content)| PlanAssessment {
                context: job.0.context.clone(),
                result,
                content,
            }))
    }
    pub fn revalidate_plan(&mut self, assessment: &PlanAssessment) -> Result<()> {
        self.revalidate_content(&assessment.context, &assessment.content)
    }
    pub fn new(bundle: Bundle, trust: Trust, limits: Limits, max_jobs: usize) -> Result<Self> {
        validate_limits(limits)?;
        if !(1..=8).contains(&max_jobs) {
            return Err(Error::Bounds);
        }
        Ok(Self {
            context: Arc::new(Context {
                bundle,
                trust,
                limits,
            }),
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_jobs,
            last_unix_ms: system_time()?,
        })
    }
    pub fn replace(&mut self, bundle: Bundle, trust: Trust, limits: Limits) -> Result<()> {
        validate_limits(limits)?;
        self.sample()?;
        self.context = Arc::new(Context {
            bundle,
            trust,
            limits,
        });
        Ok(())
    }
    pub fn start(&mut self, candidate: Candidate) -> Result<Job> {
        self.start_task(move |context, start| {
            let checked = ContentValidated::check(
                &candidate,
                context.bundle.sources(),
                &context.trust,
                start,
                context.limits,
            )?;
            Ok(((), checked))
        })
        .map(Job)
    }
    fn start_task<T: Send + 'static>(
        &mut self,
        compute: impl FnOnce(&Context, u64) -> Result<(T, ContentValidated)> + Send + 'static,
    ) -> Result<JobCore<T>> {
        let accepted_unix_ms = self.sample()?;
        self.in_flight
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.max_jobs).then_some(n + 1)
            })
            .map_err(|_| Error::Capacity)?;
        let lease = Arc::new(Lease {
            in_flight: self.in_flight.clone(),
        });
        let worker_lease = lease.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        let context = self.context.clone();
        let worker_context = context.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let worker_finished = finished.clone();
        thread::Builder::new()
            .name("tickwire-deployment-check".into())
            .spawn(move || {
                let _finished = Finished(worker_finished);
                let result = (|| {
                    let start = system_time()?;
                    if start < accepted_unix_ms {
                        return Err(Error::NotReady);
                    }
                    let (value, checked) = compute(&worker_context, start)?;
                    let finished_unix_ms = system_time()?;
                    window(start, finished_unix_ms, checked.expires_unix_ms())?;
                    Ok(Completed {
                        value,
                        checked,
                        finished_unix_ms,
                    })
                })();
                let _ = sender.send(result);
                drop(worker_lease);
            })
            .map_err(|_| Error::Transport)?;
        Ok(JobCore {
            context,
            receiver: Some(receiver),
            lease: Some(lease),
            finished,
            accepted_unix_ms,
        })
    }
    pub fn poll(&mut self, job: &mut Job) -> Result<Option<Assessment>> {
        Ok(self.poll_task(&mut job.0)?.map(|((), content)| Assessment {
            context: job.0.context.clone(),
            content,
        }))
    }
    fn poll_task<T>(&mut self, job: &mut JobCore<T>) -> Result<Option<(T, ContentValidated)>> {
        if !Arc::ptr_eq(&self.context, &job.context) {
            return Err(Error::Conflict);
        }
        let result = match job.receiver.as_ref().ok_or(Error::NotReady)?.try_recv() {
            Ok(result) => result,
            Err(TryRecvError::Empty) => {
                self.sample()?;
                return Ok(None);
            }
            Err(TryRecvError::Disconnected) => Err(Error::Transport),
        };
        job.receiver.take();
        job.lease.take();
        let Completed {
            value,
            checked,
            finished_unix_ms,
        } = result?;
        self.last_unix_ms = self.last_unix_ms.max(finished_unix_ms);
        let now = self.sample()?;
        if checked.checked_unix_ms() < job.accepted_unix_ms {
            return Err(Error::NotReady);
        }
        window(checked.checked_unix_ms(), now, checked.expires_unix_ms())?;
        Ok(Some((value, checked)))
    }
    pub fn revalidate(&mut self, assessment: &Assessment) -> Result<()> {
        self.revalidate_content(&assessment.context, &assessment.content)
    }
    fn revalidate_content(
        &mut self,
        context: &Arc<Context>,
        content: &ContentValidated,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.context, context) {
            return Err(Error::Conflict);
        }
        window(
            content.checked_unix_ms(),
            self.sample()?,
            content.expires_unix_ms(),
        )
    }
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
    fn sample(&mut self) -> Result<u64> {
        let now = system_time()?;
        if now < self.last_unix_ms {
            return Err(Error::NotReady);
        }
        self.last_unix_ms = now;
        Ok(now)
    }
}
fn validate_limits(limits: Limits) -> Result<()> {
    if [
        limits.accounts.max_age_ms,
        limits.credentials.max_age_ms,
        limits.resources.max_age_ms,
    ]
    .iter()
    .any(|v| !(1..=60_000).contains(v))
        || !(1..=4096).contains(&limits.projections.max_targets)
        || !(1..=512 * 1024 * 1024).contains(&limits.projections.max_total_bytes)
    {
        return Err(Error::Bounds);
    }
    Ok(())
}
fn system_time() -> Result<u64> {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::NotReady)?
        .as_millis();
    if ms == 0 || ms > (1u128 << 53) - 1 {
        return Err(Error::NotReady);
    }
    Ok(ms as u64)
}
fn window(start: u64, now: u64, expires: u64) -> Result<()> {
    if start == 0 || now < start || now >= expires {
        Err(Error::NotReady)
    } else {
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completion_window_rejects_clock_regression_and_exact_expiry() {
        assert!(window(100, 100, 101).is_ok());
        assert!(window(100, 99, 101).is_err());
        assert!(window(100, 101, 101).is_err());
        assert!(window(0, 1, 2).is_err());
    }
    #[test]
    fn task_capacity_is_owned_until_both_worker_and_client_release() {
        let count = Arc::new(AtomicUsize::new(1));
        let client = Arc::new(Lease {
            in_flight: count.clone(),
        });
        let worker = client.clone();
        drop(client);
        assert_eq!(count.load(Ordering::Acquire), 1);
        drop(worker);
        assert_eq!(count.load(Ordering::Acquire), 0);
    }
}
