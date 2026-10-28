//! Owned child processes and authenticated control connections.
#![forbid(unsafe_code)]
mod observation;
pub use observation::{LiveChildObservation, current_process_identity};
mod resources;
pub use resources::{LiveResourceObservation, ResourceLimits, ResourcePlan};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::PathBuf,
    process::{Child, ExitStatus, Stdio},
    time::Instant,
};
use tickwire_control::{Identity, Policy, ProcessIdentity, Session};
use tickwire_control_sys::{Connection, ControlListener, ExecutableImage};

#[derive(Debug)]
pub enum Error {
    Invalid,
    Conflict,
    Capacity,
    Exited,
    Identity,
    Busy,
    ResourcesRequired,
    Io(std::io::Error),
    Control(tickwire_control::Error),
    Transport(tickwire_control_sys::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone)]
pub struct LaunchSpec {
    pub identity: Identity,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub working_directory: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub uid: u32,
    pub gid: u32,
    pub inherit_output: bool,
}
struct Entry {
    child: Child,
    image: ExecutableImage,
    resources: Option<tickwire_control_sys::ResourceDomain>,
    identity: Identity,
    uid: u32,
    gid: u32,
    connected: bool,
    last_control_epoch: u64,
    exit: Option<ExitStatus>,
}
pub struct Registry {
    entries: BTreeMap<[u8; 16], Entry>,
    capacity: usize,
}
/// This proves only that this directly owned child exited. It is deliberately
/// not a proof that all descendants released shared-memory resources.
pub struct ExitObservation {
    instance: [u8; 16],
    generation: u64,
    pid: u32,
    status: ExitStatus,
}
impl ExitObservation {
    pub fn instance(&self) -> [u8; 16] {
        self.instance
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn pid(&self) -> u32 {
        self.pid
    }
    pub fn status(&self) -> ExitStatus {
        self.status
    }
}
pub struct AuthenticatedChild {
    pub connection: Connection,
    pub session: Session,
    pub process: ProcessIdentity,
}
impl Registry {
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 || capacity > 4096 {
            return Err(Error::Capacity);
        }
        Ok(Self {
            entries: BTreeMap::new(),
            capacity,
        })
    }
    pub fn spawn(&mut self, spec: LaunchSpec) -> Result<ProcessIdentity> {
        self.spawn_inner(spec, None)
    }
    fn spawn_inner(
        &mut self,
        spec: LaunchSpec,
        resources: Option<ResourcePlan>,
    ) -> Result<ProcessIdentity> {
        if self.entries.len() >= self.capacity {
            return Err(Error::Capacity);
        }
        if spec.identity.instance == [0; 16]
            || spec.identity.generation == 0
            || !spec.executable.is_absolute()
            || !spec.working_directory.is_absolute()
            || spec.arguments.len() > 128
            || spec.environment.len() > 128
            || spec
                .arguments
                .iter()
                .any(|s| s.len() > 16384 || s.contains('\0'))
            || spec.environment.iter().any(|(k, v)| {
                k.is_empty()
                    || k.contains(['=', '\0'])
                    || v.contains('\0')
                    || k.len() > 256
                    || v.len() > 65536
            })
        {
            return Err(Error::Invalid);
        }
        if self.entries.contains_key(&spec.identity.instance) {
            return Err(Error::Conflict);
        }
        let image =
            ExecutableImage::open(&spec.executable, 512 * 1024 * 1024).map_err(Error::Transport)?;
        // The digest is over the immutable image that will actually execute,
        // never a pathname that may name different bytes by the time of exec.
        let mut file = image.reader().map_err(Error::Transport)?;
        let mut sha = Sha256::new();
        let mut count = 0u64;
        let mut buffer = [0u8; 65536];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            count = count
                .checked_add(n as u64)
                .filter(|n| *n <= image.len())
                .ok_or(Error::Invalid)?;
            sha.update(&buffer[..n]);
        }
        let digest: [u8; 32] = sha.finalize().into();
        if count != image.len() || digest != spec.identity.build_hash {
            return Err(Error::Identity);
        }
        let mut resources = resources
            .map(|plan| tickwire_control_sys::ResourceDomain::acquire(&plan.cgroup, plan.limits))
            .transpose()
            .map_err(Error::Transport)?;
        let configure = |command: &mut std::process::Command| {
            command
                .args(spec.arguments)
                .current_dir(spec.working_directory)
                .env_clear()
                .envs(spec.environment)
                .stdin(Stdio::null());
            if spec.inherit_output {
                command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
            } else {
                command.stdout(Stdio::null()).stderr(Stdio::null());
            }
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                command.arg0(&spec.executable).uid(spec.uid).gid(spec.gid);
            }
        };
        let child = match resources.as_mut() {
            Some(domain) => domain.spawn(&image, configure),
            None => image.spawn(configure),
        }
        .map_err(Error::Transport)?;
        let process = ProcessIdentity {
            pid: child.id(),
            uid: spec.uid,
            gid: spec.gid,
        };
        self.entries.insert(
            spec.identity.instance,
            Entry {
                child,
                image,
                resources,
                identity: spec.identity,
                uid: spec.uid,
                gid: spec.gid,
                connected: false,
                last_control_epoch: 0,
                exit: None,
            },
        );
        Ok(process)
    }
    pub fn process(&mut self, instance: [u8; 16]) -> Result<ProcessIdentity> {
        let e = self.entries.get_mut(&instance).ok_or(Error::Identity)?;
        if e.exit.is_none() {
            e.exit = e.child.try_wait()?;
        }
        if e.exit.is_some() {
            return Err(Error::Exited);
        }
        Ok(ProcessIdentity {
            pid: e.child.id(),
            uid: e.uid,
            gid: e.gid,
        })
    }
    /// Child connects after exec. Kernel credentials must match the still-owned
    /// Child; a parent-created socketpair is not used as child identity evidence.
    pub fn accept(
        &mut self,
        listener: &ControlListener,
        mut policy: Policy,
        now: u64,
        deadline: Instant,
    ) -> Result<AuthenticatedChild> {
        let expected = self.process(policy.peer.instance)?;
        let e = self
            .entries
            .get_mut(&policy.peer.instance)
            .ok_or(Error::Identity)?;
        if e.connected || policy.peer != e.identity || policy.control_epoch <= e.last_control_epoch
        {
            return Err(Error::Conflict);
        }
        if policy.peer_process != expected {
            return Err(Error::Identity);
        }
        let mut connection = listener.accept(deadline).map_err(Error::Transport)?;
        let actual = connection.peer_credentials();
        let actual = ProcessIdentity {
            pid: actual.pid,
            uid: actual.uid,
            gid: actual.gid,
        };
        if actual != expected {
            return Err(Error::Identity);
        }
        let hello = connection.recv(deadline).map_err(Error::Transport)?;
        if !hello.fds().is_empty() {
            return Err(Error::Identity);
        }
        if !e
            .image
            .matches_process(actual.pid)
            .map_err(Error::Transport)?
        {
            return Err(Error::Identity);
        }
        policy.peer_process = actual;
        let (session, welcome) =
            Session::accept(&policy, actual, hello.bytes(), now).map_err(Error::Control)?;
        connection
            .send(&welcome, &[], deadline)
            .map_err(Error::Transport)?;
        // If the process exited during authentication, do not publish its session.
        if let Some(status) = e.child.try_wait()? {
            e.exit = Some(status);
            return Err(Error::Exited);
        }
        if !e
            .image
            .matches_process(actual.pid)
            .map_err(Error::Transport)?
        {
            return Err(Error::Identity);
        }
        // Sampling /proc may race an exit; do not publish a dead child's session.
        if let Some(status) = e.child.try_wait()? {
            e.exit = Some(status);
            return Err(Error::Exited);
        }
        e.connected = true;
        e.last_control_epoch = policy.control_epoch;
        Ok(AuthenticatedChild {
            connection,
            session,
            process: actual,
        })
    }
    /// Reconnection requires a later epoch; it does not renew trading authority.
    pub fn disconnected(&mut self, instance: [u8; 16], epoch: u64) -> Result<()> {
        let e = self.entries.get_mut(&instance).ok_or(Error::Identity)?;
        if epoch != e.last_control_epoch {
            return Err(Error::Identity);
        }
        e.connected = false;
        Ok(())
    }
    pub fn observe_exits(&mut self) -> Result<Vec<ExitObservation>> {
        let mut out = Vec::new();
        for (instance, e) in &mut self.entries {
            if e.exit.is_some() {
                continue;
            }
            if let Some(status) = e.child.try_wait()? {
                e.exit = Some(status);
                e.connected = false;
                out.push(ExitObservation {
                    instance: *instance,
                    generation: e.identity.generation,
                    pid: e.child.id(),
                    status,
                });
            }
        }
        Ok(out)
    }
    /// Signals only a child owned by this Registry, never a recovered bare PID.
    pub fn terminate(&mut self, instance: [u8; 16]) -> Result<ExitObservation> {
        let e = self.entries.get_mut(&instance).ok_or(Error::Identity)?;
        let status = if let Some(status) = e.exit {
            status
        } else {
            if let Some(status) = e.child.try_wait()? {
                status
            } else {
                e.child.kill()?;
                e.child.wait()?
            }
        };
        e.exit = Some(status);
        e.connected = false;
        Ok(ExitObservation {
            instance,
            generation: e.identity.generation,
            pid: e.child.id(),
            status,
        })
    }
}
impl Drop for Registry {
    fn drop(&mut self) {
        for e in self.entries.values_mut() {
            if e.exit.is_none() {
                if matches!(e.child.try_wait(), Ok(Some(_))) {
                    continue;
                }
                let _ = e.child.kill();
                let _ = e.child.wait();
            }
        }
    }
}
