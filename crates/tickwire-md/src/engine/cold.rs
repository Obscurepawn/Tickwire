//! One bounded cold housekeeping worker (`md-cold`).
//!
//! Spawned via [`tickwire_control_sys::spawn_pinned_thread`] before the reactor pins itself
//! to `reactor_cpu`, so DNS, TLS root loading, snapshot preparation, and expansion file I/O
//! execute on `housekeeping_cpus` even when the parent process starts under a single-core
//! `taskset` mask.
use super::*;
use std::{
    path::PathBuf,
    sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    thread::JoinHandle,
};
use tickwire_control_sys::CpuSet;

pub(super) enum Task {
    Read(Vec<(PathBuf, usize)>),
    Write(Vec<(PathBuf, Vec<u8>)>),
    Install {
        directory: PathBuf,
        admissions: Vec<crate::server::Admission>,
        files: Vec<(PathBuf, Vec<u8>)>,
    },
    Build {
        feeds: Vec<config::Feed>,
        index: config::FeedIndex,
        domain: tickwire_clock::ClockDomain,
        tokens: Vec<owners::Reserved>,
    },
}

pub(super) enum Output {
    Read(Vec<Option<Vec<u8>>>),
    Written,
    Built(Vec<owners::Built>),
}

pub(super) struct Worker {
    submit: Option<SyncSender<Task>>,
    replies: Receiver<std::result::Result<Output, String>>,
    handle: Option<JoinHandle<()>>,
    tid: u32,
    busy: bool,
}

fn current_linux_tid() -> u32 {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_link("/proc/thread-self")
            .ok()
            .and_then(|p| p.file_name()?.to_str()?.parse::<u32>().ok())
            .unwrap_or(0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

impl Worker {
    pub(super) fn new(housekeeping_cpus: Option<&CpuSet>) -> Result<Self> {
        let (submit, requests) = mpsc::sync_channel::<Task>(1);
        let (reply, replies) = mpsc::sync_channel::<std::result::Result<Output, String>>(1);
        let (tid_tx, tid_rx) = mpsc::sync_channel::<u32>(1);
        let handle =
            tickwire_control_sys::spawn_pinned_thread("md-cold", housekeeping_cpus, move || {
                let _ = tid_tx.send(current_linux_tid());
                while let Ok(task) = requests.recv() {
                    let result = execute(task).map_err(|error| error.to_string());
                    if reply.send(result).is_err() {
                        break;
                    }
                }
            })?;
        let tid = tid_rx
            .recv()
            .map_err(|_| invalid("md-cold failed to report startup tid"))?;
        Ok(Self {
            submit: Some(submit),
            replies,
            handle: Some(handle),
            tid,
            busy: false,
        })
    }

    pub(super) fn tid(&self) -> u32 {
        self.tid
    }

    pub(super) fn run_blocking(&mut self, task: Task) -> Result<Output> {
        self.submit(task)?;
        match self.replies.recv() {
            Ok(result) => {
                self.busy = false;
                result.map_err(|message| invalid(&message))
            }
            Err(_) => {
                self.busy = false;
                Err(invalid("MD cold worker closed"))
            }
        }
    }

    pub(super) fn submit(&mut self, task: Task) -> Result<()> {
        if self.busy {
            return Err(invalid("MD cold worker busy"));
        }
        let sender = self
            .submit
            .as_ref()
            .ok_or_else(|| invalid("MD cold worker closed"))?;
        sender.try_send(task).map_err(|error| match error {
            TrySendError::Full(_) => invalid("MD cold queue full"),
            TrySendError::Disconnected(_) => invalid("MD cold worker closed"),
        })?;
        self.busy = true;
        Ok(())
    }

    pub(super) fn poll(&mut self) -> Result<Option<std::result::Result<Output, String>>> {
        if !self.busy {
            return Ok(None);
        }
        match self.replies.try_recv() {
            Ok(result) => {
                self.busy = false;
                Ok(Some(result))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(invalid("MD cold worker closed")),
        }
    }

    pub(super) fn busy(&self) -> bool {
        self.busy
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.submit.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn execute(task: Task) -> Result<Output> {
    match task {
        Task::Read(paths) => {
            let mut values = Vec::with_capacity(paths.len());
            for (path, maximum) in paths {
                match config::read(&path, maximum) {
                    Ok(bytes) => values.push(Some(bytes)),
                    Err(error)
                        if error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        values.push(None)
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(Output::Read(values))
        }
        Task::Write(files) => {
            for (path, bytes) in files {
                config::write_bytes(&path, &bytes)?;
            }
            Ok(Output::Written)
        }
        Task::Install {
            directory,
            admissions,
            files,
        } => {
            for admission in admissions {
                crate::server::register_admission(&directory, &admission)?;
            }
            for (path, bytes) in files {
                config::write_bytes(&path, &bytes)?;
            }
            Ok(Output::Written)
        }
        Task::Build {
            feeds,
            index,
            domain,
            tokens,
        } => {
            if feeds.len() != tokens.len() {
                return Err(invalid("MD owner token binding"));
            }
            Ok(Output::Built(
                feeds
                    .iter()
                    .zip(tokens)
                    .map(|(feed, tokens)| owners::build(feed, domain, &index, tokens))
                    .collect::<Result<_>>()?,
            ))
        }
    }
}
