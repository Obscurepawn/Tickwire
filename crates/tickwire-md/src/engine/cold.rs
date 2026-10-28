//! One bounded cold worker. The runtime never waits for DNS or filesystem completion.
use super::*;
use std::{
    path::PathBuf,
    sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
};

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
    submit: SyncSender<Task>,
    replies: Receiver<std::result::Result<Output, String>>,
    busy: bool,
}
impl Worker {
    pub(super) fn new() -> Result<Self> {
        let (submit, requests) = mpsc::sync_channel(1);
        let (reply, replies) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("md-cold".into())
            .stack_size(8 << 20)
            .spawn(move || {
                while let Ok(task) = requests.recv() {
                    let result = execute(task).map_err(|error| error.to_string());
                    if reply.send(result).is_err() {
                        break;
                    }
                }
            })?;
        Ok(Self {
            submit,
            replies,
            busy: false,
        })
    }
    pub(super) fn submit(&mut self, task: Task) -> Result<()> {
        if self.busy {
            return Err(invalid("MD cold worker busy"));
        }
        self.submit.try_send(task).map_err(|error| match error {
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
