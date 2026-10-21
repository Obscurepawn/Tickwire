//! Local file/journal observations. This layer does not verify control or runtime authority.
#![forbid(unsafe_code)]
use tickwire_config::projection_object::ProjectionObject;
use tickwire_config_journal::{FileFact, Hash, Identity, Journal, Prepared, Receipt, RuntimeFact};
use tickwire_journal::store::Store as JournalStore;
mod local;

#[derive(Debug)]
pub enum Error {
    Invalid(&'static str),
    Conflict,
    Drift,
    Poisoned,
    File(tickwire_config_fs::Error),
    Journal(tickwire_config_journal::Error),
    Configuration(tickwire_config::Error),
}
pub type Result<T> = core::result::Result<T, Error>;
impl From<tickwire_config_fs::Error> for Error {
    fn from(e: tickwire_config_fs::Error) -> Self {
        Self::File(e)
    }
}
impl From<tickwire_config_journal::Error> for Error {
    fn from(e: tickwire_config_journal::Error) -> Self {
        Self::Journal(e)
    }
}
impl From<tickwire_config::Error> for Error {
    fn from(e: tickwire_config::Error) -> Self {
        Self::Configuration(e)
    }
}
/// Trusted persistence boundary. `install` returns only after file and directory sync.
pub trait Files {
    fn create_directory(&mut self, path: &str) -> Result<()>;
    fn read(&mut self, path: &str, max_bytes: usize) -> Result<Option<Vec<u8>>>;
    fn install(&mut self, path: &str, bytes: &[u8], previous: Option<Hash>) -> Result<FileFact>;
}
impl Files for tickwire_config_fs::Store {
    fn create_directory(&mut self, path: &str) -> Result<()> {
        Ok(self.create_directory(path)?)
    }
    fn read(&mut self, path: &str, max_bytes: usize) -> Result<Option<Vec<u8>>> {
        match tickwire_config_fs::Store::read(self, path, max_bytes) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(tickwire_config_fs::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }
    fn install(&mut self, path: &str, bytes: &[u8], previous: Option<Hash>) -> Result<FileFact> {
        let f = tickwire_config_fs::Store::install(self, path, bytes, previous)?;
        Ok(FileFact {
            render_hash: f.hash(),
            bytes: f.bytes() as u64,
            device: f.device(),
            inode: f.inode(),
        })
    }
}
pub struct Local<F: Files, S: JournalStore> {
    files: F,
    journal: Journal<S>,
    poisoned: bool,
}
impl<F: Files, S: JournalStore> Local<F, S> {
    pub fn journal(&self) -> &Journal<S> {
        &self.journal
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
}
