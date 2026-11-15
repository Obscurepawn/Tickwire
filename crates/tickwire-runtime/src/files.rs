//! Cold-startup and off-thread `SIGUSR1` static Risk Management System (`RMS`) configuration file
//! loader for [`crate::OrderRoutingSystem`] and [`crate::OrsProcess`].
//!
//! All file I/O and [`RmsConfigTable`] parsing execute strictly off the `REQ`/`RESP` hot paths on
//! `housekeeping_cpus`. If reading, parsing, or validating a reloaded RMS configuration fails on
//! `SIGUSR1`, [`reload_rms_from_file`] returns an [`RmsFileError`] while preserving the previously
//! active [`tickwire_risk::RmsEngine`] symbol limits unchanged.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use tickwire_risk::{RmsConfigError, RmsConfigTable, RmsEngine, RmsReloadHandle};

/// Error returned when loading or reloading a static RMS configuration file.
#[derive(Debug)]
pub enum RmsFileError {
    /// Reading the static RMS configuration file failed.
    Io { path: PathBuf, source: io::Error },
    /// Parsing or validating the RMS symbol limit table failed.
    Config {
        path: PathBuf,
        error: RmsConfigError,
    },
}

impl fmt::Display for RmsFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(f, "failed to read RMS config {}: {source}", path.display())
            }
            Self::Config { path, error } => {
                write!(f, "invalid RMS config {}: {error}", path.display())
            }
        }
    }
}

impl std::error::Error for RmsFileError {}

/// Reads and validates an [`RmsConfigTable`] from `path` during cold startup.
pub fn load_rms_config_file(path: &Path) -> Result<RmsConfigTable, RmsFileError> {
    let raw = fs::read_to_string(path).map_err(|source| RmsFileError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    RmsConfigTable::parse_str(&raw).map_err(|error| RmsFileError::Config {
        path: path.to_path_buf(),
        error,
    })
}

/// Constructs a flat [`RmsEngine`] with `symbol_count` slots and applies the initial static RMS
/// table at `path`.
pub fn build_rms_engine_from_file(
    symbol_count: usize,
    path: &Path,
) -> Result<(RmsEngine, RmsReloadHandle), RmsFileError> {
    let table = load_rms_config_file(path)?;
    let engine = RmsEngine::new(symbol_count);
    let handle = engine.reload_handle();
    handle
        .apply_table(&table)
        .map_err(|error| RmsFileError::Config {
            path: path.to_path_buf(),
            error,
        })?;
    Ok((engine, handle))
}

/// Reloads `path` off-thread (`SIGUSR1` handler) into `reload_handle`.
///
/// On any file-read, syntax, or validation error, returns `Err(RmsFileError)` without mutating
/// any active symbol limit or advancing `reload_handle.generation()`.
pub fn reload_rms_from_file(
    path: &Path,
    reload_handle: &RmsReloadHandle,
) -> Result<u64, RmsFileError> {
    let raw = fs::read_to_string(path).map_err(|source| RmsFileError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    reload_handle
        .reload_limits(&raw)
        .map_err(|error| RmsFileError::Config {
            path: path.to_path_buf(),
            error,
        })
}
