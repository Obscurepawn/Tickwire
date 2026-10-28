//! Same-process declared-window publication with actual producer and account facts.
use crate::handover::{NEW_INSTANCE, atomic_json, hex, json, publish, read};
use crate::{Result, Scenario};
use serde_json::{Value, json};
use std::{fs, path::Path};
use tickwire_release::Object;
use tickwire_strategy_sdk::bootstrap::Bootstrap;
mod configure;
mod driver;
mod material;
mod proof;
pub(crate) use configure::configure;
pub(crate) use driver::Driver;
pub(crate) use proof::check_export;
#[cfg(test)]
mod tests;
