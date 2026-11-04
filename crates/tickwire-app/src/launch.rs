pub mod group;

use crate::Result;
use tickwire_runtime::files;

pub fn build(
    config: &crate::strategy::Config,
    settings: &files::Configuration,
    plane: tickwire_runtime_policy::ControlPlane,
) -> Result<Box<dyn crate::strategy::AppStrategy>> {
    use crate::strategy::Config;
    match config {
        Config::Hosted { config: host } => crate::hosted::build(host, settings, plane),
        Config::Multi { members } => group::build(members, settings, plane),
        other => Ok(Box::new(crate::strategy::FileStrategy::new(other)?)),
    }
}
