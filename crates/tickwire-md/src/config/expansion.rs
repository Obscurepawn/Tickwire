use super::*;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpansionControl {
    pub plan_path: PathBuf,
    pub result_path: PathBuf,
    pub effective_path: PathBuf,
    pub consumer_directory: PathBuf,
    pub initial_revision: u64,
}
impl ExpansionControl {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.initial_revision == 0 {
            return Err(invalid("MD expansion revision"));
        }
        let mut paths = std::collections::BTreeSet::new();
        for path in [&self.plan_path, &self.result_path, &self.effective_path] {
            if !path.is_absolute() || !paths.insert(path) || path.file_name().is_none() {
                return Err(invalid("MD expansion control paths"));
            }
            private_directory(path.parent().ok_or_else(|| invalid("control parent"))?)?;
        }
        private_directory(&self.consumer_directory)
    }
    pub(crate) fn consumer_path(&self, pid: u32, instance: [u8; 16], kind: &str) -> PathBuf {
        let instance = instance
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.consumer_directory
            .join(format!("{pid}-{instance}.{kind}.json"))
    }
}
fn private_directory(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !path.is_absolute()
        || !metadata.is_dir()
        || metadata.mode() & 0o077 != 0
        || std::fs::canonicalize(path)? != path
    {
        return Err(invalid("MD expansion private directory"));
    }
    Ok(())
}
