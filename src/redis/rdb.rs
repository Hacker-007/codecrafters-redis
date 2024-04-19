pub struct RDBConfig {
    pub dir: String,
    pub file_name: String,
}

impl RDBConfig {
    pub fn new(dir: String, file_name: String) -> Self {
        Self { dir, file_name }
    }
}

pub struct RDBPesistence {
    pub config: RDBConfig,
}

impl RDBPesistence {
    pub fn new(config: RDBConfig) -> Self {
        Self { config }
    }

    pub async fn setup(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
