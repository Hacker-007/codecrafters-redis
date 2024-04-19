pub mod handler;
pub mod command;

pub struct RDBConfig {
    dir: String,
    file_name: String,
}

impl RDBConfig {
    pub fn new(dir: String, file_name: String) -> Self {
        Self { dir, file_name }
    }
}

pub struct RDBPesistence {
    config: RDBConfig,
}

impl RDBPesistence {
    pub fn new(config: RDBConfig) -> Self {
        Self {
            config,
        }
    }
}