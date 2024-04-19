use bytes::Bytes;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ConfigSection {
    Get {
        keys: Vec<Bytes>
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisPersistenceCommand {
    Config {
        section: ConfigSection,
    },
}