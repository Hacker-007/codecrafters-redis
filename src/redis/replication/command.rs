use bytes::Bytes;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum InfoSection {
    Replication,
    Default,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ReplConfSection {
    Port { listening_port: u16 },
    Capa { capabilities: Vec<Bytes> },
    GetAck,
    Ack { processed_bytes: usize },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisReplicationCommand {
    Info {
        section: InfoSection,
    },
    ReplConf {
        section: ReplConfSection,
    },
    PSync {
        replication_id: String,
        replication_offset: i64,
    },
    Wait {
        num_replicas: usize,
        timeout: usize,
    },
}

impl RedisReplicationCommand {
    pub fn is_getack(&self) -> bool {
        matches!(
            self,
            Self::ReplConf {
                section: ReplConfSection::GetAck
            }
        )
    }
}
