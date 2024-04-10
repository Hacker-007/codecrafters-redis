use bytes::Bytes;

use crate::redis::resp::RESPValue;

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
        if let Self::ReplConf {
            section: ReplConfSection::GetAck,
        } = self
        {
            true
        } else {
            false
        }
    }
}

impl From<&RedisReplicationCommand> for RESPValue {
    fn from(command: &RedisReplicationCommand) -> Self {
        match command {
            RedisReplicationCommand::Info {
                section: InfoSection::Default,
            } => RESPValue::Array(vec![RESPValue::BulkString(Bytes::from_static(b"INFO"))]),
            RedisReplicationCommand::Info {
                section: InfoSection::Replication,
            } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"INFO")),
                RESPValue::BulkString(Bytes::from_static(b"replication")),
            ]),
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::Port { listening_port },
            } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"REPLCONF")),
                RESPValue::BulkString(Bytes::from_static(b"listening-port")),
                RESPValue::BulkString(Bytes::copy_from_slice(
                    listening_port.to_string().as_bytes(),
                )),
            ]),
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::Capa { capabilities },
            } => {
                let mut values = vec![
                    RESPValue::BulkString(Bytes::from_static(b"REPLCONF")),
                    RESPValue::BulkString(Bytes::from_static(b"capa")),
                ];

                for capability in capabilities {
                    values.push(RESPValue::BulkString(capability.clone()));
                }

                RESPValue::Array(values)
            }
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::GetAck,
            } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"REPLCONF")),
                RESPValue::BulkString(Bytes::from_static(b"GETACK")),
                RESPValue::BulkString(Bytes::from_static(b"*")),
            ]),
            RedisReplicationCommand::PSync {
                replication_id,
                replication_offset,
            } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"PSYNC")),
                RESPValue::BulkString(Bytes::copy_from_slice(replication_id.as_bytes())),
                RESPValue::BulkString(Bytes::copy_from_slice(
                    replication_offset.to_string().as_bytes(),
                )),
            ]),
            RedisReplicationCommand::Wait {
                num_replicas,
                timeout,
            } => RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"WAIT")),
                RESPValue::BulkString(Bytes::copy_from_slice(num_replicas.to_string().as_bytes())),
                RESPValue::BulkString(Bytes::copy_from_slice(timeout.to_string().as_bytes())),
            ]),
        }
    }
}
