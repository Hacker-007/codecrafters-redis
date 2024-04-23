use std::time::SystemTime;

use bytes::Bytes;

use crate::redis::{
    replication::command::{InfoSection, RedisReplicationCommand, ReplConfSection},
    resp::command::{ConfigSection, RedisCommand, RedisServerCommand, RedisStoreCommand},
};

use super::{array, bulk_string};

pub fn get(key: impl AsRef<[u8]>) -> Bytes {
    array(vec![bulk_string("GET"), bulk_string(key)]).into()
}

pub fn set(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>, px: Option<&SystemTime>) -> Bytes {
    let mut values = vec![bulk_string("SET"), bulk_string(key), bulk_string(value)];
    if let Some(px) = px {
        let duration = match px.elapsed() {
            Ok(duration) => duration,
            Err(err) => err.duration(),
        };

        values.push(bulk_string("PX"));
        values.push(bulk_string(format!("{}", duration.as_millis())));
    }

    array(values).into()
}

pub fn keys(key: &Bytes) -> Bytes {
    array(vec![bulk_string("KEYS"), bulk_string(key)]).into()
}

pub fn ty(key: &Bytes) -> Bytes {
    array(vec![bulk_string("TYPE"), bulk_string(key)]).into()
}

pub fn xadd(
    key: impl AsRef<[u8]>,
    entry_id: impl AsRef<[u8]>,
    fields: &[(impl AsRef<[u8]>, impl AsRef<[u8]>)],
) -> Bytes {
    let mut values = vec![bulk_string("XADD"), bulk_string(key), bulk_string(entry_id)];
    for (field, value) in fields {
        values.push(bulk_string(field));
        values.push(bulk_string(value));
    }

    array(values).into()
}

pub fn ping() -> Bytes {
    array(vec![bulk_string("PING")]).into()
}

pub fn echo(message: impl AsRef<[u8]>) -> Bytes {
    array(vec![bulk_string("ECHO"), bulk_string(message)]).into()
}

pub fn config(section: &ConfigSection) -> Bytes {
    let mut values = vec![bulk_string("CONFIG")];
    match section {
        ConfigSection::Get { keys } => {
            values.push(bulk_string("GET"));
            for key in keys {
                values.push(bulk_string(key));
            }
        }
    }

    array(values).into()
}

pub fn info(section: InfoSection) -> Bytes {
    let mut values = vec![bulk_string("INFO")];
    match section {
        InfoSection::Default => {}
        InfoSection::Replication => values.push(bulk_string("replication")),
    }

    array(values).into()
}

pub fn replconf_port(listening_port: u16) -> Bytes {
    array(vec![
        bulk_string("REPLCONF"),
        bulk_string("listening-port"),
        bulk_string(format!("{}", listening_port)),
    ])
    .into()
}

pub fn replconf_capa(capabilities: &[Bytes]) -> Bytes {
    let mut values = vec![bulk_string("REPLCONF"), bulk_string("capa")];

    for capability in capabilities {
        values.push(bulk_string(capability));
    }

    array(values).into()
}

pub fn replconf_get_ack() -> Bytes {
    array(vec![
        bulk_string("REPLCONF"),
        bulk_string("GETACK"),
        bulk_string("*"),
    ])
    .into()
}

pub fn replconf_ack(processed_bytes: usize) -> Bytes {
    array(vec![
        bulk_string("REPLCONF"),
        bulk_string("ACK"),
        bulk_string(format!("{}", processed_bytes)),
    ])
    .into()
}

pub fn psync(replication_id: &str, replication_offset: i64) -> Bytes {
    array(vec![
        bulk_string("PSYNC"),
        bulk_string(replication_id),
        bulk_string(format!("{}", replication_offset)),
    ])
    .into()
}

pub fn wait(num_replicas: usize, timeout: usize) -> Bytes {
    array(vec![
        bulk_string("WAIT"),
        bulk_string(format!("{}", num_replicas)),
        bulk_string(format!("{}", timeout)),
    ])
    .into()
}

impl From<&RedisCommand> for Bytes {
    fn from(command: &RedisCommand) -> Self {
        match command {
            RedisCommand::Store(command) => command.into(),
            RedisCommand::Server(command) => command.into(),
            RedisCommand::Replication(command) => command.into(),
        }
    }
}

impl From<&RedisStoreCommand> for Bytes {
    fn from(command: &RedisStoreCommand) -> Self {
        match command {
            RedisStoreCommand::Get { key } => get(key),
            RedisStoreCommand::Set { key, value, px } => set(key, value, px.as_ref()),
            RedisStoreCommand::Keys { key } => keys(key),
            RedisStoreCommand::Type { key } => ty(key),
            RedisStoreCommand::XAdd {
                key,
                entry_id,
                fields,
            } => xadd(key, entry_id, fields),
        }
    }
}

impl From<&RedisServerCommand> for Bytes {
    fn from(command: &RedisServerCommand) -> Self {
        match command {
            RedisServerCommand::Ping => ping(),
            RedisServerCommand::Echo { message } => echo(message),
            RedisServerCommand::Config { section } => config(section),
        }
    }
}

impl From<&RedisReplicationCommand> for Bytes {
    fn from(command: &RedisReplicationCommand) -> Self {
        match command {
            RedisReplicationCommand::Info { section } => info(*section),
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::Port { listening_port },
            } => replconf_port(*listening_port),
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::Capa { capabilities },
            } => replconf_capa(capabilities),
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::GetAck,
            } => replconf_get_ack(),
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::Ack { processed_bytes },
            } => replconf_ack(*processed_bytes),
            RedisReplicationCommand::PSync {
                replication_id,
                replication_offset,
            } => psync(replication_id, *replication_offset),
            RedisReplicationCommand::Wait {
                num_replicas,
                timeout,
            } => wait(*num_replicas, *timeout),
        }
    }
}
