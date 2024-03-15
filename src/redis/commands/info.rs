use std::{io::Write, net::TcpStream};

use crate::redis::{value::RedisValue, Redis, RedisMode};

use super::InfoSection;

pub fn process(section: InfoSection, redis: &Redis, stream: &mut TcpStream) -> anyhow::Result<()> {
    if section == InfoSection::Replication {
        match &redis.mode {
            RedisMode::Master {
                replication_id,
                replication_offset,
            } => {
                write!(
                    stream,
                    "{}{}{}",
                    RedisValue::BulkString("role:master".to_string()),
                    RedisValue::BulkString(format!("master_replid:{}", replication_id)),
                    RedisValue::BulkString(format!("master_offset:{}", replication_offset))
                )?;
            }
            RedisMode::Slave { .. } => write!(
                stream,
                "{}",
                RedisValue::BulkString("role:slave".to_string())
            )?,
        }

        return Ok(());
    }

    Err(anyhow::anyhow!(
        "[redis - error] command 'info' currently only supports section 'replication'"
    ))
}
