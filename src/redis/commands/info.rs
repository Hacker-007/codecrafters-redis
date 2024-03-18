use std::{io::Write, net::TcpStream};

use crate::redis::{value::RedisValue, Redis, RedisMode};

use super::InfoSection;

pub fn process(
    section: InfoSection,
    redis: &Redis,
    stream: &mut TcpStream,
) -> anyhow::Result<()> {
    if section == InfoSection::Replication {
        let info = match &redis.mode {
            RedisMode::Master {
                replication_id,
                replication_offset,
                ..
            } => format!(
                "role:master\nmaster_replid:{replication_id}\nmaster_repl_offset:{replication_offset}",
            ),
            RedisMode::Slave { .. } => "role:slave".to_string(),
        };

        write!(stream, "{}", RedisValue::BulkString(info))?;
        return Ok(())
    }

    Err(anyhow::anyhow!(
        "[redis - error] command 'info' currently only supports section 'replication'"
    ))
}
