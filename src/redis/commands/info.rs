use crate::redis::{value::RedisValue, Redis, RedisMode};

use super::InfoSection;

pub fn process(section: InfoSection, redis: &Redis) -> anyhow::Result<RedisValue> {
    if section == InfoSection::Replication {
        let info = match &redis.mode {
            RedisMode::Master {
                replication_id,
                replication_offset,
            } => format!(
                "role:master\nmaster_replid:{}\nmaster_repl_offset:{}",
                replication_id, replication_offset
            ),
            RedisMode::Slave { .. } => "role:slave".to_string(),
        };

        return Ok(RedisValue::BulkString(info));
    }

    Err(anyhow::anyhow!(
        "[redis - error] command 'info' currently only supports section 'replication'"
    ))
}
