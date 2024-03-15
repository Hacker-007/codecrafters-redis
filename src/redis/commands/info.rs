use crate::redis::{value::RedisValue, Redis, RedisMode};

use super::InfoSection;

pub fn process(section: InfoSection, redis: &Redis) -> anyhow::Result<RedisValue> {
    if section == InfoSection::Replication {
        let role = match redis.mode {
            RedisMode::Master => "role:master".to_string(),
            RedisMode::Slave { .. } => "role:slave".to_string(),
        };

        return Ok(RedisValue::BulkString(role));
    }

    Err(anyhow::anyhow!(
        "[redis - error] command 'info' currently only supports section 'replication'"
    ))
}
