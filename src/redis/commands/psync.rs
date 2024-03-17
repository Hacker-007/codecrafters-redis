use crate::redis::{value::RedisValue, Redis, RedisMode};

pub fn process(redis: &Redis) -> anyhow::Result<RedisValue> {
    if let RedisMode::Master {
        replication_id,
        replication_offset,
    } = &redis.mode
    {
        Ok(RedisValue::SimpleString(format!(
            "FULLRESYNC {} {}",
            replication_id, *replication_offset
        )))
    } else {
        Err(anyhow::anyhow!(
            "[redis - error] Redis must be running in master mode to respond to 'PSYNC' command"
        ))
    }
}
