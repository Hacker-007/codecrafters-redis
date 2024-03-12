use crate::redis::{value::RedisValue, Redis};

pub fn process(key: String, redis: &mut Redis) -> anyhow::Result<RedisValue> {
    if let Some(value) = redis.store.get(&key) {
        Ok(RedisValue::BulkString(value.clone()))
    } else {
        Ok(RedisValue::NullBulkString)
    }
}
