use crate::redis::{value::RedisValue, Redis};

pub fn process(key: String, value: String, redis: &mut Redis) -> anyhow::Result<RedisValue> {
    redis.store.insert(key, value);
    Ok(RedisValue::SimpleString("OK".to_string()))
}
