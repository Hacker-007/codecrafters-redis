use std::time::SystemTime;

use crate::redis::{value::RedisValue, Redis, StoreValue};

pub fn process(
    key: String,
    value: String,
    px: Option<SystemTime>,
    redis: &Redis,
) -> anyhow::Result<RedisValue> {
    redis.lock_store().insert(
        key,
        StoreValue {
            value,
            expiration: px,
        },
    );

    Ok(RedisValue::SimpleString("OK".to_string()))
}
