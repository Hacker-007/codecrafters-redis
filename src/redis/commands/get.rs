use std::time::SystemTime;

use crate::redis::{value::RedisValue, Redis, StoreValue};

pub fn process(key: String, redis: &mut Redis) -> anyhow::Result<RedisValue> {
    match redis.store.get(&key) {
        Some(StoreValue {
            expiration: Some(expiration),
            ..
        }) if *expiration <= SystemTime::now() => {
            redis.store.remove(&key);
            Ok(RedisValue::NullBulkString)
        }
        Some(StoreValue { value, .. }) => Ok(RedisValue::BulkString(value.clone())),
        _ => Ok(RedisValue::NullBulkString),
    }
}
