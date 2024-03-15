use std::time::SystemTime;

use crate::redis::{value::RedisValue, Redis, StoreValue};

pub fn process(key: String, redis: &Redis) -> anyhow::Result<RedisValue> {
    let mut store = redis.lock_store();
    match store.get(&key) {
        Some(StoreValue {
            expiration: Some(expiration),
            ..
        }) if *expiration <= SystemTime::now() => {
            store.remove(&key);
            Ok(RedisValue::NullBulkString)
        }
        Some(StoreValue { value, .. }) => Ok(RedisValue::BulkString(value.clone())),
        _ => Ok(RedisValue::NullBulkString),
    }
}
