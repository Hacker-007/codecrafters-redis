use std::{io::Write, time::SystemTime};

use crate::redis::{value::RedisValue, Redis, StoreValue};

pub fn process(key: String, redis: &Redis, stream: &mut impl Write) -> anyhow::Result<()> {
    let mut store = redis.lock_store();
    let value = match store.get(&key) {
        Some(StoreValue {
            expiration: Some(expiration),
            ..
        }) if *expiration <= SystemTime::now() => {
            store.remove(&key);
            RedisValue::NullBulkString
        }
        Some(StoreValue { value, .. }) => RedisValue::BulkString(value.clone()),
        _ => RedisValue::NullBulkString,
    };

    write!(stream, "{value}")?;
    Ok(())
}
