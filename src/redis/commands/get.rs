use std::{net::TcpStream, time::SystemTime, io::Write};

use crate::redis::{value::RedisValue, Redis, StoreValue};

pub fn process(key: String, redis: &Redis, stream: &mut TcpStream) -> anyhow::Result<()> {
    let mut store = redis.lock_store();
    match store.get(&key) {
        Some(StoreValue {
            expiration: Some(expiration),
            ..
        }) if *expiration <= SystemTime::now() => {
            store.remove(&key);
            write!(stream, "{}", RedisValue::NullBulkString)?;
            Ok(())
        }
        Some(StoreValue { value, .. }) => {
            write!(stream, "{}", RedisValue::BulkString(value.clone()))?;
            Ok(())
        }
        _ => {
            write!(stream, "{}", RedisValue::NullBulkString)?;
            Ok(())
        }
    }
}
