use std::{io::Write, time::SystemTime};

use crate::redis::{value::RedisValue, Redis, StoreValue};

pub fn process(
    key: String,
    value: String,
    px: Option<SystemTime>,
    redis: &Redis,
    stream: &mut impl Write,
) -> anyhow::Result<()> {
    redis.lock_store().insert(
        key,
        StoreValue {
            value,
            expiration: px,
        },
    );

    write!(stream, "{}", RedisValue::SimpleString("OK".to_string()))?;
    Ok(())
}
