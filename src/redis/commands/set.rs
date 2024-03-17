use std::{io::Write, net::TcpStream, time::SystemTime};

use crate::redis::{value::RedisValue, Redis, StoreValue};

pub fn process(
    key: String,
    value: String,
    px: Option<SystemTime>,
    redis: &Redis,
    stream: &mut TcpStream,
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
