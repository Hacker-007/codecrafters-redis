use crate::redis::value::RedisValue;

pub fn process() -> anyhow::Result<RedisValue> {
    Ok(RedisValue::SimpleString("PONG".to_string()))
}
