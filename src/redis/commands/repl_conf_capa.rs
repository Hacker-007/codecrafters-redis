use crate::redis::value::RedisValue;

pub fn process() -> anyhow::Result<RedisValue> {
    Ok(RedisValue::SimpleString("OK".to_string()))
}
