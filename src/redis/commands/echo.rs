use crate::redis::value::RedisValue;

pub fn process(echo: String) -> anyhow::Result<RedisValue> {
    Ok(RedisValue::BulkString(echo))
}
