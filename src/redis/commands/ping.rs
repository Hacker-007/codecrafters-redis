use std::io::Write;

use crate::redis::value::RedisValue;

pub fn process(stream: &mut impl Write) -> anyhow::Result<()> {
    write!(stream, "{}", RedisValue::SimpleString("PONG".to_string()))?;
    Ok(())
}
