use std::io::Write;

use crate::redis::value::RedisValue;

pub fn process(writer: &mut impl Write) -> anyhow::Result<()> {
    write!(writer, "{}", RedisValue::SimpleString("PONG".to_string()))?;
    Ok(())
}
