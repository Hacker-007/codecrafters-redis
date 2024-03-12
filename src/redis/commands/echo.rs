use std::io::Write;

use crate::redis::value::RedisValue;

pub fn process(echo: String, writer: &mut impl Write) -> anyhow::Result<()> {
    write!(writer, "{}", RedisValue::BulkString(echo))?;
    Ok(())
}
