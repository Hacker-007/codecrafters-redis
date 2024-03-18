use std::io::Write;

use crate::redis::value::RedisValue;

pub fn process(echo: String, stream: &mut impl Write) -> anyhow::Result<()> {
    write!(stream, "{}", RedisValue::BulkString(echo))?;
    Ok(())
}
