use std::{net::TcpStream, io::Write};

use crate::redis::value::RedisValue;

pub fn process(echo: String, stream: &mut TcpStream) -> anyhow::Result<()> {
    write!(stream, "{}", RedisValue::BulkString(echo))?;
    Ok(())
}
