use std::{net::TcpStream, io::Write};

use crate::redis::value::RedisValue;

pub fn process(stream: &mut TcpStream) -> anyhow::Result<()> {
    write!(stream, "{}", RedisValue::SimpleString("PONG".to_string()))?;
    Ok(())
}
