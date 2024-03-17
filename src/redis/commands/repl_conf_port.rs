use std::{io::Write, net::TcpStream};

use crate::redis::value::RedisValue;

pub fn process(stream: &mut TcpStream) -> anyhow::Result<()> {
    write!(stream, "{}", RedisValue::SimpleString("OK".to_string()))?;
    Ok(())
}
