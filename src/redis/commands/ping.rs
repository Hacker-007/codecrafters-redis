use std::{collections::VecDeque, io::Write};

use crate::redis::value::RedisValue;

pub fn process(_: VecDeque<String>, writer: &mut impl Write) -> anyhow::Result<()> {
    write!(
        writer,
        "{}",
        RedisValue::SimpleString("PONG".to_string())
    )?;

    Ok(())
}