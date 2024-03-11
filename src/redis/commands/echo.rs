use std::{collections::VecDeque, io::Write};

use crate::redis::value::RedisValue;

pub fn process(mut arguments: VecDeque<String>, writer: &mut impl Write) -> anyhow::Result<()> {
    anyhow::ensure!(
        arguments.len() == 1,
        "[redis-error] command 'echo' requires one argument"
    );

    let message = arguments.pop_front().unwrap();
    write!(writer, "{}", RedisValue::BulkString(message))?;
    Ok(())
}
