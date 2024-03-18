use std::{io::Write, net::TcpStream};

use bytes::Bytes;

use crate::redis::{value::RedisValue, Redis, RedisMode};

const EMPTY_RDB_HEX: &'static str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub fn process(redis: &Redis, stream: &mut TcpStream) -> anyhow::Result<()> {
    if let RedisMode::Master {
        replication_id,
        replication_offset,
        ..
    } = &redis.mode
    {
        write!(
            stream,
            "{}",
            RedisValue::SimpleString(format!(
                "FULLRESYNC {} {}",
                replication_id, *replication_offset
            ))
        )?;

        let rdb_file = (0..EMPTY_RDB_HEX.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&EMPTY_RDB_HEX[i..i + 2], 16))
            .collect::<Result<Bytes, _>>()?;

        write!(stream, "${}\r\n", rdb_file.len())?;
        stream.write_all(&rdb_file)?;
        let stream = stream.try_clone()?;
        redis.add_slave_stream(stream)?;
        
        return Ok(());
    } else {
        Err(anyhow::anyhow!(
            "[redis - error] Redis must be running in master mode to respond to 'PSYNC' command"
        ))
    }
}
