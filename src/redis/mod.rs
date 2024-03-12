use std::net::TcpStream;

use self::commands::{echo, ping, RedisCommand};
use self::{resp_reader::RESPReader, value::RedisValue};

mod commands;
pub mod resp_reader;
pub mod value;

pub struct Redis {
    reader: RESPReader,
    writer: TcpStream,
}

impl Redis {
    pub fn new(client: TcpStream) -> anyhow::Result<Self> {
        let reader_client = client.try_clone()?;

        Ok(Self {
            reader: RESPReader::new(reader_client),
            writer: client,
        })
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        loop {
            let value = RedisValue::parse(&mut self.reader)?;
            if let RedisValue::Array(values) = value {
                let command: RedisCommand = values.try_into()?;
                match command {
                    RedisCommand::Ping => ping::process(&mut self.writer)?,
                    RedisCommand::Echo { echo } => echo::process(echo, &mut self.writer)?,
                }
            } else {
                println!("[redis-error] expected a command encoded as an array of binary strings")
            }
        }
    }
}
