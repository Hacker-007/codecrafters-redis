use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;

use self::commands::{echo, ping, get, set, RedisCommand};
use self::{resp_reader::RESPReader, value::RedisValue};

mod commands;
pub mod resp_reader;
pub mod value;

pub struct Redis {
    reader: RESPReader,
    writer: TcpStream,
    store: HashMap<String, String>,
}

impl Redis {
    pub fn new(client: TcpStream) -> anyhow::Result<Self> {
        let reader_client = client.try_clone()?;

        Ok(Self {
            reader: RESPReader::new(reader_client),
            writer: client,
            store: HashMap::new(),
        })
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        loop {
            let value = RedisValue::parse(&mut self.reader)?;
            if let RedisValue::Array(values) = value {
                let command: RedisCommand = values.try_into()?;
                let result = match command {
                    RedisCommand::Ping => ping::process()?,
                    RedisCommand::Echo { echo } => echo::process(echo)?,
                    RedisCommand::Get { key } => get::process(key, self)?,
                    RedisCommand::Set { key, value } => set::process(key, value, self)?,
                };

                write!(self.writer, "{}", result)?;
            } else {
                println!("[redis-error] expected a command encoded as an array of binary strings")
            }
        }
    }
}
