use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::time::SystemTime;

use self::commands::{echo, get, ping, set, RedisCommand};
use self::{resp_reader::RESPReader, value::RedisValue};

mod commands;
pub mod resp_reader;
pub mod value;

type StoreKey = String;
struct StoreValue {
    pub(self) value: String,
    pub(self) expiration: Option<SystemTime>,
}

pub struct Redis {
    reader: RESPReader,
    writer: TcpStream,
    store: HashMap<StoreKey, StoreValue>,
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
                    RedisCommand::Set { key, value, px } => set::process(key, value, px, self)?,
                };

                write!(self.writer, "{}", result)?;
            } else {
                println!("[redis - error] expected a command encoded as an array of binary strings")
            }
        }
    }
}
