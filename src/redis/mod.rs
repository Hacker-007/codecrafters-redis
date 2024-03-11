use std::io::Write;
use std::net::TcpStream;

use self::{resp_reader::RESPReader, value::RedisValue};

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
            match value {
                RedisValue::SimpleString(ref value) => {
                    println!("[redis - error] unexpected string '{value}'")
                }
                RedisValue::BulkString(ref data) => {
                    println!("[redis - error] unexpected string '{data:#?}'")
                }
                RedisValue::Array(_) => write!(
                    self.writer,
                    "{}",
                    RedisValue::SimpleString("PONG".to_string())
                )?,
            }
        }
    }
}
