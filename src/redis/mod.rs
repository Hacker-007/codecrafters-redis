use std::collections::VecDeque;
use std::net::TcpStream;

use self::commands::{echo, ping};
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
                let (command_name, arguments) = self.parse_command(values)?;
                match command_name.to_lowercase().as_str() {
                    "ping" => ping::process(arguments, &mut self.writer)?,
                    "echo" => echo::process(arguments, &mut self.writer)?,
                    command => println!("[redis-error] unknown command '{command}' received"),
                }
            } else {
                println!("[redis-error] expected a command encoded as an array of binary strings")
            }
        }
    }

    fn parse_command(
        &self,
        values: VecDeque<RedisValue>,
    ) -> anyhow::Result<(String, VecDeque<String>)> {
        anyhow::ensure!(
            values.len() >= 1,
            "[redis-error] client input must be a non-empty array"
        );

        let mut values = values
            .into_iter()
            .map(|value| {
                if let RedisValue::BulkString(s) = value {
                    Ok(s)
                } else {
                    Err(anyhow::anyhow!(
                        "[redis-error] client input must be an array of bulk strings"
                    ))
                }
            })
            .collect::<Result<VecDeque<_>, _>>()?;

        let command_name = values.pop_front().unwrap();
        let arguments = values;
        Ok((command_name, arguments))
    }
}
