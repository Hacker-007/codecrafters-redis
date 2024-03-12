use std::{
    io::{BufRead, BufReader, Read},
    net::TcpStream,
};

use anyhow::Context;

pub struct RESPReader {
    reader: BufReader<TcpStream>,
}

impl RESPReader {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            reader: BufReader::new(stream),
        }
    }

    pub fn read_line(&mut self) -> anyhow::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.reader.read_until(b'\n', &mut bytes)?;
        bytes.pop();
        bytes.pop();
        Ok(bytes)
    }

    pub fn read_string(&mut self) -> anyhow::Result<String> {
        let bytes = self.read_line()?;
        String::from_utf8(bytes).context("[redis - error] value not a valid UTF-8 string")
    }

    pub fn read_usize(&mut self) -> anyhow::Result<usize> {
        let s = self.read_string()?;
        s.parse()
            .context("[redis - error] value is not a valid unsigned number")
    }

    pub fn read_i32(&mut self) -> anyhow::Result<i32> {
        let s = self.read_string()?;
        s.parse()
            .context("[redis - error] value is not a valid signed number")
    }
}

impl Read for RESPReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }
}