mod resp_stream;
mod resp;
mod command;

use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream, ToSocketAddrs,
};

use self::resp_stream::RESPStream;

pub struct RedisStream {
    read_stream: RESPStream<OwnedReadHalf>,
    write_stream: OwnedWriteHalf,
}

impl RedisStream {
    pub async fn connect(address: impl ToSocketAddrs) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let (read_stream, write_stream) = stream.into_split();
        Ok(Self {
            read_stream: RESPStream::new(read_stream),
            write_stream,
        })
    }
}
