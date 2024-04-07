use std::net::SocketAddr;

use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream, ToSocketAddrs,
    },
    sync::mpsc,
};

use super::{command::RedisCommand, resp::RESPValue, resp_stream::RESPStream};

pub struct RedisServer {
    listener: TcpListener,
}

impl RedisServer {
    pub async fn bind(address: impl ToSocketAddrs) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        Ok(Self { listener })
    }

    pub async fn accept(
        &mut self,
    ) -> anyhow::Result<(RedisReadStream, RedisWriteStream, SocketAddr)> {
        let (stream, address) = self.listener.accept().await?;
        let (read_stream, write_stream) = self.convert_stream(stream).await;
        Ok((read_stream, write_stream, address))
    }

    async fn convert_stream(&self, stream: TcpStream) -> (RedisReadStream, RedisWriteStream) {
        let (read_stream, write_stream) = stream.into_split();
        let read_stream = RESPStream::new(read_stream);
        let read_rx = self.setup_read_hook(read_stream);
        let write_tx = self.setup_write_hook(write_stream);

        (RedisReadStream { read_rx }, RedisWriteStream { write_tx })
    }

    fn setup_read_hook(
        &self,
        mut read_stream: RESPStream<OwnedReadHalf>,
    ) -> mpsc::Receiver<anyhow::Result<RedisCommand>> {
        let (read_tx, read_rx) = mpsc::channel(32);
        tokio::task::spawn(async move {
            loop {
                let command = read_stream
                    .read_value()
                    .await
                    .and_then(|value| value.try_into());

                if read_stream.is_closed() || read_tx.send(command).await.is_err() {
                    break;
                }
            }
        });

        read_rx
    }

    fn setup_write_hook(&self, mut write_stream: OwnedWriteHalf) -> mpsc::Sender<Vec<u8>> {
        let (write_tx, mut write_rx) = mpsc::channel::<Vec<_>>(32);
        tokio::task::spawn(async move {
            while let Some(bytes) = write_rx.recv().await {
                if write_stream.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });

        write_tx
    }
}

pub struct RedisReadStream {
    read_rx: mpsc::Receiver<anyhow::Result<RedisCommand>>,
}

impl RedisReadStream {
    pub async fn read(&mut self) -> anyhow::Result<Option<RedisCommand>> {
        match self.read_rx.recv().await {
            Some(Ok(command)) => Ok(Some(command)),
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }
}

#[derive(Clone)]
pub struct RedisWriteStream {
    write_tx: mpsc::Sender<Vec<u8>>,
}

impl RedisWriteStream {
    pub async fn write(&mut self, bytes: impl Into<Vec<u8>>) -> anyhow::Result<()> {
        self.write_tx.send(bytes.into()).await?;
        Ok(())
    }
}
