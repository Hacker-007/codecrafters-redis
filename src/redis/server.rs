use std::{fmt::Display, net::SocketAddr, ops::AddAssign};

use bytes::Bytes;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, ToSocketAddrs},
    sync::mpsc,
};

use super::resp::{command::RedisCommand, resp_reader::RESPReader};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(usize);

impl ClientId {
    pub fn primary() -> Self {
        Self(usize::MAX)
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AddAssign<usize> for ClientId {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs;
    }
}

#[derive(Debug)]
pub struct RedisServer {
    id: ClientId,
    listener: TcpListener,
}

pub struct RedisReadStream(mpsc::Receiver<anyhow::Result<RedisCommand>>);

impl RedisReadStream {
    pub async fn read(&mut self) -> anyhow::Result<Option<RedisCommand>> {
        match self.0.recv().await {
            Some(Ok(command)) => Ok(Some(command)),
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }
}

#[derive(Clone)]
pub struct RedisWriteStream {
    should_send: bool,
    tx: mpsc::Sender<Bytes>,
}

impl RedisWriteStream {
    pub fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self {
            should_send: true,
            tx,
        }
    }
}

impl RedisWriteStream {
    pub async fn write(&self, bytes: impl Into<Bytes>) -> anyhow::Result<()> {
        if self.should_send {
            self.tx.send(bytes.into()).await?;
        }

        Ok(())
    }

    pub fn close(&mut self) {
        self.should_send = false;
    }
}

pub struct ClientConnectionInfo {
    pub id: ClientId,
    pub address: SocketAddr,
}

impl RedisServer {
    pub async fn start(addresses: impl ToSocketAddrs) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(addresses).await?;
        Ok(Self {
            id: ClientId(0),
            listener,
        })
    }

    pub async fn accept(
        &mut self,
    ) -> anyhow::Result<(RedisReadStream, RedisWriteStream, ClientConnectionInfo)> {
        let (stream, address) = self.listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut read_half = RESPReader::new(read_half);
        let (read_tx, read_rx) = mpsc::channel(32);
        let (write_tx, mut write_rx) = mpsc::channel::<Bytes>(32);
        tokio::spawn(async move {
            loop {
                let command = read_half
                    .read_value()
                    .await
                    .and_then(|value| value.try_into());

                if read_half.is_closed() || read_tx.send(command).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            while let Some(bytes) = write_rx.recv().await {
                if write_half.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });

        let id = self.id;
        self.id += 1;
        Ok((
            RedisReadStream(read_rx),
            RedisWriteStream::new(write_tx),
            ClientConnectionInfo { id, address },
        ))
    }
}
