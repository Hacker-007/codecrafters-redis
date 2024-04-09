use std::net::SocketAddr;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::redis::resp::{
    command::{InfoSection, RedisCommand, RedisServerCommand},
    RESPValue,
};

use super::{
    server::{RedisReadStream, RedisServer, RedisWriteStream},
    store::RedisStore,
};

pub struct RedisCommandPacket {
    command: RedisCommand,
    write_stream: RedisWriteStream,
}

async fn process_stream(
    mut read_stream: RedisReadStream,
    write_stream: RedisWriteStream,
    command_tx: mpsc::Sender<RedisCommandPacket>,
) -> anyhow::Result<()> {
    loop {
        match read_stream.read().await {
            Ok(Some(command)) => {
                command_tx
                    .send(RedisCommandPacket {
                        command,
                        write_stream: write_stream.clone(),
                    })
                    .await?;
            }
            Ok(None) => return Ok(()),
            Err(err) => return Err(err),
        }
    }
}

#[derive(Debug)]
pub struct RedisManager {
    store: RedisStore,
    address: SocketAddr,
}

impl RedisManager {
    pub fn manage(store: RedisStore, address: SocketAddr) -> Self {
        Self { store, address }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let server = RedisServer::start(self.address).await?;
        eprintln!("[redis] server started at {}", self.address);

        let (command_tx, mut command_rx) = mpsc::channel(32);
        self.setup_client_connection_handling(server, command_tx);
        while let Some(RedisCommandPacket {
            command,
            mut write_stream,
        }) = command_rx.recv().await
        {
            match command {
                RedisCommand::Store(command) => {
                    let mut output = BytesMut::with_capacity(2048).writer();
                    self.store.handle(command, &mut output)?;
                    write_stream.write(output.into_inner().freeze()).await?;
                }
                RedisCommand::Server(RedisServerCommand::Ping) => self.ping(write_stream).await?,
                RedisCommand::Server(RedisServerCommand::Echo { echo }) => {
                    self.echo(echo, write_stream).await?
                }
                RedisCommand::Server(RedisServerCommand::Info { section }) => {
                    self.info(section, write_stream).await?
                }
                _ => todo!(),
            }
        }

        Ok(())
    }

    async fn ping(&mut self, mut write_stream: RedisWriteStream) -> anyhow::Result<()> {
        let response = RESPValue::BulkString(Bytes::from_static(b"PONG"));
        write_stream.write(Bytes::from(response)).await
    }

    async fn echo(
        &mut self,
        echo: Bytes,
        mut write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        let response = RESPValue::BulkString(echo);
        write_stream.write(Bytes::from(response)).await
    }

    async fn info(
        &mut self,
        section: InfoSection,
        mut write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        match section {
            InfoSection::Default | InfoSection::Replication => {
                let response = format!(
                    "role:master\nmaster_replid:{}\nmaster_repl_offset:{}",
                    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0
                );

                let response = RESPValue::BulkString(Bytes::copy_from_slice(response.as_bytes()));
                write_stream.write(Bytes::from(response)).await
            }
        }
    }

    fn setup_client_connection_handling(
        &mut self,
        mut server: RedisServer,
        command_tx: mpsc::Sender<RedisCommandPacket>,
    ) {
        tokio::spawn(async move {
            loop {
                let (read_stream, write_stream, address) = server.accept().await?;
                eprintln!("[redis] client at {address} connected");
                let command_tx = command_tx.clone();
                tokio::spawn(async move {
                    if process_stream(read_stream, write_stream, command_tx)
                        .await
                        .is_err()
                    {
                        eprintln!(
                            "[redis - error] an unknown error occurred when processing client stream"
                        )
                    }

                    eprintln!("[redis] client at {address} disconnected");
                });
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        });
    }
}
