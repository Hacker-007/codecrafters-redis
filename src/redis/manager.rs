use std::net::SocketAddr;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::redis::resp::command::{RedisCommand, RedisServerCommand};

use super::{
    replication::{RedisReplicationMode, RedisReplicator},
    resp::encoding,
    server::{ClientConnectionInfo, RedisReadStream, RedisServer, RedisWriteStream},
    store::RedisStore,
};

pub struct RedisCommandPacket {
    client_info: ClientConnectionInfo,
    command: RedisCommand,
    write_stream: RedisWriteStream,
}

impl RedisCommandPacket {
    pub fn new(
        client_info: ClientConnectionInfo,
        command: RedisCommand,
        write_stream: RedisWriteStream,
    ) -> Self {
        Self {
            client_info,
            command,
            write_stream,
        }
    }
}

pub struct RedisManager {
    store: RedisStore,
    replicator: RedisReplicator,
    address: SocketAddr,
}

impl RedisManager {
    pub fn manage(
        store: RedisStore,
        replication_mode: RedisReplicationMode,
        address: SocketAddr,
    ) -> Self {
        Self {
            store,
            replicator: RedisReplicator::new(address, replication_mode),
            address,
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let (command_tx, mut command_rx) = mpsc::channel(32);
        let server = RedisServer::start(self.address).await?;
        eprintln!("[redis] server started at {}", self.address);

        self.replicator.setup(command_tx.clone()).await?;
        self.setup_client_connection_handling(server, command_tx);
        while let Some(RedisCommandPacket {
            client_info,
            command,
            write_stream,
        }) = command_rx.recv().await
        {
            match &command {
                RedisCommand::Store(command) => {
                    let mut output = BytesMut::with_capacity(2048).writer();
                    self.store.handle(command, &mut output)?;
                    write_stream.write(output.into_inner().freeze()).await?;
                    if command.is_write() {
                        self.replicator.try_replicate(command.into()).await?;
                    }
                }
                RedisCommand::Server(RedisServerCommand::Ping) => self.ping(write_stream).await?,
                RedisCommand::Server(RedisServerCommand::Echo { message }) => {
                    self.echo(message.clone(), write_stream).await?
                }
                RedisCommand::Replication(command) => {
                    self.replicator
                        .handle_command(client_info, command, write_stream)
                        .await?
                }
            }

            self.replicator.post_command_hook(&command);
        }

        Ok(())
    }

    async fn ping(&mut self, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        write_stream.write(encoding::bulk_string("PONG")).await
    }

    async fn echo(&mut self, message: Bytes, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        write_stream.write(encoding::bulk_string(message)).await
    }
}

impl RedisManager {
    fn setup_client_connection_handling(
        &mut self,
        mut server: RedisServer,
        command_tx: mpsc::Sender<RedisCommandPacket>,
    ) {
        tokio::spawn(async move {
            loop {
                let (read_stream, write_stream, client_info) = server.accept().await?;
                let address = client_info.address;
                eprintln!("[redis] client at {} connected", address);
                let command_tx = command_tx.clone();
                tokio::spawn(async move {
                    if let Err(err) =
                        Self::process_stream(client_info, read_stream, write_stream, command_tx)
                            .await
                    {
                        eprintln!("{err}");
                        eprintln!(
                            "[redis - error] an unknown error occurred when processing client stream"
                        )
                    }

                    eprintln!("[redis] client at {} disconnected", address);
                });
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        });
    }

    async fn process_stream(
        client_info: ClientConnectionInfo,
        mut read_stream: RedisReadStream,
        write_stream: RedisWriteStream,
        command_tx: mpsc::Sender<RedisCommandPacket>,
    ) -> anyhow::Result<()> {
        loop {
            match read_stream.read().await {
                Ok(Some(command)) => {
                    command_tx
                        .send(RedisCommandPacket {
                            client_info: client_info.clone(),
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
}
