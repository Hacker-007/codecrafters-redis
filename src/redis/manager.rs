use std::net::SocketAddr;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::redis::resp::command::{RedisCommand, RedisServerCommand};

use super::{
    rdb::{RDBConfig, RDBPesistence},
    replication::{RedisReplication, RedisReplicationMode},
    resp::{command::ConfigSection, encoding},
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
    address: SocketAddr,
    store: RedisStore,
    replication: RedisReplication,
    rdb_persistence: RDBPesistence,
}

impl RedisManager {
    pub fn new(
        address: SocketAddr,
        store: RedisStore,
        replication_mode: RedisReplicationMode,
        rdb_config: RDBConfig,
    ) -> Self {
        Self {
            address,
            store,
            replication: RedisReplication::new(address, replication_mode),
            rdb_persistence: RDBPesistence::new(rdb_config),
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let (command_tx, mut command_rx) = mpsc::channel(32);
        let server = RedisServer::start(self.address).await?;
        eprintln!("[redis] server started at {}", self.address);

        self.rdb_persistence.setup().await?;
        self.replication.setup(command_tx.clone()).await?;
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
                        self.replication.try_replicate(command.into()).await?;
                    }
                }
                RedisCommand::Server(RedisServerCommand::Ping) => self.ping(write_stream).await?,
                RedisCommand::Server(RedisServerCommand::Echo { message }) => {
                    self.echo(message.clone(), write_stream).await?
                }
                RedisCommand::Server(RedisServerCommand::Config { section }) => {
                    self.config(section, write_stream).await?
                }
                RedisCommand::Replication(command) => {
                    self.replication
                        .handle_command(client_info, command, write_stream)
                        .await?
                }
            }

            self.replication.post_command_hook(&command);
        }

        Ok(())
    }

    async fn ping(&mut self, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        write_stream.write(encoding::bulk_string("PONG")).await
    }

    async fn echo(&mut self, message: Bytes, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        write_stream.write(encoding::bulk_string(message)).await
    }

    async fn config(
        &mut self,
        section: &ConfigSection,
        write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        match section {
            ConfigSection::Get { keys } => {
                let mut values = vec![];
                for key in keys {
                    values.push(encoding::bulk_string(key));
                    if &**key == b"dir" {
                        values.push(encoding::bulk_string(&self.rdb_persistence.config.dir));
                    } else if &**key == b"dbfilename" {
                        values.push(encoding::bulk_string(&self.rdb_persistence.config.file_name));
                    } else {
                        return Err(anyhow::anyhow!(
                            "[redis - error] unexpected configuration key found"
                        ));
                    }
                }

                write_stream.write(encoding::array(values)).await
            }
        }
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
