use std::net::SocketAddr;

use anyhow::Context;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream, ToSocketAddrs,
    },
    sync::mpsc,
};

use crate::redis::resp::{
    command::{InfoSection, RedisCommand, RedisServerCommand, ReplConfSection},
    RESPValue,
};

use super::{
    resp::{command::RedisStoreCommand, resp_reader::RESPReader},
    server::{RedisReadStream, RedisServer, RedisWriteStream},
    store::RedisStore,
};

const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub struct RedisCommandPacket {
    command: RedisCommand,
    write_stream: RedisWriteStream,
}

pub enum RedisReplicationMode {
    Primary {
        replication_id: String,
        replication_offset: u64,
        replicas: Vec<RedisWriteStream>,
    },
    Replica {
        primary_host: String,
        primary_port: u16,
        processed_bytes: usize,
    },
}

pub struct RedisManager {
    store: RedisStore,
    replication_mode: RedisReplicationMode,
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
            replication_mode,
            address,
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let (command_tx, mut command_rx) = mpsc::channel(32);
        if let RedisReplicationMode::Replica {
            primary_host,
            primary_port,
            ..
        } = &self.replication_mode
        {
            self.complete_handshake(
                (primary_host.to_string(), *primary_port),
                command_tx.clone(),
            )
            .await?;
        }

        let server = RedisServer::start(self.address).await?;
        eprintln!("[redis] server started at {}", self.address);
        self.setup_client_connection_handling(server, command_tx);
        while let Some(RedisCommandPacket {
            command,
            write_stream,
        }) = command_rx.recv().await
        {
            match &command {
                RedisCommand::Store(command) => {
                    let mut output = BytesMut::with_capacity(2048).writer();
                    self.store.handle(command, &mut output)?;
                    write_stream.write(output.into_inner().freeze()).await?;
                    self.try_replicate(command).await?;
                }
                RedisCommand::Server(RedisServerCommand::Ping) => self.ping(write_stream).await?,
                RedisCommand::Server(RedisServerCommand::Echo { echo }) => {
                    self.echo(echo.clone(), write_stream).await?
                }
                RedisCommand::Server(RedisServerCommand::Info { section }) => {
                    self.info(*section, write_stream).await?
                }
                RedisCommand::Server(RedisServerCommand::ReplConf {
                    section: ReplConfSection::Port { .. },
                }) => self.repl_conf_port(write_stream).await?,
                RedisCommand::Server(RedisServerCommand::ReplConf {
                    section: ReplConfSection::Capa { .. },
                }) => self.repl_conf_capa(write_stream).await?,
                RedisCommand::Server(RedisServerCommand::ReplConf {
                    section: ReplConfSection::GetAck,
                }) => self.getack(write_stream).await?,
                RedisCommand::Server(RedisServerCommand::PSync { .. }) => {
                    self.psync(write_stream.clone()).await?;
                    if let RedisReplicationMode::Primary { replicas, .. } =
                        &mut self.replication_mode
                    {
                        replicas.push(write_stream)
                    }
                }
                RedisCommand::Server(RedisServerCommand::Wait { num_replicas, timeout }) => {
                    self.wait(*num_replicas, *timeout, write_stream).await?;
                }
            }

            if let RedisReplicationMode::Replica {
                processed_bytes, ..
            } = &mut self.replication_mode
            {
                let value = RESPValue::from(&command);
                let bytes = Bytes::from(value);
                *processed_bytes += bytes.len();
            }
        }

        Ok(())
    }

    async fn ping(&mut self, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        let response = RESPValue::BulkString(Bytes::from_static(b"PONG"));
        write_stream.write(Bytes::from(response)).await
    }

    async fn echo(&mut self, echo: Bytes, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        let response = RESPValue::BulkString(echo);
        write_stream.write(Bytes::from(response)).await
    }

    async fn info(
        &mut self,
        section: InfoSection,
        write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        match section {
            InfoSection::Default | InfoSection::Replication => {
                let response = match &self.replication_mode {
                    RedisReplicationMode::Primary {
                        replication_id,
                        replication_offset,
                        ..
                    } => format!(
                        "role:master\nmaster_replid:{}\nmaster_repl_offset:{}",
                        replication_id, replication_offset
                    ),
                    RedisReplicationMode::Replica { .. } => "role:slave".to_string(),
                };

                let response = RESPValue::BulkString(Bytes::copy_from_slice(response.as_bytes()));
                write_stream.write(Bytes::from(response)).await
            }
        }
    }

    async fn repl_conf_port(&mut self, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        write_stream.write(Bytes::from_static(b"+OK\r\n")).await
    }

    async fn repl_conf_capa(&mut self, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        write_stream.write(Bytes::from_static(b"+OK\r\n")).await
    }

    async fn psync(&mut self, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        if let RedisReplicationMode::Primary {
            replication_id,
            replication_offset,
            ..
        } = &self.replication_mode
        {
            let resync = format!("+FULLRESYNC {} {}\r\n", replication_id, *replication_offset);
            let bytes = Bytes::copy_from_slice(resync.as_bytes());
            write_stream.write(bytes).await?;
            let rdb_file = (0..EMPTY_RDB_HEX.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&EMPTY_RDB_HEX[i..i + 2], 16))
                .collect::<Result<Bytes, _>>()?;

            let mut bytes = BytesMut::new();
            let prefix = format!("${}\r\n", rdb_file.len());
            bytes.extend_from_slice(prefix.as_bytes());
            bytes.extend_from_slice(&rdb_file);
            write_stream.write(bytes).await
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] Redis must be running in primary mode to respond to 'PSYNC' command"
            ))
        }
    }

    async fn getack(&mut self, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        if let RedisReplicationMode::Replica {
            processed_bytes, ..
        } = &self.replication_mode
        {
            let value = RESPValue::Array(vec![
                RESPValue::BulkString(Bytes::from_static(b"REPLCONF")),
                RESPValue::BulkString(Bytes::from_static(b"ACK")),
                RESPValue::BulkString(Bytes::copy_from_slice(
                    processed_bytes.to_string().as_bytes(),
                )),
            ]);

            let bytes = Bytes::from(value);
            write_stream.write(bytes).await
        } else {
            Err(anyhow::anyhow!("[redis - error] Redis must be running as a replica to respond to 'replconf getack' command"))
        }
    }

    async fn wait(&mut self, _num_replicas: usize, _timeout: usize, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        if let RedisReplicationMode::Primary { replicas, .. } = &self.replication_mode {
            let replica_count = format!(":{}\r\n", replicas.len());    
            write_stream.write(Bytes::copy_from_slice(replica_count.as_bytes())).await
        } else {
            Err(anyhow::anyhow!("[redis - error] Redis must be running in primary mode to respond to 'WAIT' command"))
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
                let (read_stream, write_stream, address) = server.accept().await?;
                eprintln!("[redis] client at {address} connected");
                let command_tx = command_tx.clone();
                tokio::spawn(async move {
                    if let Err(err) =
                        Self::process_stream(read_stream, write_stream, command_tx).await
                    {
                        eprintln!("{err}");
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
}

impl RedisManager {
    async fn complete_handshake(
        &mut self,
        primary_address: impl ToSocketAddrs,
        command_tx: mpsc::Sender<RedisCommandPacket>,
    ) -> anyhow::Result<()> {
        let primary_stream = TcpStream::connect(primary_address).await?;
        let (read_stream, mut write_stream) = primary_stream.into_split();
        let mut read_stream = RESPReader::new(read_stream);
        self.send_ping(&mut read_stream, &mut write_stream).await?;
        self.send_replconf_port(&mut read_stream, &mut write_stream)
            .await?;
        self.send_replconf_capa(&mut read_stream, &mut write_stream)
            .await?;
        self.send_psync(read_stream, write_stream, command_tx)
            .await?;

        Ok(())
    }

    async fn send_ping(
        &mut self,
        read_stream: &mut RESPReader<OwnedReadHalf>,
        write_stream: &mut OwnedWriteHalf,
    ) -> anyhow::Result<()> {
        let ping = RESPValue::Array(vec![RESPValue::BulkString(Bytes::from_static(b"PING"))]);
        let bytes = Bytes::from(ping);
        write_stream.write_all(&bytes).await?;
        match read_stream.read_value().await {
            Ok(RESPValue::SimpleString(s)) if &*s == b"PONG" => Ok(()),
            _ => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'PONG' from primary"
            )),
        }
    }

    async fn send_replconf_port(
        &mut self,
        read_stream: &mut RESPReader<OwnedReadHalf>,
        write_stream: &mut OwnedWriteHalf,
    ) -> anyhow::Result<()> {
        let port = self.address.port();
        let replconf_port = RESPValue::Array(vec![
            RESPValue::BulkString(Bytes::from_static(b"replconf")),
            RESPValue::BulkString(Bytes::from_static(b"listening-port")),
            RESPValue::BulkString(Bytes::copy_from_slice(port.to_string().as_bytes())),
        ]);

        let bytes = Bytes::from(replconf_port);
        write_stream.write_all(&bytes).await?;
        match read_stream.read_value().await {
            Ok(RESPValue::SimpleString(s)) if &*s == b"OK" => Ok(()),
            _ => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'OK' from primary"
            )),
        }
    }

    async fn send_replconf_capa(
        &mut self,
        read_stream: &mut RESPReader<OwnedReadHalf>,
        write_stream: &mut OwnedWriteHalf,
    ) -> anyhow::Result<()> {
        let replconf_capa = RESPValue::Array(vec![
            RESPValue::BulkString(Bytes::from_static(b"replconf")),
            RESPValue::BulkString(Bytes::from_static(b"capa")),
            RESPValue::BulkString(Bytes::from_static(b"psync2")),
        ]);

        let bytes = Bytes::from(replconf_capa);
        write_stream.write_all(&bytes).await?;
        match read_stream.read_value().await {
            Ok(RESPValue::SimpleString(s)) if &*s == b"OK" => Ok(()),
            _ => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'OK' from primary"
            )),
        }
    }

    async fn send_psync(
        &mut self,
        mut read_half: RESPReader<OwnedReadHalf>,
        mut write_half: OwnedWriteHalf,
        command_tx: mpsc::Sender<RedisCommandPacket>,
    ) -> anyhow::Result<()> {
        let psync = RESPValue::Array(vec![
            RESPValue::BulkString(Bytes::from_static(b"psync")),
            RESPValue::BulkString(Bytes::from_static(b"?")),
            RESPValue::BulkString(Bytes::from_static(b"-1")),
        ]);

        let bytes = Bytes::from(psync);
        write_half.write_all(&bytes).await?;
        let response = read_half.read_value().await?;
        let response = if let RESPValue::SimpleString(response) = response {
            String::from_utf8(response.to_vec())?
        } else {
            return Err(anyhow::anyhow!(
                "[redis - error] expected a simple-string encoded response from the primary"
            ));
        };

        if let Some(primary_info) = response.strip_prefix("FULLRESYNC ") {
            let mut primary_info = primary_info.split_ascii_whitespace();
            let _replication_id = primary_info.next().unwrap();
            let _replication_offset = primary_info.next().unwrap().parse::<usize>()?;
            let _rdb_file = read_half.read_rdb_file().await?;
            tokio::spawn(async move {
                let (write_tx, mut write_rx) = mpsc::channel::<Bytes>(32);
                let write_stream = RedisWriteStream::new(write_tx);
                tokio::spawn(async move {
                    while let Some(bytes) = write_rx.recv().await {
                        write_half.write_all(&bytes).await?;
                    }

                    anyhow::Ok(())
                });

                loop {
                    let command = read_half
                        .read_value()
                        .await
                        .and_then(|value| value.try_into())
                        .context("[redis - error] unable to parse RESP value into command")?;

                    let mut write_stream = write_stream.clone();
                    // NOTE: replica should only respond to 'getack' command sent by primary
                    if !matches!(
                        command,
                        RedisCommand::Server(RedisServerCommand::ReplConf {
                            section: ReplConfSection::GetAck
                        })
                    ) {
                        write_stream.close();
                    }

                    let packet = RedisCommandPacket {
                        command,
                        write_stream,
                    };

                    if read_half.is_closed() || command_tx.send(packet).await.is_err() {
                        break;
                    }
                }

                anyhow::Ok(())
            });

            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] expected 'FULLRESYNC' from primary but got '{response}'"
            ))
        }
    }
}

impl RedisManager {
    async fn try_replicate(&self, command: &RedisStoreCommand) -> anyhow::Result<()> {
        if command.is_write() {
            if let RedisReplicationMode::Primary { replicas, .. } = &self.replication_mode {
                let value: RESPValue = command.into();
                let bytes: Bytes = value.into();
                for replica in replicas {
                    replica.write(bytes.clone()).await?;
                }
            }
        }

        Ok(())
    }
}
