use bytes::{Bytes, BytesMut};

use crate::redis::{
    resp::RESPValue,
    server::{ClientId, RedisWriteStream},
};

use super::{
    command::{InfoSection, RedisReplicationCommand, ReplConfSection},
    RedisReplicationMode, RedisReplicator, ReplicaInfo,
};

const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

impl RedisReplicator {
    pub async fn handle_command(
        &mut self,
        id: ClientId,
        command: &RedisReplicationCommand,
        write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        match command {
            RedisReplicationCommand::Info { section } => self.info(*section, write_stream).await?,
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::Port { .. },
            } => self.repl_conf_port(write_stream).await?,
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::Capa { .. },
            } => self.repl_conf_capa(write_stream).await?,
            RedisReplicationCommand::PSync { .. } => {
                self.psync(write_stream.clone()).await?;
                self.add_replica(ReplicaInfo {
                    id,
                    write_stream,
                    acked_bytes: 0,
                });
            }
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::GetAck,
            } => self.getack(write_stream).await?,
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::Ack { processed_bytes },
            } => self.ack(id, *processed_bytes).await?,
            RedisReplicationCommand::Wait {
                num_replicas,
                timeout,
            } => {
                self.wait(*num_replicas, *timeout, write_stream).await?;
            }
        }

        Ok(())
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

    async fn ack(&mut self, id: ClientId, processed_bytes: usize) -> anyhow::Result<()> {
        if let RedisReplicationMode::Primary { replicas, .. } = &mut self.replication_mode {
            let replica_info = replicas.get_mut(&id).ok_or_else(|| {
                anyhow::anyhow!("[redis - error] reference to replica with unknown client id")
            })?;

            replica_info.acked_bytes = processed_bytes;
            Ok(())
        } else {
            Err(anyhow::anyhow!("[redis - error] Redis must be running as a primary to handle 'replconf ack' response"))
        }
    }

    async fn wait(
        &mut self,
        _num_replicas: usize,
        _timeout: usize,
        write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        if let RedisReplicationMode::Primary { replicas, .. } = &self.replication_mode {
            let replica_count = format!(":{}\r\n", replicas.len());
            write_stream
                .write(Bytes::copy_from_slice(replica_count.as_bytes()))
                .await
        } else {
            Err(anyhow::anyhow!("[redis - error] Redis must be running in primary mode to respond to 'WAIT' command"))
        }
    }
}
