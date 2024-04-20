use std::{sync::atomic::Ordering, time::Duration};

use bytes::Bytes;
use tokio::task::JoinSet;

use crate::redis::{
    resp::encoding,
    server::{ClientConnectionInfo, ClientId, RedisWriteStream},
};

use super::{
    acker::Acker,
    command::{InfoSection, RedisReplicationCommand, ReplConfSection},
    RedisReplication, RedisReplicationMode, ReplicaInfo,
};

const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

impl RedisReplication {
    pub async fn handle_command(
        &mut self,
        client_info: ClientConnectionInfo,
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
                    id: client_info.id,
                    write_stream,
                    acker: Acker::new(0),
                });
            }
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::GetAck,
            } => self.getack(write_stream).await?,
            RedisReplicationCommand::ReplConf {
                section: ReplConfSection::Ack { processed_bytes },
            } => self.ack(client_info.id, *processed_bytes).await?,
            RedisReplicationCommand::Wait {
                num_replicas,
                timeout,
            } => {
                self.wait(client_info, *num_replicas, *timeout, write_stream)
                    .await?;
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
                let info = match &self.replication_mode {
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

                write_stream.write(encoding::bulk_string(info)).await
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
            let resync = encoding::simple_string(format!(
                "+FULLRESYNC {} {}",
                replication_id, *replication_offset
            ));

            write_stream.write(resync).await?;
            let rdb_file = (0..EMPTY_RDB_HEX.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&EMPTY_RDB_HEX[i..i + 2], 16))
                .collect::<Result<Bytes, _>>()?;

            let rdb_file: Bytes = encoding::bulk_string(rdb_file).into();
            let rdb_file = rdb_file.slice(0..rdb_file.len() - 2);
            write_stream.write(rdb_file).await
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
            write_stream
                .write(encoding::replconf_ack(*processed_bytes))
                .await
        } else {
            Err(anyhow::anyhow!("[redis - error] Redis must be running as a replica to respond to 'replconf getack' command"))
        }
    }

    async fn ack(&mut self, id: ClientId, processed_bytes: usize) -> anyhow::Result<()> {
        if let RedisReplicationMode::Primary { replicas, .. } = &mut self.replication_mode {
            let replica_info = replicas.get_mut(&id).ok_or_else(|| {
                anyhow::anyhow!("[redis - error] reference to replica with unknown client id")
            })?;

            replica_info.acker.ack(processed_bytes);
            Ok(())
        } else {
            Err(anyhow::anyhow!("[redis - error] Redis must be running as a primary to handle 'replconf ack' response"))
        }
    }

    async fn wait(
        &mut self,
        client_info: ClientConnectionInfo,
        num_replicas: usize,
        timeout: usize,
        write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        if let RedisReplicationMode::Primary {
            replicas,
            replicated_bytes,
            ..
        } = &mut self.replication_mode
        {
            let mut join_set = JoinSet::new();
            let mut acked_replicas = replicas
                .values()
                .filter(|replica_info| replica_info.acker.get_bytes() == *replicated_bytes)
                .count();

            let replica_count = replicas.len();
            if acked_replicas >= std::cmp::min(num_replicas, replica_count) {
                let replica_count: i64 = acked_replicas.try_into()?;
                return write_stream.write(encoding::integer(replica_count)).await;
            }

            client_info.is_read_blocked.store(true, Ordering::SeqCst);
            let bytes = encoding::replconf_get_ack();
            *replicated_bytes += bytes.len();
            let expected_acked_bytes = *replicated_bytes - bytes.len();
            for replica_info in replicas.values_mut() {
                let mut rx = replica_info.acker.subscribe();
                replica_info.write_stream.write(bytes.clone()).await?;
                join_set.spawn(async move {
                    rx.recv()
                        .await
                        .map(|acked_bytes| acked_bytes == expected_acked_bytes)
                });
            }

            tokio::spawn(async move {
                let timeout_millis = timeout.try_into()?;
                let _ = tokio::time::timeout(Duration::from_millis(timeout_millis), async {
                    while let Some(Ok(Ok(is_up_to_date))) = join_set.join_next().await {
                        if is_up_to_date {
                            acked_replicas += 1;
                            if acked_replicas >= std::cmp::min(num_replicas, replica_count) {
                                break;
                            }
                        }
                    }
                })
                .await;

                client_info.is_read_blocked.store(false, Ordering::SeqCst);
                let replica_count: i64 = acked_replicas.try_into()?;
                write_stream.write(encoding::integer(replica_count)).await
            });

            Ok(())
        } else {
            Err(anyhow::anyhow!("[redis - error] Redis must be running in primary mode to respond to 'WAIT' command"))
        }
    }
}
