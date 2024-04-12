use std::{collections::HashMap, fmt::Debug, net::SocketAddr};

use bytes::Bytes;
use tokio::sync::mpsc;

use super::{
    manager::RedisCommandPacket,
    resp::{command::RedisCommand, RESPValue},
    server::{ClientId, RedisWriteStream},
};

pub mod command;
pub mod handler;
pub mod handshake;

pub struct ReplicaInfo {
    id: ClientId,
    write_stream: RedisWriteStream,
    acked_bytes: usize,
}

impl Debug for ReplicaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaInfo")
            .field("id", &self.id)
            .field("acked_bytes", &self.acked_bytes)
            .finish()
    }
}

pub enum RedisReplicationMode {
    Primary {
        replication_id: String,
        replication_offset: u64,
        replicas: HashMap<ClientId, ReplicaInfo>,
    },
    Replica {
        primary_host: String,
        primary_port: u16,
        processed_bytes: usize,
    },
}

pub struct RedisReplicator {
    address: SocketAddr,
    replication_mode: RedisReplicationMode,
}

impl RedisReplicator {
    pub fn new(address: SocketAddr, replication_mode: RedisReplicationMode) -> Self {
        Self {
            address,
            replication_mode,
        }
    }

    pub async fn setup(
        &mut self,
        command_tx: mpsc::Sender<RedisCommandPacket>,
    ) -> anyhow::Result<()> {
        if let RedisReplicationMode::Replica {
            primary_host,
            primary_port,
            ..
        } = &self.replication_mode
        {
            handshake::complete_handshake(
                self.address.port(),
                (primary_host.to_string(), *primary_port),
                command_tx.clone(),
            )
            .await?;
        }

        Ok(())
    }

    pub async fn try_replicate(&self, bytes: Bytes) -> anyhow::Result<()> {
        if let RedisReplicationMode::Primary { replicas, .. } = &self.replication_mode {
            for (_, replica_info) in replicas {
                replica_info.write_stream.write(bytes.clone()).await?;
            }
        }

        Ok(())
    }

    pub fn post_command_hook(&mut self, command: &RedisCommand) {
        if let RedisReplicationMode::Replica {
            processed_bytes, ..
        } = &mut self.replication_mode
        {
            let value = RESPValue::from(command);
            let bytes = Bytes::from(value);
            *processed_bytes += bytes.len();
        }
    }

    fn add_replica(&mut self, replica_info: ReplicaInfo) {
        if let RedisReplicationMode::Primary { replicas, .. } = &mut self.replication_mode {
            replicas.insert(replica_info.id, replica_info);
        }
    }
}
