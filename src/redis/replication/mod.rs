use std::{collections::HashMap, fmt::Debug, net::SocketAddr, ops::Deref};

use bytes::Bytes;
use tokio::sync::mpsc;

use self::acker::Acker;

use super::{
    manager::RedisCommandPacket,
    resp::command::RedisCommand,
    server::{ClientId, RedisWriteStream},
};

mod acker;
pub mod command;
pub mod handler;
pub mod handshake;

pub struct ReplicaInfo {
    id: ClientId,
    write_stream: RedisWriteStream,
    acker: Acker,
}

impl Debug for ReplicaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaInfo")
            .field("id", &self.id)
            .field("acked_bytes", &self.acker.get_bytes())
            .finish()
    }
}

pub enum RedisReplicationMode {
    Primary {
        replication_id: String,
        replication_offset: u64,
        replicas: HashMap<ClientId, ReplicaInfo>,
        replicated_bytes: usize,
    },
    Replica {
        primary_host: String,
        primary_port: u16,
        processed_bytes: usize,
    },
}

impl RedisReplicationMode {
    pub fn primary(replication_id: String) -> Self {
        Self::Primary {
            replication_id,
            replication_offset: 0,
            replicas: HashMap::default(),
            replicated_bytes: 0,
        }
    }

    pub fn replica(primary_host: String, primary_port: u16) -> Self {
        Self::Replica {
            primary_host,
            primary_port,
            processed_bytes: 0,
        }
    }
}

pub struct RedisReplication {
    address: SocketAddr,
    replication_mode: RedisReplicationMode,
}

impl RedisReplication {
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
                (primary_host.deref(), *primary_port),
                command_tx.clone(),
            )
            .await?;
        }

        Ok(())
    }

    pub async fn try_replicate(&mut self, bytes: Bytes) -> anyhow::Result<()> {
        if let RedisReplicationMode::Primary {
            ref replicas,
            ref mut replicated_bytes,
            ..
        } = &mut self.replication_mode
        {
            *replicated_bytes += bytes.len();
            for replica_info in replicas.values() {
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
            let bytes = Bytes::from(command);
            *processed_bytes += bytes.len();
        }
    }

    fn add_replica(&mut self, replica_info: ReplicaInfo) {
        if let RedisReplicationMode::Primary { replicas, .. } = &mut self.replication_mode {
            replicas.insert(replica_info.id, replica_info);
        }
    }
}
