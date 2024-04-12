use std::collections::HashMap;

use redis::{manager::RedisManager, replication::RedisReplicationMode, store::RedisStore};

mod redis;

fn parse_option<T>(option_name: &str, option_parser: impl Fn(std::env::Args) -> T) -> Option<T> {
    let mut args = std::env::args();
    args.find(|arg_name| arg_name == option_name)
        .map(|_| (option_parser)(args))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let host = [127, 0, 0, 1];
    let port = parse_option("--port", |mut args| {
        args.next()
            .expect("[redis - error] value expected for port")
            .parse::<u16>()
            .expect("[redis - error] expected port value to be a positive number")
    })
    .unwrap_or(6379);

    let redis_mode = parse_option("--replicaof", |mut args| {
        (
            args.next()
                .expect("[redis - error] expected host of primary for replica to connect to"),
            args.next()
                .expect("[redis - error] expected port of primary for replica to connect to"),
        )
    });

    let mode = if let Some((primary_host, primary_port)) = redis_mode {
        let primary_port = primary_port.parse()?;
        RedisReplicationMode::Replica {
            primary_host,
            primary_port,
            processed_bytes: 0,
        }
    } else {
        RedisReplicationMode::Primary {
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            replication_offset: 0,
            replicas: HashMap::default(),
        }
    };

    let store = RedisStore::new();
    RedisManager::manage(store, mode, (host, port).into())
        .start()
        .await
}
