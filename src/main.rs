use redis::{
    manager::RedisManager, rdb::RDBConfig, replication::RedisReplicationMode, store::RedisStore,
};

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

    let replication_mode = parse_option("--replicaof", |mut args| {
        (
            args.next()
                .expect("[redis - error] expected host of primary for replica to connect to"),
            args.next()
                .expect("[redis - error] expected port of primary for replica to connect to"),
        )
    });

    let rdb_dir = parse_option("--dir", |mut args| {
        args.next()
            .expect("[redis - error] value expected for RDB directory")
    })
    .unwrap_or("./".to_string());

    let rdb_file_name = parse_option("--dbfilename", |mut args| {
        args.next()
            .expect("[redis - error] value expected for RDB file name")
    })
    .unwrap_or("dump.rdb".to_string());

    let mode = if let Some((primary_host, primary_port)) = replication_mode {
        let primary_port = primary_port.parse()?;
        RedisReplicationMode::replica(primary_host, primary_port)
    } else {
        RedisReplicationMode::primary("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string())
    };

    let store = RedisStore::new();
    RedisManager::new(
        (host, port).into(),
        store,
        mode,
        RDBConfig::new(rdb_dir, rdb_file_name),
    )
    .start()
    .await
}
