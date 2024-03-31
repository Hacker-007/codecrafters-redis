mod redis;

use bytes::BytesMut;
use redis::{server::RedisServer, CommandPacket, Redis, RedisMode};

fn parse_option<T>(option_name: &str, option_parser: impl Fn(std::env::Args) -> T) -> Option<T> {
    let mut args = std::env::args();
    args.find(|arg_name| arg_name == option_name)
        .map(|_| (option_parser)(args))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port = parse_option("--port", |mut args| {
        args.next()
            .expect("[redis - error] value expected for port")
            .parse::<u64>()
            .expect("[redis - error] expected port value to be a positive number")
    })
    .unwrap_or(6379);

    let redis_mode = parse_option("--replicaof", |mut args| {
        (
            args.next()
                .expect("[redis - error] master host expected for replica"),
            args.next()
                .expect("[redis - error] master port expected for replic"),
        )
    })
    .map(|(host, port)| RedisMode::Slave {
        master_host: host,
        master_port: port,
    })
    .unwrap_or(RedisMode::Master {
        replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        replication_offset: 0,
    });

    let cloned_redis_mode = redis_mode.clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        let mut redis = Redis::new(redis_mode);
        while let Some(CommandPacket { command, response_tx }) = rx.recv().await {
            let mut output_sink = BytesMut::with_capacity(4096);
            redis.handle(command, &mut output_sink)?;
            if let Err(_) = response_tx.send(output_sink) {
                eprintln!("[redis - error] unknown error occurred when sending response")
            }
        }

        anyhow::Ok(())
    });
    
    let server = RedisServer::start(port, cloned_redis_mode).await?;
    server.run(tx).await
}
