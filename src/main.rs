mod redis;

use redis::server::{Redis, RedisMode};

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
    .unwrap_or(RedisMode::Master);

    let redis = Redis::start(port, redis_mode).await?;
    redis.run().await
}
