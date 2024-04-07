mod redis;

fn parse_option<T>(option_name: &str, option_parser: impl Fn(std::env::Args) -> T) -> Option<T> {
    let mut args = std::env::args();
    args.find(|arg_name| arg_name == option_name)
        .map(|_| (option_parser)(args))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _port = parse_option("--port", |mut args| {
        args.next()
            .expect("[redis - error] value expected for port")
            .parse::<u64>()
            .expect("[redis - error] expected port value to be a positive number")
    })
    .unwrap_or(6379);

    let _redis_mode = parse_option("--replicaof", |mut args| {
        (
            args.next()
                .expect("[redis - error] primary host expected for replica"),
            args.next()
                .expect("[redis - error] primary port expected for replic"),
        )
    });
    
    Ok(())
}
