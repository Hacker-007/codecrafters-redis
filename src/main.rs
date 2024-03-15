mod redis;

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

use crate::redis::{commands::RedisCommand, resp_reader::RESPReader, value::RedisValue, Redis};

fn get_stream_ip(stream: &TcpStream) -> anyhow::Result<String> {
    let ip = match stream.local_addr()? {
        SocketAddr::V4(addr) => addr.ip().to_string(),
        SocketAddr::V6(addr) => addr.ip().to_string(),
    };

    Ok(ip)
}

async fn process_stream(mut stream: TcpStream, redis: Arc<Redis>) -> anyhow::Result<()> {
    let mut reader = RESPReader::new(stream.try_clone()?);
    loop {
        let Ok(value) = RedisValue::parse(&mut reader) else {
            break
        };

        if let RedisValue::Array(values) = value {
            let command: RedisCommand = values.try_into()?;
            redis.handle_command(command, &mut stream)?;
        } else {
            println!("[redis - error] expected a command encoded as an array of binary strings")
        }
    }

    Ok(())
}

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

    let redis = parse_option("--replicaof", |mut args| {
        (
            args.next()
                .expect("[redis - error] master host expected for replica"),
            args.next()
                .expect("[redis - error] master port expected for replic"),
        )
    })
    .map(|(master_host, master_port)| Redis::slave(master_host, master_port))
    .unwrap_or(Redis::master(
        "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        "0".to_string(),
    ));

    let url = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&url)?;
    println!("[redis] server started at {url}");

    let redis = Arc::new(redis);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let ip = get_stream_ip(&stream)?;
                println!("[redis] connection established with {ip}");
                let redis = Arc::clone(&redis);
                tokio::spawn(async move {
                    process_stream(stream, redis).await?;
                    println!("[redis] connection closed with {ip}");
                    Ok::<(), anyhow::Error>(())
                });
            }
            Err(err) => {
                eprintln!("[redis - error] unknown error occurred: {}", err);
            }
        }
    }

    Ok(())
}
