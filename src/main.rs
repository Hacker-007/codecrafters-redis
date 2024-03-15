mod redis;

use std::sync::Arc;
use std::{
    io::Write,
    net::{SocketAddr, TcpListener, TcpStream},
};

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
            let result = redis.handle_command(command)?;
            write!(stream, "{}", result)?;
        } else {
            println!("[redis - error] expected a command encoded as an array of binary strings")
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args();
    args.next();
    let port = match args.next() {
        Some(ref arg_name) if arg_name == "--port" => args
            .next()
            .expect("[redis - error] value expected for port")
            .parse::<u64>()
            .expect("[redis - error] expected port value to be a positive number"),
        _ => 6379,
    };

    let url = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&url)?;
    println!("[redis] server started at {url}");

    let redis = Arc::new(Redis::new());
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
