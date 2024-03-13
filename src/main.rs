mod redis;

use std::net::{SocketAddr, TcpListener, TcpStream};

use crate::redis::Redis;

fn get_stream_ip(stream: &TcpStream) -> anyhow::Result<String> {
    let ip = match stream.local_addr()? {
        SocketAddr::V4(addr) => addr.ip().to_string(),
        SocketAddr::V6(addr) => addr.ip().to_string(),
    };

    Ok(ip)
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
    let listener = TcpListener::bind(&url).unwrap();
    
    println!("[redis] server started at {url}");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                tokio::spawn(async move {
                    let ip = get_stream_ip(&stream)?;
                    println!("[redis] connection established with {ip}");
                    let mut redis = Redis::new(stream)?;
                    if redis.run().is_err() {
                        println!("[redis] connection closed with {ip}");
                    }

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
