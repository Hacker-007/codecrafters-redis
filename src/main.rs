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
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    println!("[redis] server started at 127.0.0.1:6379");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                tokio::spawn(async move {
                    let ip = get_stream_ip(&stream)?;
                    println!("[redis] connection established with {ip}");
                    let mut redis = Redis::new(stream)?;
                    if let Err(_) = redis.run() {
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
