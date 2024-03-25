mod redis;

use redis::server::RedisServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let redis = RedisServer::start().await?;
    loop {
        match redis.accept().await {
            Ok((stream, addr)) => {
                eprintln!("[redis] connected to client at {addr}")
                
            }
            Err(_) => eprintln!("[redis - error] unable to connect to client"),
        }
    }
}