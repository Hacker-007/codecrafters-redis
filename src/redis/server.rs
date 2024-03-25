use tokio::net::TcpListener;

pub struct RedisServer {

}

impl RedisServer {
    pub async fn start() -> anyhow::Result<TcpListener> {
        let listener = TcpListener::bind("127.0.0.1:6379").await?;

        Ok(listener)
    }
}