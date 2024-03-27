use tokio::net::TcpListener;

pub enum RedisMode {
    Slave {
        master_host: String,
        master_port: String,
    },
    Master,
}

pub struct Redis {
    tcp_listener: TcpListener,
    port: u64,
    mode: RedisMode,
}

impl Redis {
    pub async fn start(port: u64, mode: RedisMode) -> anyhow::Result<Self> {
        let tcp_listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        Ok(Self {
            tcp_listener,
            port,
            mode,
        })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        loop {
            let (client_stream, addr) = self.tcp_listener.accept().await?;
            let (read_stream, write_stream) = client_stream.into_split();
            eprintln!("[redis] client connected at {addr}");
        }
    }
}
