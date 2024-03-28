use tokio::{
    io::AsyncWriteExt, net::{
        TcpListener, TcpStream,
    }, sync::{mpsc, oneshot}
};

use crate::redis::{command::Command, resp::RESPValueReader};

use super::CommandPacket;

pub struct RedisServer {
    tcp_listener: TcpListener,
}

impl RedisServer {
    pub async fn start(port: u64) -> anyhow::Result<Self> {
        let tcp_listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        Ok(Self { tcp_listener })
    }

    pub async fn run(&self, tx: mpsc::Sender<CommandPacket>) -> anyhow::Result<()> {
        loop {
            let (client_stream, addr) = self.tcp_listener.accept().await?;
            eprintln!("[redis] connection established with {addr}");
            let tx = tx.clone();
            tokio::spawn(async move {
                let _ = Self::process_stream(client_stream, tx);
                eprintln!("[redis] connection closed with {addr}");
                anyhow::Ok(())
            });
        }
    }

    async fn process_stream(
        mut client_stream: TcpStream,
        tx: mpsc::Sender<CommandPacket>,
    ) -> anyhow::Result<()> {
        let (mut read_stream, mut write_stream) = client_stream.split();
        let mut reader = RESPValueReader::new();
        loop {
            let value = reader.next_value(&mut read_stream).await?;
            let command: Command = value.try_into()?;
            let (response_tx, response_rx) = oneshot::channel();
            tx.send(CommandPacket::new(command, response_tx)).await?;
            let response_bytes = response_rx.await?;
            write_stream.write_all(&response_bytes).await?;
        }
    }
}
