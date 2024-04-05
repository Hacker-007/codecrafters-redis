use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpListener, TcpStream,
    },
    sync::{mpsc, oneshot},
};

use crate::redis::{command::Command, resp::RESPValueReader};

use super::{resp::RESPValue, resp_builder::RESPBuilder, CommandPacket, RedisMode};

pub struct RedisServer {
    tcp_listener: TcpListener,
    port: u64,
    mode: RedisMode,
}

impl RedisServer {
    pub async fn start(port: u64, mode: RedisMode) -> anyhow::Result<Self> {
        let server_address = format!("127.0.0.1:{}", port);
        let tcp_listener = TcpListener::bind(&server_address).await?;
        eprintln!("[redis] started server at {server_address}");

        Ok(Self {
            tcp_listener,
            port,
            mode,
        })
    }

    pub async fn run(&self, tx: mpsc::Sender<CommandPacket>) -> anyhow::Result<()> {
        if let RedisMode::Replica {
            primary_host,
            primary_port,
        } = &self.mode
        {
            self.connect_to_primary(primary_host, primary_port, tx.clone())
                .await?;
        }

        loop {
            let (client_stream, addr) = self.tcp_listener.accept().await?;
            eprintln!("[redis] connection established with {addr}");
            let tx = tx.clone();
            tokio::spawn(async move {
                let _ = Self::process_stream(client_stream, tx).await;
                eprintln!("[redis] connection closed with {addr}");
                anyhow::Ok(())
            });
        }
    }

    async fn connect_to_primary(
        &self,
        primary_host: &str,
        primary_port: &str,
        _tx: mpsc::Sender<CommandPacket>,
    ) -> anyhow::Result<()> {
        let mut stream = TcpStream::connect(format!("{primary_host}:{primary_port}")).await?;
        let (mut read_stream, mut write_stream) = stream.split();
        let mut reader = RESPValueReader::new();
        self.send_ping(&mut write_stream, &mut read_stream, &mut reader)
            .await?;
        self.send_replconf_port(&mut write_stream, &mut read_stream, &mut reader)
            .await?;
        self.send_replconf_capa(&mut write_stream, &mut read_stream, &mut reader)
            .await?;
        self.send_psync(&mut write_stream, &mut read_stream, &mut reader)
            .await?;

        Ok(())
    }

    async fn process_stream(
        mut client_stream: TcpStream,
        tx: mpsc::Sender<CommandPacket>,
    ) -> anyhow::Result<()> {
        let (mut read_stream, mut write_stream) = client_stream.split();
        let mut reader = RESPValueReader::new();
        loop {
            let value = reader.read_value(&mut read_stream).await?;
            let command: Command = value.try_into()?;
            let (response_tx, response_rx) = oneshot::channel();
            tx.send(CommandPacket::new(command, response_tx)).await?;
            let response_bytes = response_rx.await?;
            write_stream.write_all(&response_bytes).await?;
        }
    }

    async fn send_ping(
        &self,
        write_stream: &mut WriteHalf<'_>,
        read_stream: &mut ReadHalf<'_>,
        reader: &mut RESPValueReader,
    ) -> anyhow::Result<()> {
        let ping = format!("{}", RESPBuilder::array().bulk("ping").build());
        write_stream.write(ping.as_bytes()).await?;
        let response = reader.read_value(read_stream).await;
        dbg!(&response);
        match &response {
            Ok(RESPValue::SimpleString(s)) if s.to_ascii_lowercase() == "pong" => Ok(()),
            Ok(response) => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'PONG' from primary but got '{response}'"
            )),
            Err(_) => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'PONG' from primary but got nothing"
            )),
        }
    }

    async fn send_replconf_port(
        &self,
        write_stream: &mut WriteHalf<'_>,
        read_stream: &mut ReadHalf<'_>,
        reader: &mut RESPValueReader,
    ) -> anyhow::Result<()> {
        let replconf = format!(
            "{}",
            RESPBuilder::array()
                .bulk("replconf")
                .bulk("listening-port")
                .bulk(self.port)
                .build()
        );
        write_stream.write(replconf.as_bytes()).await?;
        let response = reader.read_value(read_stream).await;
        match &response {
            Ok(RESPValue::SimpleString(s)) if s.to_ascii_lowercase() == "ok" => Ok(()),
            Ok(response) => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'OK' from primary but got '{response}'"
            )),
            Err(_) => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'OK' from primary but got nothing"
            )),
        }
    }

    async fn send_replconf_capa(
        &self,
        write_stream: &mut WriteHalf<'_>,
        read_stream: &mut ReadHalf<'_>,
        reader: &mut RESPValueReader,
    ) -> anyhow::Result<()> {
        let replconf = format!(
            "{}",
            RESPBuilder::array()
                .bulk("replconf")
                .bulk("capa")
                .bulk("psync2")
                .build()
        );
        write_stream.write(replconf.as_bytes()).await?;
        let response = reader.read_value(read_stream).await;
        match &response {
            Ok(RESPValue::SimpleString(s)) if s.to_ascii_lowercase() == "ok" => Ok(()),
            Ok(response) => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'OK' from primary but got '{response}'"
            )),
            Err(_) => Err(anyhow::anyhow!(
                "[redis - error] expected simple-string encoded 'OK' from primary but got nothing"
            )),
        }
    }

    async fn send_psync(
        &self,
        write_stream: &mut WriteHalf<'_>,
        read_stream: &mut ReadHalf<'_>,
        reader: &mut RESPValueReader,
    ) -> anyhow::Result<()> {
        let psync = format!(
            "{}",
            RESPBuilder::array()
                .bulk("psync")
                .bulk("?")
                .bulk("-1")
                .build()
        );
        write_stream.write(psync.as_bytes()).await?;
        let response = reader.read_value(read_stream).await;
        let Ok(RESPValue::SimpleString(response)) = response else {
            return Err(anyhow::anyhow!("[redis - error] expected a simple-string encoded response from the primary"))
        };

        if let Some(primary_info) = response.strip_prefix("FULLRESYNC ") {
            let mut primary_info = primary_info.split_ascii_whitespace();
            let _replication_id = primary_info.next().unwrap();
            let _replication_offset = primary_info.next().unwrap().parse::<usize>()?;
            let _rdb_file = reader.read_rdb_file(read_stream).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "[redis - error] expected 'FULLRESYNC' from primary but got '{response}'"
            ))
        }
    }
}
