#![allow(unused)]

use redis::server::{RedisReadStream, RedisWriteStream};
use tokio::sync::mpsc;

use crate::redis::{
    server::RedisServer,
    store::{RedisCommandPacket, RedisMode, RedisStore},
};

mod redis;

async fn process_stream(
    mut read_stream: RedisReadStream,
    write_stream: RedisWriteStream,
    store_tx: mpsc::Sender<RedisCommandPacket>,
) -> anyhow::Result<()> {
    loop {
        let command = match read_stream.read().await? {
            Some(command) => command,
            None => break,
        };

        store_tx
            .send(RedisCommandPacket {
                command,
                write_stream: write_stream.clone(),
            })
            .await?;
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

    let redis_mode = parse_option("--replicaof", |mut args| {
        (
            args.next()
                .expect("[redis - error] expected host of primary for replica to connect to"),
            args.next()
                .expect("[redis - error] expected port of primary for replica to connect to"),
        )
    });

    let mut store = if let Some((primary_host, primary_port)) = redis_mode {
        let primary_port = primary_port.parse()?;
        RedisStore::replica(primary_host, primary_port)
    } else {
        RedisStore::primary("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(), 0)
    };

    store.start(port as usize).await?;
    let (store_tx, mut store_rx) = mpsc::channel(32);
    tokio::spawn(async move {
        while let Some(RedisCommandPacket {
            command,
            write_stream,
        }) = store_rx.recv().await
        {
            if store.handle(command, write_stream).await.is_err() {
                eprintln!("[redis - error] an unknown error occurred when handling client command");
                break;
            }
        }

        anyhow::Ok(())
    });

    let mut server = RedisServer::bind(format!("127.0.0.1:{port}")).await?;
    eprintln!("[redis] server started at 127.0.0.1:6379");
    loop {
        let (read_stream, write_stream, address) = server.accept().await?;
        let store_tx = store_tx.clone();
        tokio::task::spawn(async move {
            eprintln!("[redis] connection established with {address}");
            let _ = process_stream(read_stream, write_stream, store_tx).await;
            eprintln!("[redis] connection closed with {address}");
        });
    }
}
