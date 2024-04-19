use anyhow::Context;
use bytes::Bytes;
use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream, ToSocketAddrs,
    },
    sync::mpsc,
};

use crate::redis::{
    manager::RedisCommandPacket,
    resp::{command::RedisCommand, encoding, resp_reader::RESPReader, RESPValue},
    server::{ClientId, RedisWriteStream},
};

pub async fn complete_handshake(
    replica_port: u16,
    primary_address: impl ToSocketAddrs,
    command_tx: mpsc::Sender<RedisCommandPacket>,
) -> anyhow::Result<()> {
    let primary_stream = TcpStream::connect(primary_address).await?;
    let (read_stream, mut write_stream) = primary_stream.into_split();
    let mut read_stream = RESPReader::new(read_stream);
    send_ping(&mut read_stream, &mut write_stream).await?;
    send_replconf_port(&mut read_stream, &mut write_stream, replica_port).await?;
    send_replconf_capa(&mut read_stream, &mut write_stream).await?;
    send_psync(read_stream, write_stream, command_tx).await?;

    Ok(())
}

async fn send_ping(
    read_stream: &mut RESPReader<OwnedReadHalf>,
    write_stream: &mut OwnedWriteHalf,
) -> anyhow::Result<()> {
    write_stream.write_all(&encoding::ping()).await?;
    match read_stream.read_value().await {
        Ok(RESPValue::SimpleString(s)) if &*s == b"PONG" => Ok(()),
        _ => Err(anyhow::anyhow!(
            "[redis - error] expected simple-string encoded 'PONG' from primary"
        )),
    }
}

async fn send_replconf_port(
    read_stream: &mut RESPReader<OwnedReadHalf>,
    write_stream: &mut OwnedWriteHalf,
    port: u16,
) -> anyhow::Result<()> {
    write_stream
        .write_all(&encoding::replconf_port(port))
        .await?;
    match read_stream.read_value().await {
        Ok(RESPValue::SimpleString(s)) if &*s == b"OK" => Ok(()),
        _ => Err(anyhow::anyhow!(
            "[redis - error] expected simple-string encoded 'OK' from primary"
        )),
    }
}

async fn send_replconf_capa(
    read_stream: &mut RESPReader<OwnedReadHalf>,
    write_stream: &mut OwnedWriteHalf,
) -> anyhow::Result<()> {
    write_stream
        .write_all(&encoding::replconf_capa(&[Bytes::from_static(b"psync2")]))
        .await?;
    match read_stream.read_value().await {
        Ok(RESPValue::SimpleString(s)) if &*s == b"OK" => Ok(()),
        _ => Err(anyhow::anyhow!(
            "[redis - error] expected simple-string encoded 'OK' from primary"
        )),
    }
}

async fn send_psync(
    mut read_half: RESPReader<OwnedReadHalf>,
    mut write_half: OwnedWriteHalf,
    command_tx: mpsc::Sender<RedisCommandPacket>,
) -> anyhow::Result<()> {
    write_half.write_all(&encoding::psync("?", -1)).await?;
    let response = read_half.read_value().await?;
    let response = if let RESPValue::SimpleString(response) = response {
        String::from_utf8(response.to_vec())?
    } else {
        return Err(anyhow::anyhow!(
            "[redis - error] expected a simple-string encoded response from the primary"
        ));
    };

    if let Some(primary_info) = response.strip_prefix("FULLRESYNC ") {
        let mut primary_info = primary_info.split_ascii_whitespace();
        let _replication_id = primary_info.next().unwrap();
        let _replication_offset = primary_info.next().unwrap().parse::<usize>()?;
        let _rdb_file = read_half.read_rdb_file().await?;

        let write_stream = setup_replica_write_stream(write_half);
        tokio::spawn(async move {
            loop {
                let command = read_half
                    .read_value()
                    .await
                    .and_then(|value| value.try_into())
                    .context("[redis - error] unable to parse RESP value into command")?;

                let mut write_stream = write_stream.clone();
                match command {
                    RedisCommand::Replication(ref command) if command.is_getack() => {}
                    _ => {
                        write_stream.close();
                    }
                }

                let packet = RedisCommandPacket::new(ClientId::primary(), command, write_stream);
                if read_half.is_closed() || command_tx.send(packet).await.is_err() {
                    break;
                }
            }

            anyhow::Ok(())
        });

        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "[redis - error] expected 'FULLRESYNC' from primary but got '{response}'"
        ))
    }
}

fn setup_replica_write_stream(mut write_half: OwnedWriteHalf) -> RedisWriteStream {
    let (write_tx, mut write_rx) = mpsc::channel::<Bytes>(32);
    let write_stream = RedisWriteStream::new(write_tx);
    tokio::spawn(async move {
        while let Some(bytes) = write_rx.recv().await {
            write_half.write_all(&bytes).await?;
        }

        anyhow::Ok(())
    });

    write_stream
}
