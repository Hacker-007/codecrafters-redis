use std::{collections::HashMap, time::SystemTime};

use bytes::Bytes;

use super::{
    resp::{command::RedisStoreCommand, encoding},
    server::RedisWriteStream,
};

type StoreKey = Bytes;

#[derive(Debug)]
pub struct StoreValue {
    value: Bytes,
    expiration: Option<SystemTime>,
}

#[derive(Debug)]
pub struct RedisStore {
    items: HashMap<StoreKey, StoreValue>,
}

impl RedisStore {
    pub fn new() -> Self {
        Self {
            items: HashMap::default(),
        }
    }

    pub async fn handle(
        &mut self,
        command: &RedisStoreCommand,
        write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        match command {
            RedisStoreCommand::Get { key } => {
                let value = match self.items.get(key) {
                    Some(StoreValue {
                        expiration: Some(expiration),
                        ..
                    }) if *expiration <= SystemTime::now() => {
                        self.items.remove(key);
                        encoding::null_bulk_string()
                    }
                    Some(StoreValue { value, .. }) => encoding::bulk_string(value),
                    _ => encoding::null_bulk_string(),
                };

                write_stream.write(value).await?;
                Ok(())
            }
            RedisStoreCommand::Set { key, value, px } => {
                self.items.insert(
                    key.clone(),
                    StoreValue {
                        value: value.clone(),
                        expiration: px.as_ref().copied(),
                    },
                );

                write_stream.write(Bytes::from_static(b"+OK\r\n")).await?;
                Ok(())
            }
            RedisStoreCommand::Keys { key } => {
                if &**key == b"*" {
                    let keys = self.items.keys().map(encoding::bulk_string).collect();
                    write_stream.write(encoding::array(keys)).await?;
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(
                        "[redis - error] unknown key pattern found for command 'KEYS'"
                    ))
                }
            }
            RedisStoreCommand::Type { key } => {
                let value = match self.items.get(key) {
                    Some(_) => encoding::simple_string(b"string"),
                    None => encoding::simple_string(b"none"),
                };

                write_stream.write(value).await?;
                Ok(())
            }
        }
    }

    pub fn merge(&mut self, other: RedisStore) {
        for (key, value) in other.items {
            self.items.insert(key, value);
        }
    }
}
