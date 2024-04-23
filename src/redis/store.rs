use std::{
    collections::{BTreeMap, HashMap},
    time::SystemTime,
};

use bytes::Bytes;

use super::{
    resp::{command::RedisStoreCommand, encoding},
    server::RedisWriteStream,
};

type StoreKey = Bytes;

#[derive(Debug)]
pub enum StoreValue {
    String {
        value: Bytes,
        expiration: Option<SystemTime>,
    },
    Stream {
        entries: BTreeMap<Bytes, Vec<(Bytes, Bytes)>>,
    },
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
                    Some(StoreValue::String {
                        expiration: Some(expiration),
                        ..
                    }) if *expiration <= SystemTime::now() => {
                        self.items.remove(key);
                        encoding::null_bulk_string()
                    }
                    Some(StoreValue::String { value, .. }) => encoding::bulk_string(value),
                    Some(StoreValue::Stream { .. }) => return Err(anyhow::anyhow!("[redis - error] attempted to get value from stream using `GET` instead of `XREAD`")),
                    _ => encoding::null_bulk_string(),
                };

                write_stream.write(value).await?;
                Ok(())
            }
            RedisStoreCommand::Set { key, value, px } => {
                self.items.insert(
                    key.clone(),
                    StoreValue::String {
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
                    Some(StoreValue::String { .. }) => encoding::simple_string(b"string"),
                    Some(StoreValue::Stream { .. }) => encoding::simple_string(b"stream"),
                    None => encoding::simple_string(b"none"),
                };

                write_stream.write(value).await?;
                Ok(())
            }
            RedisStoreCommand::XAdd {
                key,
                entry_id,
                fields,
            } => {
                let stream = self.items
                    .entry(key.clone())
                    .or_insert_with(|| StoreValue::Stream {
                        entries: BTreeMap::default(),
                    });

                if let StoreValue::Stream { entries } = stream {
                    entries.insert(entry_id.clone(), fields.clone());
                    write_stream.write(encoding::bulk_string(entry_id)).await
                } else {
                    Err(anyhow::anyhow!("[redis - error] expected key to reference stream"))
                }
            }
        }
    }

    pub fn merge(&mut self, other: RedisStore) {
        for (key, value) in other.items {
            self.items.insert(key, value);
        }
    }
}
