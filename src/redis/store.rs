use std::{collections::HashMap, io::Write, time::SystemTime};

use bytes::Bytes;

use super::resp::{command::RedisStoreCommand, RESPValue};

type StoreKey = Bytes;

#[derive(Debug)]
pub struct StoreValue {
    value: Bytes,
    expiration: Option<SystemTime>,
}

#[derive(Debug)]
pub struct RedisStore {
    values: HashMap<StoreKey, StoreValue>,
}

impl RedisStore {
    pub fn new() -> Self {
        Self {
            values: HashMap::default(),
        }
    }

    pub fn handle(
        &mut self,
        command: &RedisStoreCommand,
        output_writer: &mut impl Write,
    ) -> anyhow::Result<()> {
        match command {
            RedisStoreCommand::Get { key } => {
                let value = match self.values.get(key) {
                    Some(StoreValue {
                        expiration: Some(expiration),
                        ..
                    }) if *expiration <= SystemTime::now() => {
                        self.values.remove(key);
                        RESPValue::NullBulkString
                    }
                    Some(StoreValue { value, .. }) => RESPValue::BulkString(value.clone()),
                    _ => RESPValue::NullBulkString,
                };

                let bytes = Bytes::from(value);
                output_writer.write_all(&bytes)?;
                Ok(())
            }
            RedisStoreCommand::Set { key, value, px } => {
                self.values.insert(
                    key.clone(),
                    StoreValue {
                        value: value.clone(),
                        expiration: px.as_ref().copied(),
                    },
                );

                output_writer.write_all(b"+OK\r\n")?;
                Ok(())
            }
        }
    }
}
