use std::{
    path::Path,
    time::{Duration, SystemTime},
};

use bytes::{Buf, BytesMut};

use crate::redis::resp::command::RedisStoreCommand;

use super::{resp::RESPValue, store::RedisStore};

pub struct RDBConfig {
    pub dir: String,
    pub file_name: String,
}

impl RDBConfig {
    pub fn new(dir: String, file_name: String) -> Self {
        Self { dir, file_name }
    }
}

pub struct RDBPesistence {
    pub config: RDBConfig,
}

impl RDBPesistence {
    pub fn new(config: RDBConfig) -> Self {
        Self { config }
    }

    pub async fn setup(&mut self) -> anyhow::Result<RedisStore> {
        let mut store = RedisStore::new();
        let path = Path::new(&self.config.dir).join(&self.config.file_name);
        if !path.try_exists()? {
            return Ok(store);
        }

        let mut buf = BytesMut::new();
        let rdb_file = std::fs::read(path)?;
        buf.extend_from_slice(&rdb_file);
        let _ = self.parse_magic_header(&mut buf)?;
        loop {
            let op_code = buf.get_u8();
            match op_code {
                0xFA => self.parse_aux_fields(&mut buf),
                0xFB => self.parse_resize_db(&mut buf),
                0xFC => self.parse_expiry_milliseconds(&mut store, &mut buf)?,
                0xFD => self.parse_expiry_seconds(&mut store, &mut buf)?,
                0xFE => self.parse_database_selector(&mut buf)?,
                0xFF => break,
                value_encoding => self.parse_value(value_encoding, None, &mut store, &mut buf)?,
            }
        }

        Ok(store)
    }

    fn parse_magic_header(&mut self, buf: &mut BytesMut) -> anyhow::Result<usize> {
        anyhow::ensure!(
            &buf[..5] == b"REDIS",
            "[redis - error] expected magic string 'REDIS' at beginning of RDB file"
        );

        buf.advance(5);
        let version = std::str::from_utf8(&buf[..4])?.parse::<usize>()?;
        buf.advance(4);
        Ok(version)
    }

    fn parse_aux_fields(&mut self, buf: &mut BytesMut) {
        let _ = self.parse_string(buf);
        let _ = self.parse_string(buf);
    }

    fn parse_resize_db(&mut self, buf: &mut BytesMut) {
        let _ = self.parse_length(buf);
        let _ = self.parse_length(buf);
    }

    fn parse_expiry_milliseconds(
        &mut self,
        store: &mut RedisStore,
        buf: &mut BytesMut,
    ) -> anyhow::Result<()> {
        let expiry_timestamp = buf.get_u64_le();
        let expiry_timestamp = SystemTime::UNIX_EPOCH + Duration::from_millis(expiry_timestamp);
        let _ = self.parse_value(buf.get_u8(), Some(expiry_timestamp), store, buf)?;
        Ok(())
    }

    fn parse_expiry_seconds(
        &mut self,
        store: &mut RedisStore,
        buf: &mut BytesMut,
    ) -> anyhow::Result<()> {
        let expiry_timestamp = buf.get_u32_le() as u64;
        let expiry_timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(expiry_timestamp);
        let _ = self.parse_value(buf.get_u8(), Some(expiry_timestamp), store, buf)?;
        Ok(())
    }

    fn parse_database_selector(&mut self, buf: &mut BytesMut) -> anyhow::Result<()> {
        let (_, is_encoded) = self.parse_length(buf);
        anyhow::ensure!(
            !is_encoded,
            "[redis - error] expected database selector to not be an specially-encoded string"
        );

        Ok(())
    }

    fn parse_value(
        &mut self,
        value_encoding: u8,
        px: Option<SystemTime>,
        store: &mut RedisStore,
        buf: &mut BytesMut,
    ) -> anyhow::Result<()> {
        let key = self
            .parse_string(buf)
            .into_bulk_string()
            .ok_or_else(|| anyhow::anyhow!("[redis - error] RDB key must be a bulk string"))?;

        let value = match value_encoding {
            0 => self.parse_string(buf),
            encoding => todo!("[redis - todo] implement encoding for value type '{encoding}'"),
        };

        let value = value.into_bulk_string().ok_or_else(|| {
            anyhow::anyhow!("[redis - error] only bulk strings are supported for RDB values")
        })?;

        store.handle(
            &RedisStoreCommand::Set { key, value, px },
            &mut std::io::sink(),
        )?;

        Ok(())
    }

    fn parse_string(&mut self, buf: &mut BytesMut) -> RESPValue {
        let (length, is_encoded) = self.parse_length(buf);
        if is_encoded {
            match length {
                0 => RESPValue::Integer(buf.get_u8() as i64),
                1 => RESPValue::Integer(buf.get_u16() as i64),
                2 => RESPValue::Integer(buf.get_u32() as i64),
                3 => todo!("[redis - todo] implement LZF compressed string"),
                _ => unreachable!(),
            }
        } else {
            RESPValue::BulkString(buf.copy_to_bytes(length))
        }
    }

    fn parse_length(&mut self, buf: &mut BytesMut) -> (usize, bool) {
        let length_encoding = (buf[0] & 0b11000000) >> 6;
        match length_encoding {
            0b00 => {
                let length = buf.get_u8() & 0b00111111;
                (length as usize, false)
            }
            0b01 => {
                let length = (buf.get_u8() & 0b00111111) as usize;
                let length = length << 8;
                let length = length | (buf.get_u8() as usize);
                (length as usize, false)
            }
            0b10 => {
                buf.advance(1);
                (buf.get_u32() as usize, false)
            }
            0b11 => {
                let length = buf.get_u8() & 0b00111111;
                (length as usize, true)
            }
            _ => unreachable!(),
        }
    }
}
