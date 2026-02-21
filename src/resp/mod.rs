use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    error::{DecodeError, RedisError, RedisResult},
    resp::encoding::CommandPartEncoding,
};

pub mod codec;
pub mod encoding;
mod parse;

#[derive(Debug, PartialEq, Eq)]
pub enum RESPValue {
    SimpleString(Bytes),
    SimpleError(Bytes),
    Integer(i64),
    NullBulkString,
    BulkString(Bytes),
    NullArray,
    Array(Vec<RESPValue>),
}

#[derive(Debug)]
pub enum ClientMessage {
    Command(RedisCommand),
    Error(DecodeError),
}

#[derive(Debug, PartialEq, Eq)]
pub enum RedisCommand {
    Get {
        key: Bytes,
    },
    Set {
        key: Bytes,
        value: Bytes,
        condition: Option<SetCondition>,
        get: bool,
        expiration: Option<SetExpiration>,
    },
}

impl CommandPartEncoding for RedisCommand {
    fn encode(self, dest: &mut Vec<Bytes>) {
        match self {
            RedisCommand::Get { key } => ("GET", key).encode(dest),
            RedisCommand::Set {
                key,
                value,
                condition,
                get,
                expiration,
            } => {
                ("SET", key, value, condition).encode(dest);
                get.then(|| "GET".encode(dest));
                expiration.encode(dest);
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SetCondition {
    Nx,
    Xx,
    IfEq(Bytes),
    IfNe(Bytes),
    IfDeq(Bytes),
    IfDne(Bytes),
}

impl CommandPartEncoding for SetCondition {
    fn encode(self, dest: &mut Vec<Bytes>) {
        match self {
            SetCondition::Nx => "NX".encode(dest),
            SetCondition::Xx => "XX".encode(dest),
            SetCondition::IfEq(bytes) => ("IFEQ", bytes).encode(dest),
            SetCondition::IfNe(bytes) => ("IFNE", bytes).encode(dest),
            SetCondition::IfDeq(bytes) => ("IFDEQ", bytes).encode(dest),
            SetCondition::IfDne(bytes) => ("IFDNE", bytes).encode(dest),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SetExpiration {
    Ex(i64),
    Px(i64),
    ExAt(i64),
    PxAt(i64),
    KeepTtl,
}

impl CommandPartEncoding for SetExpiration {
    fn encode(self, dest: &mut Vec<Bytes>) {
        match self {
            SetExpiration::Ex(seconds) => ("EX", seconds).encode(dest),
            SetExpiration::Px(milliseconds) => ("PX", milliseconds).encode(dest),
            SetExpiration::ExAt(timestamp) => ("EXAT", timestamp).encode(dest),
            SetExpiration::PxAt(timestamp) => ("PXAT", timestamp).encode(dest),
            SetExpiration::KeepTtl => "KEEPTTL".encode(dest),
        }
    }
}
