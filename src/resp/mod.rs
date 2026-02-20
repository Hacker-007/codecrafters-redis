use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{DecodeError, RedisError, RedisResult};

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

#[derive(Debug, PartialEq, Eq)]
pub enum SetCondition {
    Nx,
    Xx,
    IfEq(Bytes),
    IfNe(Bytes),
    IfDeq(Bytes),
    IfDne(Bytes),
}

#[derive(Debug, PartialEq, Eq)]
pub enum SetExpiration {
    Ex(i64),
    Px(i64),
    ExAt(i64),
    PxAt(i64),
    KeepTtl,
}
