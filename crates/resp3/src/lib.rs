use crate::{encoding::CommandPartEncoding, error::DecodeError};
use bytes::Bytes;

pub mod codec;
pub mod encoding;
pub mod error;
mod parse;

/// A RESP3-compliant value.
///
/// See the [specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
/// for more details.
#[derive(Debug, PartialEq, Eq)]
pub enum RESPValue {
    SimpleString(Bytes),
    SimpleError(Bytes),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RESPValue>),
    Null,
}

#[derive(Debug)]
pub enum ClientMessage {
    Command(RedisCommand),
    Error(DecodeError),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConnectionCommand {
    Ping,
}

impl CommandPartEncoding for ConnectionCommand {
    fn encode(self, dest: &mut Vec<Bytes>) {
        match self {
            ConnectionCommand::Ping => "PING".encode(dest),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum StringCommand {
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

impl CommandPartEncoding for StringCommand {
    fn encode(self, dest: &mut Vec<Bytes>) {
        match self {
            StringCommand::Get { key } => ("GET", key).encode(dest),
            StringCommand::Set {
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
pub enum RedisCommand {
    Connection(ConnectionCommand),
    String(StringCommand),
}

impl CommandPartEncoding for RedisCommand {
    fn encode(self, dest: &mut Vec<Bytes>) {
        match self {
            RedisCommand::Connection(command) => command.encode(dest),
            RedisCommand::String(command) => command.encode(dest),
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
