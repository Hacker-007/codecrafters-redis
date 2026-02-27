use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::{encoding::CommandPartEncoding, error::DecodeError};

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

impl std::fmt::Display for RedisCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            RedisCommand::Connection(ConnectionCommand::Ping) => "PING",
            RedisCommand::String(StringCommand::Get { .. }) => "GET",
            RedisCommand::String(StringCommand::Set { .. }) => "SET",
        };

        write!(f, "{name}")
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetExpiration {
    Ex(i64),
    Px(i64),
    ExAt(i64),
    PxAt(i64),
    KeepTtl,
}

impl SetExpiration {
    /// Resolves this expiration as an [`SystemTime`] using
    /// the "now" time as the reference point.
    /// 
    /// Since [`SetExpiration::KeepTtl`] has no logical resolution
    /// time, `None` is returned.
    pub fn resolve(self) -> Option<SystemTime> {
        let now = SystemTime::now();
        match self {
            SetExpiration::Ex(secs) => Some(now + Duration::from_secs(secs as u64)),
            SetExpiration::Px(millis) => Some(now + Duration::from_millis(millis as u64)),
            SetExpiration::ExAt(ts) => Some(UNIX_EPOCH + Duration::from_secs(ts as u64)),
            SetExpiration::PxAt(ts) => Some(UNIX_EPOCH + Duration::from_millis(ts as u64)),
            SetExpiration::KeepTtl => return None,
        }
    }
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
