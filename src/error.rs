use bytes::Bytes;
use resp3::{encoding, error::RESPError, RESPValue};
use thiserror::Error;

/// Renders `bytes` as a best-effort UTF-8 string, resorting to
/// using `U+FFFD REPLACEMENT CHARACTER`` for any invalid characters.
fn render_bytes(bytes: &[u8]) -> impl std::fmt::Display + use<'_> {
    String::from_utf8_lossy(bytes)
}

pub type RedisResult<T> = Result<T, RedisError>;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("{0}")]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Resp(#[from] RESPError),

    #[error("key `{}` does not exist", render_bytes(key))]
    KeyNotFound { key: Bytes },
    #[error("command `{}` not supported", command)]
    UnsupportedCommand { command: String },

    #[error("an unknown error occurred")]
    Unknown,
}

impl From<RedisError> for RESPValue {
    fn from(error: RedisError) -> Self {
        encoding::simple_error(format!("ERR {error}"))
    }
}
