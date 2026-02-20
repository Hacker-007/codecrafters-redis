use bytes::Bytes;
use thiserror::Error;

pub type RedisResult<T> = Result<T, RedisError>;

/// Renders `bytes` as a best-effort UTF-8 string, resorting to
/// using `U+FFFD REPLACEMENT CHARACTER`` for any invalid characters.
fn render_bytes(bytes: &[u8]) -> impl std::fmt::Display + use<'_> {
    String::from_utf8_lossy(bytes)
}

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("[io] {0}")]
    IO(#[from] std::io::Error),
    #[error("[decode] {0}")]
    Decode(#[from] DecodeError),
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("found RESP value larger than {limit} bytes")]
    TooLarge { limit: usize },
    #[error("depth of RESP value larger than {limit}")]
    TooDeep { limit: usize },
    #[error("found unknown RESP data tag `{tag}`")]
    UnknownTag { tag: u8 },
    #[error("found unknown command `{}`", render_bytes(&command))]
    UnknownCommand { command: Bytes },
    #[error("found unknown command option `{}`", render_bytes(&option))]
    UnknownCommandOption { option: Bytes },
    #[error("unable to parse `{}` as 64-bit signed integer", render_bytes(bytes))]
    BadInteger { bytes: Vec<u8> },
    #[error("expected length `{length}` to be in the range [{min}, {max})")]
    InvalidLength { length: i64, min: i64, max: i64 },
    #[error("expected start of an array (i.e. `*`), but instead got `{byte}`")]
    ExpectedArray { byte: u8 },
    #[error("expected start of an bulk string (i.e. `$`), but instead got `{byte}`")]
    ExpectedBulkString { byte: u8 },
    #[error("expected end of command but found more arguments")]
    ExpectedCommandEnd,
    #[error("found incorrect number of arguments for command")]
    WrongArity,
    #[error("found a flag set more than one time")]
    DuplicateFlag,
}
