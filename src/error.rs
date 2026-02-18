use thiserror::Error;

pub type RedisResult<T> = Result<T, RedisError>;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("[io] {0}")]
    IO(#[from] std::io::Error),
    #[error("[io - decode] {0}")]
    Decode(#[from] DecodeError),
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("found RESP value larger than {limit} bytes")]
    TooLarge { limit: usize },
    #[error("found unknown RESP data tag `{tag}`")]
    UnknownTag { tag: u8 },
    #[error("unable to parse bytes `` as 64-bit signed integer")]
    BadInteger { bytes: Vec<u8> },
    #[error("expected `{length}` to be in the range [{min}, {max})")]
    InvalidLength { length: i64, min: i64, max: i64 },
}
