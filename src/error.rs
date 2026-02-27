use resp3::error::RESPError;
use thiserror::Error;

pub type RedisResult<T> = Result<T, RedisError>;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("{0}")]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Resp(#[from] RESPError),

    #[error("an unknown error occurred")]
    Unknown,
}
