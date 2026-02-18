use thiserror::Error;

pub type RedisResult<T> = Result<T, RedisError>;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("[redis - io] {0}")]
    IO(#[from] std::io::Error),
}
