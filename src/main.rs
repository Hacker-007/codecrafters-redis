use crate::error::RedisResult;

mod error;

#[tokio::main]
async fn main() -> RedisResult<()> {
    Ok(())
}
