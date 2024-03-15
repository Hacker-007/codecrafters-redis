use crate::redis::value::RedisValue;

use super::InfoSection;

pub fn process(section: InfoSection) -> anyhow::Result<RedisValue> {
    if section == InfoSection::Replication {
        return Ok(RedisValue::BulkString("role:master".to_string()));
    }

    Err(anyhow::anyhow!(
        "[redis - error] command 'info' currently only supports section 'replication'"
    ))
}
