use crate::redis::{resp::encoding, server::RedisWriteStream};

use super::{command::{ConfigSection, RedisPersistenceCommand}, RDBPesistence};

impl RDBPesistence {
    pub async fn handle_command(
        &mut self,
        command: &RedisPersistenceCommand,
        write_stream: RedisWriteStream,
    ) -> anyhow::Result<()> {
        match command {
            RedisPersistenceCommand::Config { section } => self.config(section, write_stream).await?,
        }

        Ok(())
    }

    async fn config(&mut self, section: &ConfigSection, write_stream: RedisWriteStream) -> anyhow::Result<()> {
        match section {
            ConfigSection::Get { keys } => {
                let mut values = vec![];
                for key in keys {
                    values.push(encoding::bulk_string(key));
                    if &**key == b"dir" {
                        values.push(encoding::bulk_string(&self.config.dir));
                    } else if &**key == b"dbfilename" {
                        values.push(encoding::bulk_string(&self.config.file_name));
                    } else {
                        return Err(anyhow::anyhow!("[redis - error] unexpected configuration key found"))
                    }
                }
                
                write_stream.write(encoding::array(values)).await
            }
        }
    }
}
