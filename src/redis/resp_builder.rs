use super::resp::RESPValue;

#[derive(Debug)]
pub struct RESPBuilder;

impl RESPBuilder {
    pub fn array() -> RESPArrayBuilder {
        RESPArrayBuilder { values: vec![] }
    }
}

#[derive(Debug)]
pub struct RESPArrayBuilder {
    values: Vec<RESPValue>,
}

impl RESPArrayBuilder {
    pub fn bulk(mut self, value: impl ToString) -> Self {
        self.values.push(RESPValue::BulkString(value.to_string()));
        self
    }

    pub fn build(self) -> RESPValue {
        RESPValue::Array(self.values.into_iter().collect())
    }
}
