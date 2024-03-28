use std::{collections::HashMap, time::SystemTime};

type StoreKey = String;

#[derive(Debug)]
pub struct StoreValue {
    pub value: String,
    pub expiration: Option<SystemTime>,
}

pub type Store = HashMap<StoreKey, StoreValue>;
