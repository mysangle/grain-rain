
use std::{
    collections::HashMap,
    sync::Arc,
};

const SCHEMA_TABLE_NAME: &str = "sqlite_schema";

#[derive(Debug)]
pub struct Schema {
    pub tables: HashMap<String, Arc<Table>>,
    pub schema_version: u32,
}

impl Schema {
    pub fn new() -> Self {
        let tables = HashMap::new();
        Self {
            tables,
            schema_version: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Table {

}

impl Table {

}
