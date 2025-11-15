
use crate::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use super::{Clock, File, IO};

/// 
pub struct MemoryIO {
    files: Arc<Mutex<HashMap<String, Arc<MemoryFile>>>>,
}

impl MemoryIO {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Clock for MemoryIO {

}

impl IO for MemoryIO {
    /// files가 path를 포함하고 있지 않으면 MemoryFile을 새로 생성한 후 files에 넣는다.
    fn open_file(&self, path: &str, _direct: bool) -> Result<Arc<dyn File>> {
        let mut files = self.files.lock().unwrap();
        if !files.contains_key(path) {
            files.insert(
                path.to_string(),
                Arc::new(MemoryFile {
                    path: path.to_string(),
                })
            );
        }

        Ok(files.get(path).unwrap().clone())
    }
}

pub struct MemoryFile {
    path: String,
}

impl File for MemoryFile {

}
