
mod error;
pub mod io;
pub mod storage;
mod util;

pub use error::GrainError;
pub use io::{MemoryIO, IO};
use std::sync::Arc;
use storage::database::{DatabaseFile, DatabaseStorage};

pub type Result<T, E = GrainError> = std::result::Result<T, E>;

/// 여러 connection 사이에 공유되는 Database
/// 따라서, Send와 Sync이어야 한다.
pub struct Database {
    path: String,
    pub io: Arc<dyn IO>,
    db_file: Arc<dyn DatabaseStorage>,
}

/// Database 내부의 모든 필드가 Send, Sync가 아닐 수도 있지만
/// Database가 Send, Sync로 사용될 수 있다고 수동으로 컴파일러에게 알린다.
/// 따라서, 개발자가 수동으로 내부 구조를 쓰레드 안전하게 직접 관리해야 함
unsafe impl Send for Database {}
unsafe impl Sync for Database {}

/// IO: 데이터베이스 엔진과 실제 스토리지(파일 시스템, 메모리 등) 사이의 '어댑터' 역할
///   - MemoryIO의 open_file을 통해 실제 파일을 읽고 쓰는 MemoryFile을 얻는다.
/// DatabaseStorage: IO에 의해 생성된 File을 내부에 가지고 있다.
///   - File에 대해 페이지 단위 접근
impl Database {
    pub fn open_new(path: &str) -> Result<(Arc<dyn IO>, Arc<Database>)> {
        let io = Self::io_for_path(path)?;
        let db = Self::open_file(io.clone(), path)?;
        Ok((io, db))
    }

    /// 파일 path로부터 IO 생성
    pub fn io_for_path(path: &str) -> Result<Arc<dyn IO>> {
        use crate::util::MEMORY_PATH;
        
        let io = match path.trim() {
            MEMORY_PATH => Arc::new(MemoryIO::new()),
            _ => {
                tracing::error!("not supported yet! use ':memory:' only at the moment");
                std::process::exit(0);
            }
        };
        Ok(io)
    }

    fn open_file(io: Arc<dyn IO>, path: &str) -> Result<Arc<Database>> {
        let file = io.open_file(path, true)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        Self::open_with_internal(io, path, &format!("{path}-wal"), db_file)
    }

    fn open_with_internal(io: Arc<dyn IO>, path: &str, wal_path: &str, db_file: Arc<dyn DatabaseStorage>) -> Result<Arc<Database>> {
        let db = Arc::new(Database {
            path: path.to_string(),
            io,
            db_file,
        });
        Ok(db)
    }
}
