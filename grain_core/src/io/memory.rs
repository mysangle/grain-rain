
use crate::Result;
use std::{
    cell::{Cell, UnsafeCell},
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};
use super::{Buffer, Completion, Clock, File, IO};

const PAGE_SIZE: usize = 4096;
type MemPage = Box<[u8; PAGE_SIZE]>;

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
                    pages: BTreeMap::new().into(),
                    size: 0.into(),
                })
            );
        }

        Ok(files.get(path).unwrap().clone())
    }
}

/// 여러 쓰레드에서 동시에 불변 참조를 가지는 상태에서 내부 값을 변경 할 수 있도록 하기 위해
/// Cell과 UnsafeCell을 사용
/// MemoryFile 자체는 쓰레드 안전하게 동작하지 않으므로 보다 상위 계층(Pager)에서 쓰레드 안전하게 사용해 주어야 함
/// 
/// 내부에서는 데이터베이스의 페이지 크기와 상관없이 항상 PAGE_SIZE(4096) 단위로 동작하도록 되어 있다.
/// NOTE: pos를 기준으로 그 이후의 영역에 내용을 쓰기 때문에 데이터베이스가 사용하는 페이지 크기와 달라도 문제는 없다.
pub struct MemoryFile {
    path: String,
    pages: UnsafeCell<BTreeMap<usize, MemPage>>,
    size: Cell<u64>,
}

/// Cell이 Sync가 아니므로 선언 필요
unsafe impl Sync for MemoryFile {}

impl File for MemoryFile {
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        tracing::debug!(
            "pwrite(path={}): pos={}, size={}",
            self.path,
            pos,
            buffer.len()
        );
        let buf_len = buffer.len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(c);
        }

        let mut offset = pos as usize;
        let mut remaining = buf_len;
        let mut buf_offset = 0;
        let data = &buffer.as_slice();

        while remaining > 0 {
            let page_no = offset / PAGE_SIZE;
            let page_offset = offset % PAGE_SIZE;
            let bytes_to_write = remaining.min(PAGE_SIZE - page_offset);

            {
                let page = self.get_or_allocate_page(page_no as u64);
                page[page_offset..page_offset + bytes_to_write]
                    .copy_from_slice(&data[buf_offset..buf_offset + bytes_to_write]);
            }

            offset += bytes_to_write;
            buf_offset += bytes_to_write;
            remaining -= bytes_to_write;
        }

        self.size
            .set(core::cmp::max(pos + buf_len as u64, self.size.get()));

        c.complete(buf_len as i32);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        tracing::debug!("size(path={}): {}", self.path, self.size.get());
        Ok(self.size.get())
    }
}

impl MemoryFile {
    /// page_no에 해당하는 페이지가 있으면 그 페이지를 리턴, 없으면 새로 생성 후 리턴
    fn get_or_allocate_page(&self, page_no: u64) -> &mut MemPage {
        unsafe {
            let pages = &mut *self.pages.get();
            pages
                .entry(page_no as usize)
                .or_insert_with(|| Box::new([0; PAGE_SIZE]))
        }
    }
}
