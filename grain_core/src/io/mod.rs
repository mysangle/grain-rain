
pub mod clock;
mod completions;
mod memory;

pub use clock::Clock;
pub use completions::*;
use crate::{
    storage::{
        buffer_pool::ArenaBuffer,
        sqlite3_ondisk::WAL_FRAME_HEADER_SIZE,
    },
    BufferPool, Result};
pub use memory::MemoryIO;
use std::{
    cell::RefCell,
    fmt::{self, Debug},
    ptr::NonNull,
    pin::Pin,
    sync::Arc,
};

pub trait File: Send + Sync {
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion>;
    
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion>;
    
    fn size(&self) -> Result<u64>;
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, direct: bool) -> Result<Arc<dyn File>>;

    fn register_fixed_buffer(&self, _ptr: NonNull<u8>, _len: usize) -> Result<u32> {
        Err(crate::GrainError::InternalError(
            "unsupported operation".to_string(),
        ))
    }

    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        // 기본 구현은 busy loop
        while !c.finished() {
            self.step()?
        }
        if let Some(inner) = &c.inner {
            if let Some(Some(err)) = inner.result.get().copied() {
                return Err(err.into());
            }
        }
        Ok(())
    }

    fn step(&self) -> Result<()> {
        Ok(())
    }
}

pub type BufferData = Pin<Box<[u8]>>;

pub enum Buffer {
    Heap(BufferData),    // temporary buffer
    Pooled(ArenaBuffer), // arena buffer
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pooled(p) => write!(f, "Pooled(len={})", p.logical_len()),
            Self::Heap(buf) => write!(f, "{buf:?}: {}", buf.len()),
        }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let len = self.len();
        if let Self::Heap(buf) = self {
            TEMP_BUFFER_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                // take ownership of the buffer by swapping it with a dummy
                let buffer = std::mem::replace(buf, Pin::new(vec![].into_boxed_slice()));
                cache.return_buffer(buffer, len);
            });
        }
    }
}

impl Buffer {
    pub fn new_pooled(buf: ArenaBuffer) -> Self {
        Self::Pooled(buf)
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Heap(buf) => buf.len(),
            Self::Pooled(buf) => buf.logical_len(),
        }
    }
    
    pub fn new_temporary(size: usize) -> Self {
        TEMP_BUFFER_CACHE.with(|cache| {
            if let Some(buffer) = cache.borrow_mut().get_buffer(size) {
                // 기존에 저장되어 있는 버퍼가 있으면 재사용
                Self::Heap(buffer)
            } else {
                // 새로 할당
                Self::Heap(Pin::new(vec![0; size].into_boxed_slice()))
            }
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Heap(buf) => {
                // SAFETY: The buffer is guaranteed to be valid for the lifetime of the slice
                unsafe { std::slice::from_raw_parts(buf.as_ptr(), buf.len()) }
            }
            Self::Pooled(buf) => buf,
        }
    }

    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }

    #[inline]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        match self {
            Self::Heap(buf) => buf.as_ptr() as *mut u8,
            Self::Pooled(buf) => buf.as_ptr() as *mut u8,
        }
    }
}

thread_local! {
    /// thread local cache to re-use temporary buffers to prevent churn when pool overflows
    pub static TEMP_BUFFER_CACHE: RefCell<TempBufferCache> = RefCell::new(TempBufferCache::new());
}

pub(crate) struct TempBufferCache {
    page_size: usize,
    page_buffers: Vec<BufferData>,
    wal_frame_buffers: Vec<BufferData>,
    // TEMP_BUFFER_CACHE에 저장되는 BufferData의 수 제한. page_buffers와 wal_frame_buffers 각각 제한
    max_cached: usize,
}

impl TempBufferCache {
    const DEFAULT_MAX_CACHE_SIZE: usize = 256;

    fn new() -> Self {
        Self {
            page_size: BufferPool::DEFAULT_PAGE_SIZE,
            page_buffers: Vec::with_capacity(8),
            wal_frame_buffers: Vec::with_capacity(8),
            max_cached: Self::DEFAULT_MAX_CACHE_SIZE,
        }
    }

    /// 기존 캐시 모두 제거
    pub fn reinit_cache(&mut self, page_size: usize) {
        self.page_buffers.clear();
        self.wal_frame_buffers.clear();
        self.page_size = page_size;
    }

    fn get_buffer(&mut self, size: usize) -> Option<BufferData> {
        match size {
            sz if sz == self.page_size => self.page_buffers.pop(),
            sz if sz == (self.page_size + WAL_FRAME_HEADER_SIZE) => self.wal_frame_buffers.pop(),
            _ => None,
        }
    }

    fn return_buffer(&mut self, buff: BufferData, len: usize) {
        let sz = self.page_size;
        let cache = match len {
            n if n.eq(&sz) => &mut self.page_buffers,
            n if n.eq(&(sz + WAL_FRAME_HEADER_SIZE)) => &mut self.wal_frame_buffers,
            _ => return,
        };
        if self.max_cached > cache.len() {
            cache.push(buff);
        }
    }
}
