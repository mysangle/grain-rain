
use crate::{
    error::GrainError,
    io::Completion,
    storage::checksum::ChecksumContext,
    Buffer, CompletionError, Result,
};
use std::sync::Arc;

pub trait DatabaseStorage: Send + Sync {
    fn read_page(&self, page_idx: usize, io_ctx: &IOContext, c: Completion) -> Result<Completion>;

    fn write_page(&self, page_idx: usize, buffer: Arc<Buffer>, io_ctx: &IOContext, c: Completion,) -> Result<Completion>;

    fn size(&self) -> Result<u64>;
}

#[derive(Clone)]
pub struct DatabaseFile {
    file: Arc<dyn crate::io::File>,
}

impl DatabaseFile {
    pub fn new(file: Arc<dyn crate::io::File>) -> Self {
        Self { file }
    }
}

impl DatabaseStorage for DatabaseFile {
    fn read_page(&self, page_idx: usize, io_ctx: &IOContext, c: Completion) -> Result<Completion> {
        assert!(page_idx as i64 >= 0, "page should be positive");
        let r = c.as_read();
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(GrainError::NotADB);
        }
        let Some(pos) = (page_idx as u64 - 1).checked_mul(size as u64) else {
            return Err(GrainError::IntegerOverflow);
        };

        match &io_ctx.encryption_or_checksum {
            EncryptionOrChecksum::Checksum(ctx) => {
                let checksum_ctx = ctx.clone();
                let read_buffer = r.buf_arc();
                let original_c = c.clone();

                let verify_complete =
                    Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                        let Ok((buf, bytes_read)) = res else {
                            return;
                        };
                        if bytes_read <= 0 {
                            tracing::trace!("Read page {page_idx} with {} bytes", bytes_read);
                            original_c.complete(bytes_read);
                            return;
                        }
                        match checksum_ctx.verify_checksum(buf.as_mut_slice(), page_idx) {
                            Ok(_) => {
                                original_c.complete(bytes_read);
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to verify checksum for page_id={page_idx}: {e}"
                                );
                                assert!(
                                    !original_c.failed(),
                                    "Original completion already has an error"
                                );
                                original_c.error(e);
                            }
                        }
                    });

                let wrapped_completion = Completion::new_read(read_buffer, verify_complete);
                self.file.pread(pos, wrapped_completion)
            }
            EncryptionOrChecksum::None => self.file.pread(pos, c),
        }
    }

    fn write_page(&self, page_idx: usize, buffer: Arc<Buffer>, io_ctx: &IOContext, c: Completion,) -> Result<Completion> {
        let buffer_size = buffer.len();
        assert!(page_idx > 0);
        assert!(buffer_size >= 512);
        assert!(buffer_size <= 65536);
        assert_eq!(buffer_size & (buffer_size - 1), 0);

        // pos: 파일 위치
        // pos값이 overflow가 나는지 확인
        let Some(pos) = (page_idx as u64 - 1).checked_mul(buffer_size as u64) else {
            return Err(GrainError::IntegerOverflow);
        };
        let buffer = match &io_ctx.encryption_or_checksum {
            // buffer에 체크섬 추가
            EncryptionOrChecksum::Checksum(ctx) => checksum_buffer(page_idx, buffer, ctx),
            EncryptionOrChecksum::None => buffer,
        };
        self.file.pwrite(pos, buffer, c)
    }

    fn size(&self) -> Result<u64> {
        self.file.size()
    }
}

#[derive(Clone)]
pub enum EncryptionOrChecksum {
    //Encryption(EncryptionContext),
    Checksum(ChecksumContext),
    None,
}

#[derive(Clone)]
pub struct IOContext {
    encryption_or_checksum: EncryptionOrChecksum,
}

impl IOContext {
    pub fn get_reserved_space_bytes(&self) -> u8 {
        match &self.encryption_or_checksum {
            EncryptionOrChecksum::Checksum(ctx) => ctx.required_reserved_bytes(),
            EncryptionOrChecksum::None => Default::default(),
        }
    }

    pub fn encryption_or_checksum(&self) -> &EncryptionOrChecksum {
        &self.encryption_or_checksum
    }
}

impl Default for IOContext {
    fn default() -> Self {
        let encryption_or_checksum = EncryptionOrChecksum::Checksum(ChecksumContext::default());
        Self {
            encryption_or_checksum,
        }
    }
}

fn checksum_buffer(page_idx: usize, buffer: Arc<Buffer>, ctx: &ChecksumContext) -> Arc<Buffer> {
    ctx.add_checksum_to_page(buffer.as_mut_slice(), page_idx)
        .unwrap();
    buffer
}
