
use crate::{
    error::GrainError,
    io::Completion,
    storage::checksum::ChecksumContext,
    Buffer, Result,
};
use std::sync::Arc;

pub trait DatabaseStorage: Send + Sync {
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
