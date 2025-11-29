
use crate::Result;

const CHECKSUM_PAGE_SIZE: usize = 4096;
const CHECKSUM_SIZE: usize = 8;
pub(crate) const CHECKSUM_REQUIRED_RESERVED_BYTES: u8 = CHECKSUM_SIZE as u8;

#[derive(Clone)]
pub struct ChecksumContext {}

/// CHECKSUM_PAGE_SIZE(4096)가 아닌 페이지에 대해서는 체크섬을 계산/검증하지 않는다.
impl ChecksumContext {
    pub fn new() -> Self {
        ChecksumContext {}
    }

    /// 페이지 크기가 CHECKSUM_PAGE_SIZE 일때만 계산
    pub fn add_checksum_to_page(&self, page: &mut [u8], _page_id: usize) -> Result<()> {
        if page.len() != CHECKSUM_PAGE_SIZE {
            return Ok(());
        }

        // compute checksum on the actual page data (excluding the reserved checksum area)
        let actual_page = &page[..CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE];
        let checksum = self.compute_checksum(actual_page);

        let checksum_bytes = checksum.to_le_bytes();
        assert_eq!(checksum_bytes.len(), CHECKSUM_SIZE);
        page[CHECKSUM_PAGE_SIZE - CHECKSUM_SIZE..].copy_from_slice(&checksum_bytes);
        Ok(())
    }

    fn compute_checksum(&self, data: &[u8]) -> u64 {
        twox_hash::XxHash3_64::oneshot(data)
    }
    
    pub fn required_reserved_bytes(&self) -> u8 {
        CHECKSUM_REQUIRED_RESERVED_BYTES
    }
}

impl Default for ChecksumContext {
    fn default() -> Self {
        Self::new()
    }
}
