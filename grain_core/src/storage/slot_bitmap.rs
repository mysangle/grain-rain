
/// 아레나에서 할당된 slot이 어디인지를 추적하는데 사용
/// 비트가 1이면 free, 비트가 0이면 allocated
/// 
/// 단일 슬롯 할당의 경우에는 scan_one_high에서 시작하여 앞쪽(낮은 인덱스)으로 검색
/// 연속 슬롯 할당의 경우에는 scan_run_low에서 시작하여 뒤쪽(높은 인덱스)으로 검색
/// 
/// SlotBitmap은 쓰레드 안전하지 않음. 사용하는 쪽에서 SpinLock같은 것을 통해 사용해야 함.
/// 
/// 비트 인덱스로 슬롯을 계산하므로
///   0번 슬롯이 비트 0에 대응하고, 63번 슬롯이 비트 63에 대응한다.
///   0을 앞, 63을 뒤로 표현
#[derive(Debug)]
pub(super) struct SlotBitmap {
    /// 비트가 1이면 free, 비트가 0이면 allocated
    words: Box<[u64]>,
    /// 아레나에 있는 전체 슬롯의 수
    n_slots: u32,
    // 마지막으로 할당된 슬롯 바로 아래 슬롯을 가리킨다.(1개의 슬롯만 할당하는 경우 사용)
    // 슬롯의 끝에서 앞으로 이동
    scan_one_high: u32,
    /// 마지막으로 할당된 슬롯의 끝의 다음 슬롯을 가리킨다.(멀티 슬롯을 할당하는 경우 사용)
    /// 슬롯의 처음에서 다음으로 이동
    scan_run_low: u32,
}

impl SlotBitmap {
    // 64비트는 2^6이므로 6으로 시프트를 한다.
    /// 64 bits per word, so shift by 6 to get slot index
    const WORD_SHIFT: u32 = 6;
    const WORD_BITS: u32 = 64;
    const WORD_MASK: u32 = 63;
    const ALL_FREE: u64 = u64::MAX;
    const ALL_ALLOCATED: u64 = 0u64;

    const ALLOC: bool = false;
    const FREE: bool = true;

    pub fn new(n_slots: u32) -> Self {
        assert!(
            n_slots % 64 == 0,
            "number of slots in map must be a multiple of 64"
        );
        let n_words = (n_slots / Self::WORD_BITS) as usize;
        // 크기가 변하지 않을 것이기 때문에 Box<[T]>를 사용
        let words = vec![Self::ALL_FREE; n_words].into_boxed_slice();

        Self {
            words,
            n_slots,
            scan_run_low: 0,
            scan_one_high: n_slots - 1,
        }
    }

    /// 상위 비트에서부터 찾기 시작해서 가장 먼저 만나는 free slot을 할당한다.
    /// 더이상 할다할 슬롯이 없으면 None을 리턴
    pub fn alloc_one(&mut self) -> Option<u32> {
        // 두번의 검색 시도
        //   첫번째: scan_one_high에서 부터 시작해서 0까지 확인. 보통은 scan_one_high에서 찾음
        //   두번째: 첫번째 시도에서 못찾으면 (n_slots - 1)에서부터 다시 시도
        for start in [self.scan_one_high, self.n_slots - 1] {
            let (mut word_idx, bit) = Self::slot_to_word_and_bit(start);
            let mut word = self.words[word_idx] & Self::mask_through(bit);
            if word != Self::ALL_ALLOCATED {
                // 빈 슬롯이 있음.
                let bit = 63 - word.leading_ones();
                // 지정한 bit를 0(allocated)으로 변경
                self.words[word_idx] &= !(1u64 << bit);
                let slot = Self::word_and_bit_to_slot(word_idx, bit);
                // slot 이상의 상위 슬롯은 모두 allocated이므로 다음 scan은 그보다 1 적은 값에서 시작하면 된다.
                self.scan_one_high = slot.saturating_sub(1);
                return Some(slot);
            }

            while word_idx > 0 {
                word_idx -= 1;
                word = self.words[word_idx];
                if word != Self::ALL_ALLOCATED {
                    let bits = 63 - word.leading_zeros();
                    self.words[word_idx] &= !(1u64 << bits);
                    let slot = Self::word_and_bit_to_slot(word_idx, bits);
                    self.scan_one_high = slot.saturating_sub(1);
                    return Some(slot);
                }
            }
            if self.scan_one_high == self.n_slots - 1 {
                return None;
            }
        }

        None
    }

    pub fn alloc_run(&mut self, need: u32) -> Option<u32> {
        if need == 0 || need > self.n_slots {
            return None;
        }
        if need == 1 {
            return self.alloc_one();
        }

        for &start_pos in &[self.scan_run_low, 0] {
            if let Some(found) = self.find_free_run_up(start_pos, need) {
                self.mark_run(found, need, Self::ALLOC);
                self.scan_run_low = found + need;
                
                let last_slot = found + need - 1;
                if last_slot > self.scan_one_high {
                    // 멀티 슬롯 할당이 scan_one_high를 넘어섰는지 확인
                    self.scan_one_high = last_slot.min(self.n_slots - 1);
                }
                return Some(found);
            }
            if start_pos == 0 {
                break;
            }
        }

        None
    }

    pub fn free_run(&mut self, start: u32, count: u32) {
        if count == 0 {
            return;
        }
        assert!(start + count <= self.n_slots, "free_run out of bounds");

        self.mark_run(start, count, Self::FREE);

        // scan_one_high 업데이트(단일 슬롯 할당 최적화)
        if count == 1 && start > self.scan_one_high {
            // start가 scan_one_high 보다 상위에 있으므로
            // scan_one_high를 start로 이동
            self.scan_one_high = start;
            return;
        }

        // scan_run_low 업데이트(연속 슬롯 할당 최적화)
        if start < self.scan_run_low {
            // start가 scan_run_low 보다 이전이면
            // scan_run_low를 start로 이동
            self.scan_run_low = start;
        }

        // scan_one_high 업데이트
        let last_slot = (start + count - 1).min(self.n_slots - 1);
        if last_slot > self.scan_one_high {
            // free가 된 마지막 슬롯이 scan_one_high보다 뒤에 있을 경우
            self.scan_one_high = last_slot;
        }
    }

    /// 특정 범위의 슬롯을 한번에 allocated나 free로 만든다.
    pub fn mark_run(&mut self, start: u32, len:u32, free: bool) {
        assert!(start + len <= self.n_slots, "mark_run out of bounds");

        let (mut word_idx, mut bit_offset) = Self::slot_to_word_and_bit(start);
        let mut remaining = len as usize;
        while remaining > 0 {
            let bits = (Self::WORD_BITS as usize) - bit_offset as usize;
            // 현재 워드에서 처리할 슬롯의 수
            let take = remaining.min(bits);
            let mask = if take == Self::WORD_BITS as usize {
                Self::ALL_FREE
            } else {
                ((1u64 << take) - 1) << bit_offset
            };
            if free {
                // 해당 마스크에 해당하는 비트를 1로 설정
                self.words[word_idx] |= mask;
            } else {
                // 마스트 영역의 비트를 0으로 설정
                self.words[word_idx] &= !mask;
            }
            // 다음 워드로 이동
            remaining -= take;
            word_idx += 1;
            bit_offset = 0;
        }
    }

    /// 모든 슬롯이 free이면 true를 리턴
    pub(super) fn check_run_free(&self, start: u32, len: u32) -> bool {
        if start + len > self.n_slots {
            return false;
        }
        let (mut word_idx, bit_offset) = Self::slot_to_word_and_bit(start);
        let mut remaining = len as usize;
        let mut pos_in_word = bit_offset as usize;

        while remaining > 0 {
            // 현재 워드에서 처리할 비트(슬롯)의 수
            let bits_to_process = remaining.min((Self::WORD_BITS as usize) - pos_in_word);

            // 현재 확인 할 비트 위치에만 1을 설정, 나머지는 0
            let mask = if bits_to_process == Self::WORD_BITS as usize {
                Self::ALL_FREE
            } else {
                ((1u64 << bits_to_process) - 1) << pos_in_word
            };
            // 확인하는 비트는 전부 1이어야 한다.
            if (self.words[word_idx] & mask) != mask {
                // 확인하는 비트 중 단 하나라도 0인 비트가 있으면 모든 슬롯이 free가 아니라는 의미
                return false;
            }
            // 다음 워드로 이동
            remaining -= bits_to_process;
            word_idx += 1;
            pos_in_word = 0;
        }

        true
    }

    /// alloc_run에 의해 호출되며, 비트맵에서 낮은 주소에서 높은 주소로 need 개수만큼의 연속된 빈 슬롯을 찾는다.
    fn find_free_run_up(&self, start: u32, need: u32) -> Option<u32> {
        let limit = self.n_slots.saturating_sub(need - 1);
        let mut pos = start;
        while pos > limit {
            let (word_idx, bit_offset) = Self::slot_to_word_and_bit(pos);

            let current_word = self.words[word_idx] & Self::mask_from(bit_offset);
            if current_word == 0 {
                // pos에서부터 워드의 끝까지 빈 슬롯이 없으므로 상위 워드로 이동
                pos = ((word_idx + 1) as u32) << Self::WORD_SHIFT;
                continue;
            }

            // pos으로부터 그 다음을 찾을 때 첫번째 빈 슬롯 위치
            let first_free_bit = current_word.trailing_zeros();
            let run_start = ((word_idx as u32) << Self::WORD_SHIFT) + first_free_bit;

            // first_free_bit 로부터 그 다음으로 연속적으로 할당이 가능한 슬롯 수 
            let free_in_cur = Self::run_len_from(self.words[word_idx], first_free_bit);
            if free_in_cur >= need {
                // 필요한 슬롯 수 이상으로 할당 가능
                return Some(run_start);
            }

            if first_free_bit + free_in_cur < Self::WORD_BITS {
                // 워드의 끝까지 가지 못하고 중간에 끝났으므로 다음 워드랑 이어서 계산 할 수 없다.
                pos = run_start + free_in_cur + 1;
                continue;
            }

            // 두 개의 워드를 이어서 연속적인 슬롯 할당이 가능한지 확인
            //   - 이전 워드에서는 free_in_cur 만큼의 슬롯 할당이 가능
            let mut slots_found = free_in_cur;
            let mut wi = word_idx + 1;
            while slots_found + Self::WORD_BITS <= need
                && wi < self.words.len()
                && self.words[wi] == Self::ALL_FREE
            {
                // 전체가 비어있는 워드인 경우
                slots_found += Self::WORD_BITS;
                wi += 1;
            }
            if slots_found >= need {
                return Some(run_start);
            }

            // 다음 워드의 시작부분에 빈 슬롯이 있는지 확인
            if wi < self.words.len() {
                let remaining = need - slots_found;
                let w = self.words[wi];
                // 연속된 빈 슬롯 수 계산
                let tail = (!w).trailing_zeros();
                if tail >= remaining {
                    return Some(run_start);
                }
                // 이번 run_start에서는 원하는 만큼의 연속적인 슬롯 할당 불가.
                // pos를 다음 위치로 이동
                pos = run_start + slots_found + 1;
                continue;
            }
            // 할당 불가
            return None;
        }
        None
    }

    #[inline]
    /// (워드 인덱스, 워드 내 비트 오프셋)을 리턴한다.
    const fn slot_to_word_and_bit(slot_idx: u32) -> (usize, u32) {
        (
            (slot_idx >> Self::WORD_SHIFT) as usize,
            slot_idx & Self::WORD_MASK,
        )
    }

    #[inline]
    const fn word_and_bit_to_slot(word_idx: usize, bit: u32) -> u32 {
        (word_idx as u32) << Self::WORD_SHIFT | bit
    }

    #[inline]
    /// 0비트에서부터 주어진 비트까지의 값을 1로 하는 값 생성
    /// 예: bit 값이 1 이면 0b11(3)이 된다.
    const fn mask_through(bit: u32) -> u64 {
        if bit == 63 {
            u64::MAX
        } else {
            (1u64 << (bit + 1)) - 1
        }
    }

    /// 0비트에서부터 주어진 비트까지의 값을 0으로 하는 값 생성
    #[inline]
    const fn mask_from(bit: u32) -> u64 {
        u64::MAX << bit
    }

    /// word의 bit지점에서부터 연속적으로 할당이 가능한 슬롯 수
    #[inline]
    const fn run_len_from(word: u64, bit: u32) -> u32 {
        (!(word >> bit)).trailing_zeros()
    }
}
