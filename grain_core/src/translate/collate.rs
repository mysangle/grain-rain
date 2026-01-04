
use std::cmp::Ordering;

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, strum_macros::Display, strum_macros::EnumString,
)]
#[strum(ascii_case_insensitive)]
/// **Pre defined collation sequences**\
/// Collating functions only matter when comparing string values.
/// Numeric values are always compared numerically, and BLOBs are always compared byte-by-byte using memcmp().
#[repr(u8)]
pub enum CollationSeq {
    Unset = 0,
    #[default]
    Binary = 1,
    NoCase = 2,
    Rtrim = 3,
}

impl CollationSeq {
    #[inline(always)]
    pub fn compare_strings(&self, lhs: &str, rhs: &str) -> Ordering {
        match self {
            CollationSeq::Unset | CollationSeq::Binary => Self::binary_cmp(lhs, rhs),
            CollationSeq::NoCase => Self::nocase_cmp(lhs, rhs),
            CollationSeq::Rtrim => Self::rtrim_cmp(lhs, rhs),
        }
    }

    #[inline(always)]
    fn binary_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline(always)]
    fn nocase_cmp(lhs: &str, rhs: &str) -> Ordering {
        let nocase_lhs = uncased::UncasedStr::new(lhs);
        let nocase_rhs = uncased::UncasedStr::new(rhs);
        nocase_lhs.cmp(nocase_rhs)
    }

    #[inline(always)]
    fn rtrim_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.trim_end_matches(' ').cmp(rhs.trim_end_matches(' '))
    }
}
