
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
