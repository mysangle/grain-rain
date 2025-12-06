
/// Sort orders
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
pub enum SortOrder {
    /// `ASC`
    Asc,
    /// `DESC`
    Desc,
}

/// foreign-key reference actions
#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RefAct {
    /// `SET NULL`
    SetNull,
    /// `SET DEFAULT`
    SetDefault,
    /// `CASCADE`
    Cascade,
    /// `RESTRICT`
    Restrict,
    /// `NO ACTION`
    NoAction,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Expr {

}
