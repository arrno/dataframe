use crate::cell::*;

pub trait ToRow {
    fn to_row(self) -> Vec<Cell>;
}
