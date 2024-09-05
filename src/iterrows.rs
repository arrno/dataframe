use crate::{cell::Cell, dataslice::*};
use std::collections::HashMap;
pub struct Iterrows<'a> {
    data_slice: DataSlice<'a>,
    index: usize,
}
impl<'a> Iterrows<'a> {
    pub fn new(slice: DataSlice<'a>) -> Self {
        Self {
            data_slice: slice,
            index: 0,
        }
    }
}
impl<'a> Iterator for Iterrows<'a> {
    type Item = HashMap<String, &'a Cell>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.data_slice.length() {
            self.index += 1;
            Some(
                self.data_slice
                    .columns()
                    .iter()
                    .map(|col| (col.name().to_string(), &col.values()[self.index - 1]))
                    .collect::<HashMap<String, &'a Cell>>(),
            )
        } else {
            None
        }
    }
}
