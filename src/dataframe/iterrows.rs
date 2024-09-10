use crate::{cell::Cell, dataframe::Dataframe, dataslice::*};
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

pub struct IntoIterrows {
    dataframe: Dataframe,
}
impl IntoIterrows {
    pub fn new(df: Dataframe) -> Self {
        Self { dataframe: df }
    }
}
impl Iterator for IntoIterrows {
    type Item = HashMap<String, Cell>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.dataframe.length() > 0 {
            Some(
                self.dataframe
                    .columns_mut()
                    .iter_mut()
                    .map(|col| (col.name().to_string(), col.values_mut().remove(0)))
                    .collect::<HashMap<String, Cell>>(),
            )
        } else {
            None
        }
    }
}
impl IntoIterator for Dataframe {
    type Item = HashMap<String, Cell>;
    type IntoIter = IntoIterrows;

    fn into_iter(self) -> IntoIterrows {
        IntoIterrows::new(self)
    }
}

// TODO
pub struct IterrowsMut<'a> {
    data_slice: DataSliceMut<'a>,
    index: usize,
}
impl<'a> IterrowsMut<'a> {
    pub fn new(slice: DataSliceMut<'a>) -> Self {
        Self {
            data_slice: slice,
            index: 0,
        }
    }
}

impl<'a> Iterator for IterrowsMut<'a> {
    type Item = HashMap<String, &'a &'a mut Cell>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.data_slice.length() {
            self.index += 1;
            Some(
                self.data_slice
                    .columns()
                    .iter()
                    .map(|col| (col.name().to_string(), &col.values()[self.index - 1]))
                    .collect::<HashMap<String, &'a &'a mut Cell>>(),
            )
        } else {
            None
        }
    }
}
