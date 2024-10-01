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

// chunk
pub struct IterChunk<'a> {
    data_slice: DataSlice<'a>,
    chunk_size: usize,
    index: usize,
}
impl<'a> IterChunk<'a> {
    pub fn new(slice: DataSlice<'a>, size: usize) -> Self {
        Self {
            data_slice: slice,
            chunk_size: size,
            index: 0,
        }
    }
}
impl<'a> Iterator for IterChunk<'a> {
    type Item = Dataframe;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.data_slice.length() {
            self.index += self.chunk_size;
            Some(
                self.data_slice
                    .slice(
                        self.index - self.chunk_size,
                        std::cmp::min(self.index, self.data_slice.length()),
                    )
                    .unwrap()
                    .to_dataframe(),
            )
        } else {
            None
        }
    }
}
