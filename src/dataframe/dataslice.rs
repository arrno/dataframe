use crate::dataframe::*;
use crate::format::*;
use crate::iterrows;
use crate::iterrows::*;
use crate::util::*;
use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub struct DataSlice<'a> {
    title: &'a str,
    columns: Vec<ColSlice<'a>>,
}

impl<'a> DataSlice<'a> {
    pub fn new(title: &'a str, columns: Vec<ColSlice<'a>>) -> Self {
        Self { title, columns }
    }
    pub fn print(&self) {
        TableFormatter::new().print(self);
    }
    pub fn to_dataframe(&self) -> Dataframe {
        Dataframe::new(Some(self.title))
            .set_columns(
                self.columns
                    .iter()
                    .map(|col| {
                        Col::build(
                            col.name().to_string(),
                            col.values().iter().map(|val| val.clone()).collect(),
                            col.typed().clone(),
                        )
                    })
                    .collect(),
            )
            .unwrap()
    }
    pub fn title(&self) -> &'a str {
        self.title
    }

    pub fn length(&self) -> usize {
        if self.columns.len() == 0 {
            return 0;
        }
        self.columns[0].values().len()
    }
    pub fn col_names(&self) -> Vec<&str> {
        self.columns().iter().map(|col| col.name()).collect()
    }
    pub fn columns(&self) -> &Vec<ColSlice<'a>> {
        &self.columns
    }
    pub fn slice(&self, start: usize, stop: usize) -> Result<DataSlice, Error> {
        if start >= stop || stop > self.length() {
            return Err(Error::new("Invalid slice params".to_string()));
        }
        Ok(DataSlice {
            title: &self.title,
            columns: self
                .columns
                .iter()
                .map(|col| ColSlice::new(&col.name(), &col.values()[start..stop], &col.typed()))
                .collect(),
        })
    }
    pub fn col_slice(&self, cols: HashSet<&str>) -> Result<DataSlice, Error> {
        Ok(DataSlice {
            title: &self.title,
            columns: self
                .columns
                .iter()
                .filter(|col| cols.contains(col.name()))
                .map(|col| ColSlice::new(&col.name(), &col.values(), &col.typed()))
                .collect(),
        })
    }
    pub fn type_map(&self) -> HashMap<String, ()> {
        self.columns
            .iter()
            .map(|col| (format!("{}__{}", col.name(), col.typed().type_string()), ()))
            .collect()
    }
    pub fn match_count(&self, with: &Self) -> usize {
        let self_map = self.type_map();
        let with_map = with.type_map();
        self_map
            .iter()
            .filter(|(k, _)| match with_map.get(k.as_str()) {
                Some(_) => true,
                _ => false,
            })
            .collect::<HashMap<&String, &()>>()
            .len()
    }

    pub fn iter(self) -> Iterrows<'a> {
        iterrows::Iterrows::new(self)
    }
}

impl<'a> From<&'a Dataframe> for DataSlice<'a> {
    fn from(df: &'a Dataframe) -> Self {
        DataSlice {
            title: &df.title(),
            columns: df.columns().iter().map(|col| col.into()).collect(),
        }
    }
}

// // TODO
// #[derive(Debug, PartialEq)]
// pub struct DataSliceMut<'a> {
//     title: &'a str,
//     columns: Vec<ColSliceMut<'a>>,
//     length: usize,
// }

// impl<'a> DataSliceMut<'a> {
//     pub fn new(title: &'a str, columns: Vec<ColSliceMut<'a>>) -> Self {
//         let length = match columns.get(0) {
//             Some(col) => col.values().len(),
//             _ => 0,
//         };
//         Self {
//             title,
//             columns,
//             length,
//         }
//     }
//     pub fn length(&self) -> usize {
//         self.length
//     }
//     pub fn columns(&self) -> &Vec<ColSliceMut<'a>> {
//         &self.columns
//     }
// }
