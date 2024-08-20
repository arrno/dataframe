use crate::column::*;
use crate::dataframe::*;
use crate::format::*;
use crate::util::*;
use std::collections::HashMap;
use std::collections::HashSet;

pub struct DataSlice<'a> {
    title: &'a str,
    columns: Vec<ColSlice<'a>>,
}

impl<'a> DataSlice<'a> {
    pub fn new(title: &'a str, columns: Vec<ColSlice<'a>>) -> Self {
        Self { title, columns }
    }
    pub fn print(&self) {
        Formatter::new().print(self);
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
    pub fn columns(&self) -> &Vec<ColSlice<'a>> {
        &self.columns
    }
    pub fn slice(&self, start: usize, stop: usize) -> Result<DataSlice, MyErr> {
        if start >= stop || stop > self.length() {
            return Err(MyErr::new("Invalid slice params".to_string()));
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
    pub fn col_slice(&self, cols: HashSet<&str>) -> Result<DataSlice, MyErr> {
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
}

impl<'a> From<&'a Dataframe> for DataSlice<'a> {
    fn from(df: &'a Dataframe) -> Self {
        DataSlice {
            title: &df.title(),
            columns: df.columns().iter().map(|col| col.into()).collect(),
        }
    }
}
