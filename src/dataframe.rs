use crate::cell::*;
use crate::column::*;
use crate::expression::*;
use crate::format::*;
use crate::util::*;
use std::cmp::min;
use std::collections::HashMap;

pub struct Dataframe {
    title: String,
    columns: Vec<Col>,
}

pub struct DataSlice<'a> {
    title: &'a str,
    columns: Vec<ColSlice<'a>>,
}

impl<'a> DataSlice<'a> {
    pub fn print(&self) {
        Formatter::new().print(self);
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
}

impl<'a> From<&'a Dataframe> for DataSlice<'a> {
    fn from(df: &'a Dataframe) -> Self {
        DataSlice {
            title: &df.title,
            columns: df.columns.iter().map(|col| col.into()).collect(),
        }
    }
}

impl Dataframe {
    pub fn new(title: String) -> Self {
        Dataframe {
            title: title,
            columns: vec![],
        }
    }

    pub fn from_csv() {} // TODO
    pub fn to_csv() {} // TODO
    pub fn from_json() {} // TODO

    pub fn col_mut(&mut self, name: String) -> Option<&mut Vec<Cell>> {
        self.columns
            .iter_mut()
            .find(|col| col.name() == name)?
            .values_mut()
            .into()
    }

    pub fn col_map(&self) -> HashMap<String, &Vec<Cell>> {
        self.columns
            .iter()
            .map(|c| (c.name().to_string(), c.values()))
            .collect()
    }

    pub fn add_col<T>(&mut self, name: String, set: Vec<T>) -> Result<(), MyErr>
    where
        T: ToCell,
    {
        let l = self.length();
        if l != 0 && l != set.len() {
            return Err(MyErr::new("Invalid col length".to_string()));
        }
        for col in self.columns.iter() {
            if col.name() == name {
                return Err(MyErr::new("Col names must be unique".to_string()));
            }
        }
        self.columns.push(Col::new(name, set));
        Ok(())
    }

    pub fn add_opt_col<T>(&mut self, name: String, set: Vec<Option<T>>) -> Result<(), MyErr>
    where
        T: ToCell,
    {
        let l = self.length();
        if l != 0 && l != set.len() {
            return Err(MyErr::new("Invalid col length".to_string()));
        }
        for col in self.columns.iter() {
            if col.name() == name {
                return Err(MyErr::new("Col names must be unique".to_string()));
            }
        }
        self.columns.push(Col::new(name, set));
        Ok(())
    }

    pub fn add_row<T>(&mut self, set: Vec<T>) -> Result<(), MyErr>
    where
        T: ToCell,
    {
        if set.len() != self.columns.len() {
            return Err(MyErr::new("Invalid col length".to_string()));
        }
        for (i, col) in self.columns.iter().enumerate() {
            if col.values().len() > 0 && col.values()[0].zero() != set[i].ref_to_cell().zero() {
                return Err(MyErr::new("Invalid col types".to_string()));
            }
        }
        for i in 0..set.len() {
            self.columns[i].values_mut().push(set[i].ref_to_cell());
        }
        Ok(())
    }

    pub fn add_opt_row<T>(&mut self, set: Vec<Option<T>>) -> Result<(), MyErr>
    where
        T: ToCell,
    {
        if set.len() != self.columns.len() {
            return Err(MyErr::new("Invalid col length".to_string()));
        }
        for (i, col) in self.columns.iter().enumerate() {
            if col.values().len() > 0 && col.values()[0].zero() != set[i].ref_to_cell().zero() {
                return Err(MyErr::new("Invalid col types".to_string()));
            }
        }
        for i in 0..set.len() {
            self.columns[i].values_mut().push(set[i].ref_to_cell());
        }
        Ok(())
    }

    pub fn filter(&mut self, mut exp: Exp) -> Result<Self, MyErr> {
        let col_map = self.col_map();
        let filter_set = (0..self.length())
            .map(|i| {
                let val_map: HashMap<String, &Cell> =
                    col_map.iter().map(|(k, v)| (k.to_owned(), &v[i])).collect();
                exp.evaluate(&val_map)
            })
            .collect::<Vec<bool>>();

        Ok(Dataframe {
            title: self.title.clone(),
            columns: self
                .columns
                .iter()
                .map(|col| {
                    Col::build(
                        col.name().to_string(),
                        col.values()
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| filter_set[*i])
                            .map(|(_, c)| c.clone())
                            .collect(),
                        col.typed().clone(),
                    )
                })
                .collect(),
        })
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

    fn compare(&self, with: &Dataframe) -> Result<(), MyErr> {
        Ok(())
    }

    pub fn join(&mut self, with: Dataframe, on: &str) -> Result<(), MyErr> {
        Ok(())
    }

    pub fn concat(&mut self, with: Dataframe) -> Result<(), MyErr> {
        Ok(())
    }

    fn length(&self) -> usize {
        if self.columns.len() == 0 {
            return 0;
        }
        self.columns[0].values().len()
    }

    pub fn head(&self) -> Result<(), MyErr> {
        let head_df = self.slice(0, min(5, self.length()))?;
        head_df.print();
        Ok(())
    }

    pub fn print(&self) {
        DataSlice::from(self).print();
    }
}
