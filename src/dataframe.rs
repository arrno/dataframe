use crate::cell::*;
use crate::expressions::*;
use crate::util::*;
use std::cmp::{max, min};
use std::collections::HashMap;

pub struct Col {
    name: String,
    values: Vec<Cell>,
    typed: Cell,
}

impl Col {
    pub fn new<T>(name: String, set: Vec<T>) -> Self
    where
        T: ToCell,
    {
        let mut z = Cell::Null;
        if set.len() > 0 {
            z = set[0].ref_to_cell().zero();
        }
        Col {
            name: name,
            values: set.into_iter().map(|val| val.to_cell()).collect(), // should validate all types match
            typed: z,
        }
    }
    fn values_mut(&mut self) -> &mut Vec<Cell> {
        &mut self.values
    }
}

pub struct Dataframe {
    title: String,
    columns: Vec<Col>,
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
    pub fn join() {} // TODO

    pub fn col_mut(&mut self, name: String) -> Option<&mut Vec<Cell>> {
        self.columns
            .iter_mut()
            .find(|col| col.name == name)?
            .values_mut()
            .into()
    }

    pub fn col_map(&self) -> HashMap<String, &Vec<Cell>> {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), &c.values))
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
            if col.name == name {
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
            if col.name == name {
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
            if col.values.len() > 0 && col.values[0].zero() != set[i].ref_to_cell().zero() {
                return Err(MyErr::new("Invalid col types".to_string()));
            }
        }
        for i in 0..set.len() {
            self.columns[i].values.push(set[i].ref_to_cell());
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
            if col.values.len() > 0 && col.values[0].zero() != set[i].ref_to_cell().zero() {
                return Err(MyErr::new("Invalid col types".to_string()));
            }
        }
        for i in 0..set.len() {
            self.columns[i].values.push(set[i].ref_to_cell());
        }
        Ok(())
    }

    pub fn filter_simple(&mut self, exp: ExpU) -> Result<Self, MyErr> {
        let filter_col = match self.columns.iter().find(|col| col.name == *exp.target()) {
            Some(col) => col,
            None => return Err(MyErr::new("Target not found".to_string())),
        };

        let filter_set: Vec<bool> = filter_col
            .values
            .iter()
            .map(|val| match exp {
                _ => true, // TODO
            })
            .collect();

        Ok(Dataframe {
            title: self.title.clone(),
            columns: self
                .columns
                .iter()
                .map(|col| Col {
                    name: col.name.clone(),
                    typed: col.typed.clone(),
                    values: col
                        .values
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| filter_set[*i])
                        .map(|(_, c)| c.clone())
                        .collect(),
                })
                .collect(),
        })
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
                .map(|col| Col {
                    name: col.name.clone(),
                    typed: col.typed.clone(),
                    values: col
                        .values
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| filter_set[*i])
                        .map(|(_, c)| c.clone())
                        .collect(),
                })
                .collect(),
        })
    }

    pub fn slice(&self, start: usize, stop: usize) -> Result<Self, MyErr> {
        if start >= stop || stop > self.length() {
            return Err(MyErr::new("Invalid slice params".to_string()));
        }
        Ok(Dataframe {
            title: self.title.clone(),
            columns: self
                .columns
                .iter()
                .map(|col| Col {
                    name: col.name.clone(),
                    typed: col.typed.clone(),
                    values: col.values[start..stop].to_vec(),
                })
                .collect(),
        })
    }

    fn length(&self) -> usize {
        if self.columns.len() == 0 {
            return 0;
        }
        self.columns[0].values.len()
    }

    pub fn print(&self) {
        let mut col_lengths: Vec<usize> = self
            .columns
            .iter()
            .map(|col| min(MAX_CELL_DISPLAY, col.name.len()))
            .collect();
        // Calc col sizes
        self.columns.iter().enumerate().for_each(|(i, col)| {
            col.values.iter().for_each(|val| {
                col_lengths[i] = min(MAX_CELL_DISPLAY, max(col_lengths[i], val.as_string().len()))
            })
        });
        // Make sep
        let sep = (0..col_lengths.len())
            .map(|i| {
                let s = "-".to_string().repeat(col_lengths[i]);
                format!("+{s}")
            })
            .collect::<Vec<String>>()
            .join("");
        // Do print
        println!("{sep}+");
        self.columns
            .iter()
            .enumerate()
            .for_each(|(i, col)| print!("|{}", pad_string(&col.name, col_lengths[i])));
        print!("|\n");
        println!("{sep}+");
        for row in 0..self.length() {
            for col in 0..col_lengths.len() {
                print!(
                    "|{}",
                    pad_string(&self.columns[col].values[row].as_string(), col_lengths[col])
                );
            }
            print!("|\n")
        }
        println!("{sep}+");
    }

    pub fn head(&self) -> Result<(), MyErr> {
        // Slice head
        let head_df = self.slice(0, min(5, self.length()))?;
        head_df.print();
        Ok(())
    }
}
