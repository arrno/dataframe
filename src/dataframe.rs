use crate::cell::*;
use crate::column::*;
use crate::dataslice::*;
use crate::expression::*;
use crate::row::*;
use crate::util::*;
use std::cmp::min;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

enum SortOrder {
    Asc,
    Desc,
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

    pub fn title(&self) -> &String {
        &self.title
    }
    pub fn columns(&self) -> &Vec<Col> {
        &self.columns
    }

    pub fn from_rows<T>(labels: Vec<&str>, rows: Vec<T>) -> Result<Self, MyErr>
    where
        T: ToRow,
    {
        let mut df = Self::new("dataframe".to_string());
        if rows.len() == 0 {
            return Ok(df);
        }
        let mut cols: Vec<Vec<Cell>> = labels.iter().map(|_| vec![]).collect();
        for row in rows.into_iter() {
            let cells = row.to_row();
            if cells.len() != labels.len() {
                return Err(MyErr::new("Inconsistent data shape".to_string()));
            } else {
                cells
                    .into_iter()
                    .enumerate()
                    .for_each(|(i, cell)| cols[i].push(cell))
            }
        }
        for (i, col) in cols.into_iter().enumerate() {
            df.add_cell_col(labels[i].to_string(), col)?;
        }
        Ok(df)
    }

    pub fn from_csv() {} // TODO
    pub fn to_csv() {} // TODO

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

    pub fn col_map_mut(&mut self) -> HashMap<String, &mut Vec<Cell>> {
        self.columns
            .iter_mut()
            .map(|c| (c.name().to_string(), c.values_mut()))
            .collect()
    }

    pub fn into_col_map(self) -> HashMap<String, Vec<Cell>> {
        self.columns
            .into_iter()
            .map(|c| (c.name().to_string(), c.take_values()))
            .collect()
    }

    pub fn type_map(&self) -> HashMap<String, ()> {
        self.columns
            .iter()
            .map(|col| (format!("{}__{}", col.name(), col.typed().type_string()), ()))
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

    pub fn add_cell_col(&mut self, name: String, set: Vec<Cell>) -> Result<(), MyErr> {
        let l = self.length();
        if l == 0 || l != set.len() {
            return Err(MyErr::new("Invalid col length".to_string()));
        }
        for col in self.columns.iter() {
            if col.name() == name {
                return Err(MyErr::new("Col names must be unique".to_string()));
            }
        }
        let zero = set[0].zero();
        self.columns.push(Col::build(name, set, zero));
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
        Ok(DataSlice::new(
            &self.title,
            self.columns
                .iter()
                .map(|col| ColSlice::new(&col.name(), &col.values()[start..stop], &col.typed()))
                .collect(),
        ))
    }

    pub fn col_slice(&self, cols: HashSet<&str>) -> Result<DataSlice, MyErr> {
        Ok(DataSlice::new(
            &self.title,
            self.columns
                .iter()
                .filter(|col| cols.contains(col.name()))
                .map(|col| ColSlice::new(&col.name(), &col.values(), &col.typed()))
                .collect(),
        ))
    }

    fn compare(&self, with: &Dataframe) -> bool {
        let self_map = self.type_map();
        let with_map = with.type_map();
        if self_map.len() != with_map.len() {
            return false;
        }
        let match_count = self_map
            .iter()
            .filter(|(k, _)| match with_map.get(k.as_str()) {
                Some(_) => true,
                _ => false,
            })
            .collect::<HashMap<&String, &()>>()
            .len();
        match_count == self_map.len()
    }

    pub fn join(&self, with: &Dataframe, on: &str) -> Result<Self, MyErr> {
        // TODO protect against dup col names
        let self_index = self.column(on)?;
        let with_index = with.column(on)?;
        let from_slice = self.col_slice(
            with.columns
                .iter()
                .map(|col| col.name())
                .filter(|name| *name != on)
                .collect(),
        )?;
        // To prevent pushing index twice
        let with_slice = with.col_slice(
            with.columns
                .iter()
                .map(|col| col.name())
                .filter(|name| *name != on)
                .collect(),
        )?;
        if from_slice.match_count(&with_slice) > 0 {
            return Err(MyErr::new(
                "Join dataframe columns are not unique".to_string(),
            ));
        }
        let mut intersect_map: HashMap<String, Vec<usize>> = HashMap::new();
        with_index.values().iter().enumerate().for_each(|(i, val)| {
            intersect_map
                .entry(val.as_string())
                .or_insert(vec![])
                .push(i);
        });
        let mut new_df = Dataframe {
            title: self.title.clone(),
            columns: vec![
                self.columns
                    .iter()
                    .map(|c| c.empty_from())
                    .collect::<Vec<Col>>(),
                with_slice
                    .columns()
                    .iter()
                    .map(|c| c.empty_from())
                    .collect::<Vec<Col>>(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<Col>>(),
        };
        self_index.values().iter().enumerate().for_each(|(i, val)| {
            if let Some(indices) = intersect_map.get_mut(&val.as_string()) {
                indices.iter().for_each(|with_i| {
                    self.columns.iter().enumerate().for_each(|(col_i, col)| {
                        new_df.columns[col_i]
                            .values_mut()
                            .push(col.values()[i].clone())
                    });
                    with_slice
                        .columns()
                        .iter()
                        .enumerate()
                        .for_each(|(col_j, col)| {
                            new_df.columns[self.columns.len() + col_j]
                                .values_mut()
                                .push(col.values()[*with_i].clone())
                        });
                })
            }
        });
        Ok(new_df)
    }

    pub fn zip_join(&self, with: &Dataframe, on: &str) -> Result<Self, MyErr> {
        // TODO protect against dup col names
        let self_index = self.column(on)?;
        let with_index = with.column(on)?;
        let mut intersect_map: HashMap<String, (Vec<usize>, Vec<usize>)> = HashMap::new();
        vec![self_index, with_index]
            .iter()
            .enumerate()
            .for_each(|(idx, set)| {
                set.values().iter().enumerate().for_each(|(i, val)| {
                    let val_string = val.as_string();
                    let indices_op = intersect_map.get_mut(&val_string);
                    if let Some(indices) = indices_op {
                        if idx == 0 {
                            indices.0.push(i);
                        } else {
                            indices.1.push(i);
                        }
                    } else {
                        if idx == 0 {
                            intersect_map.insert(val_string, (vec![i], vec![]));
                        } else {
                            intersect_map.insert(val_string, (vec![], vec![i]));
                        }
                    }
                });
            });
        // To prevent pushing index twice
        let with_slice = with.col_slice(
            with.columns
                .iter()
                .map(|col| col.name())
                .filter(|name| *name != on)
                .collect(),
        )?;
        let mut new_df = Dataframe {
            title: self.title.clone(),
            columns: vec![
                self.columns
                    .iter()
                    .map(|c| c.empty_from())
                    .collect::<Vec<Col>>(),
                with_slice
                    .columns()
                    .iter()
                    .map(|c| c.empty_from())
                    .collect::<Vec<Col>>(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<Col>>(),
        };
        // This is an inner join
        intersect_map.iter().for_each(|(key, indices)| {
            indices.0.iter().for_each(|i| {
                indices.1.iter().for_each(|j| {
                    self.columns.iter().enumerate().for_each(|(col_i, col)| {
                        new_df.columns[col_i]
                            .values_mut()
                            .push(col.values()[*i].clone())
                    });
                    with_slice
                        .columns()
                        .iter()
                        .enumerate()
                        .for_each(|(col_j, col)| {
                            new_df.columns[self.columns.len() + col_j]
                                .values_mut()
                                .push(col.values()[*j].clone())
                        });
                })
            })
        });
        Ok(new_df)
    }

    pub fn sort(&mut self, by: &str, order: SortOrder) -> Result<(), MyErr> {
        let self_index = self.column_mut(&by)?;
        let sort_instruct = self_index
            .values()
            .iter()
            .enumerate()
            .map(|(i, v)| {
                if i == 0 {
                    Ordering::Equal
                } else {
                    match order {
                        SortOrder::Asc => {
                            if self_index.values()[i - 1] > *v {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        SortOrder::Desc => {
                            if self_index.values()[i - 1] > *v {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                    }
                }
            })
            .collect::<Vec<Ordering>>();
        self.col_map_mut().iter_mut().for_each(|(_, col)| {
            let mut idx = 0;
            col.sort_by(|_, _| {
                idx += 1;
                sort_instruct[idx]
            })
        });
        Ok(())
    }

    pub fn column(&self, name: &str) -> Result<&Col, MyErr> {
        match self.columns.iter().find(|col| col.name() == name) {
            Some(col) => Ok(col),
            None => Err(MyErr::new("join column not found on self.".to_string())),
        }
    }

    pub fn column_mut(&mut self, name: &str) -> Result<&mut Col, MyErr> {
        match self.columns.iter_mut().find(|col| col.name() == name) {
            Some(col) => Ok(col),
            None => Err(MyErr::new("join column not found on self.".to_string())),
        }
    }

    pub fn concat(&mut self, with: Dataframe) -> Result<(), MyErr> {
        if !self.compare(&with) {
            return Err(MyErr::new(
                "Concat against mismatched dataframes".to_string(),
            ));
        }
        let mut self_map = self.col_map_mut();
        with.into_col_map().into_iter().for_each(|(name, ext_col)| {
            if let Some(loc_col) = self_map.get_mut(&name) {
                loc_col.extend(ext_col);
            }
        });
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
