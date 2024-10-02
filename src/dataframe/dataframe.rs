pub use crate::{
    cell::*,
    column::*,
    expression::{Op::*, *},
    group::Reducer::*,
    row,
    row::*,
    sort::*,
    util::Error,
};
use crate::{
    dataslice::*,
    group::DataGroup,
    iterrows::{self, *},
};
use csv::Writer;
pub use dataframe_macros::ToRow;
use serde::Deserialize;
use std::{
    cmp::{max, min, Ordering},
    collections::{HashMap, HashSet},
    error::Error as StdError,
    fs::File,
};

#[derive(Debug, PartialEq)]
pub struct Dataframe {
    title: String,
    columns: Vec<Col>,
}

impl Dataframe {
    pub fn new(title: Option<&str>) -> Self {
        Dataframe {
            title: match title {
                Some(v) => v.to_string(),
                None => String::new(),
            },
            columns: vec![],
        }
    }

    pub fn title(&self) -> &String {
        &self.title
    }
    pub fn columns(&self) -> &Vec<Col> {
        &self.columns
    }
    pub fn columns_mut(&mut self) -> &mut Vec<Col> {
        &mut self.columns
    }

    pub fn from_structs<T>(rows: Vec<T>) -> Result<Self, Error>
    where
        T: ToRow,
    {
        let mut df = Self::new(None);
        if rows.len() == 0 {
            return Ok(df);
        }
        let type_checks: Vec<fn(&Cell) -> bool> = rows[0]
            .to_row()
            .iter()
            .map(|cell| cell_to_type_check(cell))
            .collect();
        let labels = rows[0].labels();
        let mut cols: Vec<Vec<Cell>> = labels.iter().map(|_| vec![]).collect();
        for row in rows.into_iter() {
            let cells = row.to_row();
            if cells.len() != labels.len() {
                return Err(Error::new("Inconsistent data shape".to_string()));
            } else {
                for (i, cell) in cells.into_iter().enumerate() {
                    if !type_checks[i](&cell) {
                        return Err(Error::new("Inconsistent col types".to_string()));
                    }
                    cols[i].push(cell)
                }
            }
        }
        for (i, col) in cols.into_iter().enumerate() {
            df.add_cell_col(labels[i].to_string(), col)?;
        }
        Ok(df)
    }

    pub fn from_string_rows(labels: Vec<String>, rows: Vec<Vec<Cell>>) -> Result<Self, Error> {
        Self::do_from_rows(labels, rows)
    }
    pub fn from_rows(labels: Vec<&str>, rows: Vec<Vec<Cell>>) -> Result<Self, Error> {
        Self::do_from_rows(labels.iter().map(|l| l.to_string()).collect(), rows)
    }

    fn do_from_rows(mut labels: Vec<String>, rows: Vec<Vec<Cell>>) -> Result<Self, Error> {
        let mut type_checks: Vec<fn(&Cell) -> bool> = vec![];
        if rows.len() > 0 {
            type_checks = rows[0]
                .iter()
                .map(|cell| cell_to_type_check(cell))
                .collect();
        }
        let mut df = Self::new(None);
        if rows.len() == 0 {
            return Ok(df);
        }
        let mut cols: Vec<Vec<Cell>> = labels.iter().map(|_| vec![]).collect();
        for cells in rows.into_iter() {
            if cells.len() != labels.len() {
                return Err(Error::new("Inconsistent data shape".to_string()));
            } else {
                for (i, cell) in cells.into_iter().enumerate() {
                    if !type_checks[i](&cell) {
                        return Err(Error::new("Inconsistent col types".to_string()));
                    }
                    cols[i].push(cell);
                }
            }
        }
        labels.reverse();
        for col in cols.into_iter() {
            df.add_cell_col(labels.pop().unwrap(), col)?;
        }
        Ok(df)
    }

    pub fn to_rows(self) -> (Vec<String>, Vec<Vec<Cell>>) {
        let mut results: Vec<Vec<Cell>> = (0..self.length()).map(|_| vec![]).collect();
        let names = self
            .columns
            .into_iter()
            .map(|c| {
                let name = c.name().to_string();
                c.take_values()
                    .into_iter()
                    .enumerate()
                    .for_each(|(col_idx, val)| results[col_idx].push(val));
                name
            })
            .collect::<Vec<String>>();
        (names, results)
    }

    pub fn from_csv<T>(file_path: &str) -> Result<Self, Error>
    where
        for<'a> T: ToRow + Deserialize<'a>,
    {
        let mut rows: Vec<Vec<Cell>> = Vec::new();
        let mut labels = vec![];
        let file = match File::open(file_path) {
            Ok(f) => f,
            Err(e) => return Err(Error::new(e.to_string())),
        };
        let mut reader = csv::Reader::from_reader(file);
        for record in reader.deserialize() {
            let record: T = match record {
                Ok(r) => r,
                Err(e) => return Err(Error::new(e.to_string())),
            };
            rows.push(record.to_row());
            if labels.len() == 0 {
                labels = record.labels();
            }
        }
        Self::from_rows(labels.iter().map(|l| l.as_str()).collect(), rows)
    }

    pub fn to_csv(&self, file_path: &str) -> Result<(), Box<dyn StdError>> {
        let mut wtr = Writer::from_path(file_path)?;
        wtr.write_record(
            self.columns
                .iter()
                .map(|col| col.name().to_string())
                .collect::<Vec<String>>(),
        )?;
        for i in 0..self.length() {
            wtr.write_record(
                self.columns
                    .iter()
                    .map(|col| col.values()[i].as_string())
                    .collect::<Vec<String>>(),
            )?;
        }
        wtr.flush()?;
        Ok(())
    }
    pub fn rename_col(&mut self, from: &str, to: &str) -> bool {
        match self.columns.iter_mut().find(|col| col.name() == from) {
            Some(col) => {
                col.rename(to.to_string());
                true
            }
            None => false,
        }
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

    fn set_columns(mut self, cols: Vec<Col>) -> Result<Self, Error> {
        if cols.len() > 0 {
            let l = cols[0].values().len();
            match cols.iter().find(|c| c.values().len() != l) {
                Some(_) => return Err(Error::new("Inconsistent data shape".to_string())),
                None => (),
            };
        }
        self.columns = cols;
        Ok(self)
    }

    pub fn add_col<T>(&mut self, name: &str, set: Vec<T>) -> Result<(), Error>
    where
        T: ToCell,
    {
        let l = self.length();
        if l != 0 && l != set.len() {
            return Err(Error::new("Invalid col length".to_string()));
        }
        for col in self.columns.iter() {
            if col.name() == name {
                return Err(Error::new("Col names must be unique".to_string()));
            }
        }
        self.columns.push(Col::new(name.into(), set));
        Ok(())
    }

    fn add_cell_col(&mut self, name: String, set: Vec<Cell>) -> Result<(), Error> {
        let l = self.length();
        if l != 0 && l != set.len() {
            return Err(Error::new("Invalid col length".to_string()));
        }
        for col in self.columns.iter() {
            if col.name() == name {
                return Err(Error::new("Col names must be unique".to_string()));
            }
        }
        let zero = set[0].zero();
        self.columns.push(Col::build(name, set, zero));
        Ok(())
    }

    pub fn add_row(&mut self, row: Vec<Cell>) -> Result<(), Error> {
        if row.len() != self.columns.len() {
            return Err(Error::new("Invalid row length".to_string()));
        }
        for (i, col) in self.columns.iter().enumerate() {
            if col.values().len() > 0 && col.values()[0].zero() != row[i].zero() {
                return Err(Error::new("Invalid col types".to_string()));
            }
        }
        row.into_iter().enumerate().for_each(|(i, cell)| {
            self.columns[i].values_mut().push(cell);
        });
        Ok(())
    }

    pub fn filter(&mut self, exp: Exp) -> Result<Self, Error> {
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

    pub fn to_slice(&self) -> DataSlice {
        self.slice(0, self.length()).unwrap()
    }

    pub fn slice(&self, start: usize, stop: usize) -> Result<DataSlice, Error> {
        if start > stop || stop > self.length() {
            return Err(Error::new("Invalid slice params".to_string()));
        }
        Ok(DataSlice::new(
            &self.title,
            self.columns
                .iter()
                .map(|col| ColSlice::new(&col.name(), &col.values()[start..stop], &col.typed()))
                .collect(),
        ))
    }

    pub fn col_slice(&self, cols: HashSet<&str>) -> Result<DataSlice, Error> {
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

    pub fn join(&self, with: &Dataframe, on: (&str, &str)) -> Result<Self, Error> {
        self.do_join(with, on, false)
    }
    pub fn left_join(&self, with: &Dataframe, on: (&str, &str)) -> Result<Self, Error> {
        self.do_join(with, on, true)
    }
    fn do_join(&self, with: &Dataframe, on: (&str, &str), left: bool) -> Result<Self, Error> {
        let self_index = self.column(on.0)?;
        let with_index = with.column(on.1)?;
        let match_count = self.match_count(&with);
        if (on.0 == on.1 && match_count != 1) || (on.0 != on.1 && match_count != 0) {
            return Err(Error::new(
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
        // To prevent adding index twice
        let with_slice = with.col_slice(
            with.columns
                .iter()
                .map(|col| col.name())
                .filter(|name| *name != on.1)
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
            } else if left {
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
                            .push(col.typed().null())
                    });
            }
        });
        Ok(new_df)
    }

    pub fn sort(&mut self, by: &str, order: SortOrder) -> Result<(), Error> {
        let self_index = self.col_mut(&by)?;
        let mut sort_instructions = Vec::new();
        self_index.values_mut().sort_by(|cur, prev| match order {
            SortOrder::Asc => {
                if cur > prev {
                    sort_instructions.push(Ordering::Greater);
                    Ordering::Greater
                } else {
                    sort_instructions.push(Ordering::Less);
                    Ordering::Less
                }
            }
            SortOrder::Desc => {
                if cur > prev {
                    sort_instructions.push(Ordering::Less);
                    Ordering::Less
                } else {
                    sort_instructions.push(Ordering::Greater);
                    Ordering::Greater
                }
            }
        });
        self.col_map_mut().iter_mut().for_each(|(name, col)| {
            let mut idx = 0;
            if name != by {
                col.sort_by(|_, _| {
                    let ord = sort_instructions[idx];
                    idx += 1;
                    ord
                })
            }
        });
        Ok(())
    }

    pub fn into_sort(self) -> DataSort {
        DataSort::new(self)
    }

    pub fn column(&self, name: &str) -> Result<&Col, Error> {
        match self.columns.iter().find(|col| col.name() == name) {
            Some(col) => Ok(col),
            None => Err(Error::new("Column not found".to_string())),
        }
    }
    fn take_column(self, name: &str) -> Result<Col, Error> {
        match self.columns.into_iter().find(|col| col.name() == name) {
            Some(col) => Ok(col),
            None => Err(Error::new("Column not found".to_string())),
        }
    }

    pub fn col_mut(&mut self, name: &str) -> Result<&mut Col, Error> {
        match self.columns.iter_mut().find(|col| col.name() == name) {
            Some(col) => Ok(col),
            None => Err(Error::new("Column not found".to_string())),
        }
    }

    pub fn cell(&mut self, loc: (usize, &str)) -> Option<&Cell> {
        if let Ok(col) = self.col_mut(loc.1) {
            if col.values().len() >= loc.0 {
                return Some(&col.values_mut()[loc.0]);
            }
        }
        None
    }

    pub fn cell_mut(&mut self, loc: (usize, &str)) -> Option<&mut Cell> {
        if let Ok(col) = self.col_mut(loc.1) {
            if col.values().len() >= loc.0 {
                return Some(&mut col.values_mut()[loc.0]);
            }
        }
        None
    }

    pub fn concat(&mut self, with: Dataframe) -> Result<(), Error> {
        if !self.compare(&with) {
            return Err(Error::new(
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

    pub fn length(&self) -> usize {
        if self.columns.len() == 0 {
            return 0;
        }
        self.columns[0].values().len()
    }

    pub fn head(&self, count: usize) {
        match self.slice(0, min(count, self.length())) {
            Ok(head_df) => head_df.print(),
            Err(_) => println!("[...]"),
        };
    }

    pub fn tail(&self, count: usize) {
        let length = self.length();
        let start = max(0, length as isize - count as isize);
        match self.slice(start as usize, length) {
            Ok(tail_df) => tail_df.print(),
            Err(_) => println!("[...]"),
        };
    }

    pub fn col_names(&self) -> Vec<&str> {
        self.columns().iter().map(|col| col.name()).collect()
    }

    pub fn col_types(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|c| format!("{} <{}>", c.name(), c.typed().type_string()))
            .collect()
    }

    pub fn info(&self) {
        let shape = format!("{}_col x {}_row", self.columns().len(), self.length());
        let columns = self.col_types().join(", ");
        println!("DF Info\nShape: {shape}\nColumns: {columns}");
    }

    pub fn print(&self) {
        DataSlice::from(self).print();
    }

    pub fn iter<'a>(&'a self) -> Iterrows<'a> {
        iterrows::Iterrows::new(self.to_slice())
    }

    pub fn iter_chunk<'a>(&'a self, size: usize) -> IterChunk<'a> {
        iterrows::IterChunk::new(self.to_slice(), size)
    }

    pub fn drop_cols(&mut self, col_names: HashSet<&str>) {
        self.columns.retain(|col| !col_names.contains(col.name()))
    }
    pub fn retain_cols(&mut self, col_names: HashSet<&str>) {
        self.columns.retain(|col| col_names.contains(col.name()))
    }

    pub fn describe(&self) -> Dataframe {
        let df = Dataframe::new(None);
        let mut cols = vec![Col::build(
            "::".to_string(),
            vec![
                Cell::Str("count".to_string()),
                Cell::Str("mean".to_string()),
                Cell::Str("std".to_string()),
                Cell::Str("min".to_string()),
                Cell::Str("25%".to_string()),
                Cell::Str("50%".to_string()),
                Cell::Str("75%".to_string()),
                Cell::Str("max".to_string()),
                Cell::Str("unique".to_string()),
                Cell::Str("top idx".to_string()),
                Cell::Str("freq".to_string()),
            ],
            Cell::Str("".to_string()),
        )];
        cols.extend(
            self.columns
                .iter()
                .map(|col| col.describe().take_column(col.name()).unwrap()),
        );
        df.set_columns(cols).unwrap()
    }

    pub fn group_by(&self, by: &str) -> DataGroup {
        DataGroup::new(self.to_slice(), by.to_string())
    }
}

// Moving these functions into dataframe module so as to not expose `set_columns`
impl<'a> DataSlice<'a> {
    pub fn to_dataframe(&self) -> Dataframe {
        Dataframe::new(Some(self.title()))
            .set_columns(
                self.columns()
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

    pub fn chunk_by(&self, by: &str) -> Result<Vec<Dataframe>, Error> {
        let mut chunks_idx: HashMap<String, usize> = HashMap::new();
        let mut chunks: Vec<Vec<Vec<Cell>>> = vec![];
        let by_idx = match self
            .columns()
            .iter()
            .enumerate()
            .find(|(_, c)| c.name() == by)
            .map(|(i, _)| i)
        {
            Some(v) => v,
            None => return Err(Error::new("Group by col not found".to_string())),
        };
        (0..self.length()).for_each(|i| {
            let key = self
                .columns()
                .get(by_idx)
                .unwrap()
                .values()
                .get(i)
                .unwrap()
                .as_string();
            let chunk_idx = match chunks_idx.get(&key) {
                Some(i) => *i,
                None => {
                    chunks.push(self.columns().iter().map(|_| vec![]).collect());
                    chunks_idx.insert(key, chunks.len() - 1);
                    chunks.len() - 1
                }
            };
            let chunk = chunks.get_mut(chunk_idx).unwrap();
            self.columns()
                .iter()
                .enumerate()
                .for_each(|(j, col)| chunk.get_mut(j).unwrap().push(col.values()[i].clone()));
        });
        Ok(chunks
            .into_iter()
            .map(|df_cols| {
                Dataframe::new(None)
                    .set_columns(
                        df_cols
                            .into_iter()
                            .enumerate()
                            .map(|(i, values)| {
                                Col::build(
                                    self.columns()[i].name().to_string(),
                                    values,
                                    self.columns()[i].typed().clone(),
                                )
                            })
                            .collect(),
                    )
                    .unwrap()
            })
            .collect())
    }
}
