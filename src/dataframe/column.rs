use crate::{cell::*, dataframe::Dataframe};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
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
        let mut z = Cell::Null(Box::new(Cell::Int(0)));
        if set.len() > 0 {
            z = set[0].ref_to_cell().zero();
        }
        Col {
            name: name,
            values: set.into_iter().map(|val| val.to_cell()).collect(), // should validate all types match
            typed: z,
        }
    }
    pub fn build(name: String, values: Vec<Cell>, typed: Cell) -> Self {
        Col {
            name,
            values,
            typed,
        }
    }
    pub fn values_mut(&mut self) -> &mut Vec<Cell> {
        &mut self.values
    }
    pub fn values(&self) -> &Vec<Cell> {
        &self.values
    }
    pub fn take_values(self) -> Vec<Cell> {
        self.values
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn typed(&self) -> &Cell {
        &self.typed
    }
    pub fn empty_from(&self) -> Col {
        Col {
            name: self.name.clone(),
            values: vec![],
            typed: self.typed.clone(),
        }
    }
    fn describe_object_empty(&self) -> Dataframe {
        Dataframe::from_rows(
            vec!["count", "unique", "top", "frequency"],
            vec![vec![Cell::Int(0), null_float(), null_float(), null_float()]],
        )
        .unwrap()
    }

    pub fn describe_object(&self) -> Dataframe {
        if self.values.len() == 0 {
            return self.describe_object_empty();
        }
        let mut df = Dataframe::new(None);
        let mut freq: HashMap<String, usize> = HashMap::new();
        let mut most = 0;
        let mut top = &self.typed;
        self.values.iter().for_each(|cell| {
            let val = freq.entry(cell.as_string()).or_insert(0);
            *val += 1;
            if *val > most {
                most = *val;
                top = cell;
            }
        });
        df.add_col::<u64>("count", vec![self.values.len() as u64])
            .unwrap();
        df.add_cell_col("unique".to_string(), vec![Cell::Uint(freq.len() as u64)])
            .unwrap();
        df.add_cell_col("top".to_string(), vec![top.clone()])
            .unwrap();
        df.add_cell_col("frequency".to_string(), vec![Cell::Uint(most as u64)])
            .unwrap();
        df
    }

    fn describe_num_empty(&self) -> Dataframe {
        Dataframe::from_rows(
            vec!["count", "mean", "std", "min", "25%", "50%", "75%", "max"],
            vec![vec![
                Cell::Int(0),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
            ]],
        )
        .unwrap()
    }
    // TODO -> zero div err, bubble up err
    pub fn describe_numeric(&self) -> Dataframe {
        if self.values.len() == 0 {
            return self.describe_num_empty();
        }
        let mut df = Dataframe::new(None);
        df.add_col::<u64>("count", vec![self.values.len() as u64])
            .unwrap();
        let mut total = self.typed.zero();
        let mut sorted_set: Vec<Cell> = self
            .values
            .iter()
            .map(|cell| {
                if let Some(c) = total.add(cell) {
                    total = c;
                }
                cell.clone()
            })
            .collect();
        sorted_set.sort_by(|cur, prev| match cur > prev {
            true => Ordering::Greater,
            false => Ordering::Less,
        });
        let min = &sorted_set[0];
        let max = &sorted_set[sorted_set.len() - 1];
        let mean = total.div_float(sorted_set.len() as f64).unwrap();
        let mean_f = match mean {
            Cell::Float(m) => m,
            _ => 0.0,
        };
        df.add_cell_col("mean".to_string(), vec![mean]).unwrap();
        let sum_sqred_diffs: f64 = self
            .values
            .iter()
            .map(|cell| {
                (match cell {
                    Cell::Int(val) => *val as f64,
                    Cell::Uint(val) => *val as f64,
                    Cell::Float(val) => *val,
                    _ => 0.0,
                } - mean_f)
                    .powi(2)
            })
            .sum();
        let std = (sum_sqred_diffs / self.values.len() as f64).sqrt();
        df.add_cell_col("std".to_string(), vec![Cell::Float(std)])
            .unwrap();
        df.add_cell_col("min".to_string(), vec![min.clone()])
            .unwrap();
        match quartiles(&sorted_set) {
            Some((quart, med, sev_fifth)) => {
                df.add_cell_col("25%".to_string(), vec![quart]).unwrap();
                df.add_cell_col("50%".to_string(), vec![med]).unwrap();
                df.add_cell_col("75%".to_string(), vec![sev_fifth]).unwrap();
            }
            None => {
                df.add_cell_col("25%".to_string(), vec![null_float()])
                    .unwrap();
                df.add_cell_col("50%".to_string(), vec![null_float()])
                    .unwrap();
                df.add_cell_col("75%".to_string(), vec![null_float()])
                    .unwrap();
            }
        };
        df.add_cell_col("max".to_string(), vec![max.clone()])
            .unwrap();
        df
    }
}

fn null_float() -> Cell {
    Cell::Null(Box::new(Cell::Float(0.0)))
}

fn quartiles(sorted_set: &Vec<Cell>) -> Option<(Cell, Cell, Cell)> {
    match sorted_set.len() {
        0 => None,
        1 => Some((
            sorted_set[0].clone(),
            sorted_set[0].clone(),
            sorted_set[0].clone(),
        )),
        _ => {
            let med_idx = sorted_set.len() as f64 / 2.0;
            let med = match med_idx.floor() {
                med_idx => sorted_set
                    .get(med_idx as usize)?
                    .add(sorted_set.get(1 + med_idx as usize)?)?
                    .div_float(2.0)?,
                _ => sorted_set.get(med_idx.ceil() as usize)?.clone(),
            };
            let quart_idx = sorted_set.len() as f64 / 4.0;
            let (quart, sev_fifth) = match quart_idx.floor() {
                quart_idx => (
                    sorted_set
                        .get(quart_idx as usize)?
                        .add(sorted_set.get(1 + quart_idx as usize)?)?
                        .div_float(2.0)?,
                    sorted_set
                        .get((quart_idx * 3.0) as usize)?
                        .add(sorted_set.get(1 + (quart_idx * 3.0) as usize)?)?
                        .div_float(2.0)?,
                ),
                _ => (
                    sorted_set.get(quart_idx.ceil() as usize)?.clone(),
                    sorted_set.get((quart_idx * 3.0).ceil() as usize)?.clone(),
                ),
            };
            Some((quart, med, sev_fifth))
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ColSlice<'a> {
    name: &'a str,
    values: &'a [Cell],
    typed: &'a Cell,
}

impl<'a> ColSlice<'a> {
    pub fn new(name: &'a str, values: &'a [Cell], typed: &'a Cell) -> Self {
        ColSlice {
            name,
            values,
            typed,
        }
    }
    pub fn values(&self) -> &'a [Cell] {
        &self.values
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn typed(&self) -> &Cell {
        &self.typed
    }
    pub fn empty_from(&self) -> Col {
        Col {
            name: self.name.to_string(),
            values: vec![],
            typed: self.typed.clone(),
        }
    }
}

impl<'a> From<&'a Col> for ColSlice<'a> {
    fn from(col: &'a Col) -> Self {
        ColSlice {
            name: &col.name,
            values: &col.values[..],
            typed: &col.typed,
        }
    }
}

// TODO
#[derive(Debug, PartialEq)]
pub struct ColSliceMut<'a> {
    name: &'a str,
    values: &'a [&'a mut Cell],
    typed: &'a Cell,
}

impl<'a> ColSliceMut<'a> {
    pub fn new(name: &'a str, values: &'a [&'a mut Cell], typed: &'a Cell) -> Self {
        ColSliceMut {
            name,
            values,
            typed,
        }
    }
    pub fn values(&self) -> &'a [&'a mut Cell] {
        self.values
    }
    pub fn name(&self) -> &str {
        self.name
    }
}
