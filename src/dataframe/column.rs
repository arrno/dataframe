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

    pub fn describe(&self) -> Dataframe {
        match self.typed.is_num() {
            true => self.describe_numeric(),
            false => self.describe_object(),
        }
    }

    fn describe_object(&self) -> Dataframe {
        if self.values.len() == 0 {
            return self.describe_with(vec![]);
        }
        let mut freq: HashMap<String, usize> = HashMap::new();
        let mut first_index: HashMap<String, usize> = HashMap::new();
        let mut most = 0;
        let mut top = &self.typed;
        self.values.iter().enumerate().for_each(|(i, cell)| {
            let val = freq.entry(cell.as_string()).or_insert(0);
            *val += 1;
            first_index.entry(cell.as_string()).or_insert(i);
            if *val > most {
                most = *val;
                top = cell;
            }
        });
        self.describe_with(vec![
            Cell::Float(self.values.len() as f64),
            null_float(),
            null_float(),
            null_float(),
            null_float(),
            null_float(),
            null_float(),
            null_float(),
            Cell::Float(freq.len() as f64),
            Cell::Float(*first_index.get(&top.as_string()).unwrap() as f64),
            Cell::Float(most as f64),
        ])
    }

    fn describe_numeric(&self) -> Dataframe {
        if self.values.len() == 0 {
            return self.describe_with(vec![]);
        }
        let mut total = self.typed.zero();
        let mut sorted_set: Vec<Cell> = self
            .values
            .iter()
            .filter(|cell| !cell.is_null())
            .map(|cell| {
                if let Some(c) = total.add_int(cell) {
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
        let mean = match total.div_float(sorted_set.len() as f64).unwrap() {
            Cell::Float(val) => Cell::Float((val * 100.0).round() / 100.0),
            _ => null_float(),
        };
        let mean_f = match mean {
            Cell::Float(m) => m,
            _ => 0.0,
        };
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
        let std = match sorted_set.len() {
            1 => null_float(),
            _ => Cell::Float(
                ((sum_sqred_diffs / self.values.len() as f64).sqrt() * 100.0).round() / 100.0,
            ),
        };
        let (quart, med, sev_fifth) = match quartiles(&sorted_set) {
            Some((quart, med, sev_fifth)) => (quart, med, sev_fifth),
            None => (null_float(), null_float(), null_float()),
        };
        self.describe_with(vec![
            Cell::Float(self.values.len() as f64),
            mean,
            std,
            min.to_float(),
            quart,
            med,
            sev_fifth,
            max.to_float(),
            null_float(),
            null_float(),
            null_float(),
        ])
    }

    fn describe_with(&self, values: Vec<Cell>) -> Dataframe {
        let col = match values.len() {
            0 => vec![
                Cell::Float(0.0),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
                null_float(),
            ],
            _ => values,
        };
        let mut df = Dataframe::new(None);
        df.add_cell_col(
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
        )
        .unwrap();
        df.add_cell_col(self.name.clone(), col).unwrap();
        df
    }
}

fn null_float() -> Cell {
    Cell::Null(Box::new(Cell::Float(0.0)))
}

fn median(sorted_set: &[Cell]) -> Option<Cell> {
    let med_idx = sorted_set.len() as f64 / 2.0;
    let med = if med_idx.floor() == med_idx {
        sorted_set
            .get(med_idx as usize)?
            .add_int(sorted_set.get(med_idx as usize - 1)?)?
            .div_float(2.0)?
    } else {
        sorted_set.get(med_idx.floor() as usize)?.clone()
    };
    Some(med)
}
fn quartiles(sorted_set: &Vec<Cell>) -> Option<(Cell, Cell, Cell)> {
    match sorted_set.len() {
        0..=3 => None,
        _ => {
            let med_idx = sorted_set.len() as f64 / 2.0;
            let med = median(&sorted_set)?;
            let (quart, sev_fifth) = (
                median(&sorted_set[0..med_idx.floor() as usize])?,
                median(&sorted_set[med_idx.ceil() as usize..sorted_set.len()])?,
            );
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
