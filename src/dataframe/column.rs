use crate::util::Error;
use crate::{cell::*, dataframe::Dataframe};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

#[derive(Debug, PartialEq, Clone)]
pub struct Col {
    name: String,
    values: Vec<Cell>,
    typed: Cell,
    type_check: fn(&Cell) -> bool,
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
            type_check: cell_to_type_check(&z),
            typed: z,
        }
    }
    pub fn build(name: String, values: Vec<Cell>, typed: Cell) -> Self {
        let type_check = cell_to_type_check(&typed);
        Col {
            name,
            values,
            typed,
            type_check,
        }
    }
    pub fn apply(&mut self, f: fn(x: &mut Cell)) -> Result<(), Error> {
        for cell in self.values.iter_mut() {
            let mut new_cell = cell.clone();
            f(&mut new_cell);
            if (self.type_check)(&new_cell) {
                *cell = new_cell
            } else {
                return Err(Error::new("Invalid cell type".to_string()));
            }
        }
        Ok(())
    }
    pub fn check_type(&self, cell: &Cell) -> bool {
        (self.type_check)(cell)
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
    pub fn rename(&mut self, new_name: String) {
        self.name = new_name
    }
    pub fn typed(&self) -> &Cell {
        &self.typed
    }
    pub fn empty_from(&self) -> Col {
        Col {
            name: self.name.clone(),
            values: vec![],
            typed: self.typed.clone(),
            type_check: cell_to_type_check(&self.typed),
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
        let (_, unique, first_index, most) = self.top().unwrap();
        self.describe_with(vec![
            Some(self.values.len() as f64),
            None::<f64>,
            None::<f64>,
            None::<f64>,
            None::<f64>,
            None::<f64>,
            None::<f64>,
            None::<f64>,
            Some(unique),
            Some(first_index),
            Some(most),
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
            Cell::Float(val) => (val * 100.0).round() / 100.0,
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
                } - mean)
                    .powi(2)
            })
            .sum();
        let std = match sorted_set.len() {
            1 => None::<f64>,
            _ => {
                Some(((sum_sqred_diffs / self.values.len() as f64).sqrt() * 100.0).round() / 100.0)
            }
        };
        let (quart, med, sev_fifth) = match quartiles(&sorted_set) {
            Some((quart, med, sev_fifth)) => (Some(quart), Some(med), Some(sev_fifth)),
            None => (None::<f64>, None::<f64>, None::<f64>),
        };
        self.describe_with(vec![
            Some(self.values.len() as f64),
            Some(mean),
            std,
            Some(min.to_float_val()),
            quart,
            med,
            sev_fifth,
            Some(max.to_float_val()),
            None::<f64>,
            None::<f64>,
            None::<f64>,
        ])
    }

    fn describe_with(&self, values: Vec<Option<f64>>) -> Dataframe {
        let col = match values.len() {
            0 => vec![
                Some(0.0),
                None::<f64>,
                None::<f64>,
                None::<f64>,
                None::<f64>,
                None::<f64>,
                None::<f64>,
                None::<f64>,
                None::<f64>,
                None::<f64>,
                None::<f64>,
            ],
            _ => values,
        };
        let mut df = Dataframe::new(None);
        df.add_col(
            "::",
            vec![
                "count", "mean", "std", "min", "25%", "50%", "75%", "max", "unique", "top idx",
                "freq",
            ],
        )
        .unwrap();
        df.add_col(self.name.as_str(), col).unwrap();
        df
    }

    pub fn count(&self) -> usize {
        self.values.len()
    }
    pub fn sum(&self) -> Option<f64> {
        match self.typed.is_num() {
            true => Some(self.values.iter().map(|cell| cell.to_float_val()).sum()),
            false => None,
        }
    }
    pub fn product(&self) -> Option<f64> {
        match self.typed.is_num() {
            true => Some(self.values.iter().map(|cell| cell.to_float_val()).product()),
            false => None,
        }
    }
    pub fn mean(&self) -> Option<f64> {
        match self.typed.is_num() {
            true => Some(
                (self
                    .values
                    .iter()
                    .map(|cell| cell.to_float_val())
                    .sum::<f64>()
                    / self.values.len() as f64
                    * 100.0)
                    .round()
                    / 100.0,
            ),
            false => None,
        }
    }
    pub fn max(&self) -> Option<f64> {
        match self.typed.is_num() {
            true => self
                .values
                .iter()
                .map(|cell| cell.to_float_val())
                .max_by(|x, y| x.total_cmp(y)),
            false => None,
        }
    }
    pub fn min(&self) -> Option<f64> {
        match self.typed.is_num() {
            true => self
                .values
                .iter()
                .map(|cell| cell.to_float_val())
                .min_by(|x, y| x.total_cmp(y)),
            false => None,
        }
    }
    pub fn top(&self) -> Option<(Cell, f64, f64, f64)> {
        if self.values.len() == 0 {
            return None;
        }
        let mut freq: HashMap<String, usize> = HashMap::new();
        let mut first_index: HashMap<String, usize> = HashMap::new();
        let mut most = 0;
        let mut top = &self.typed.zero();
        self.values.iter().enumerate().for_each(|(i, cell)| {
            let val = freq.entry(cell.as_string()).or_insert(0);
            *val += 1;
            first_index.entry(cell.as_string()).or_insert(i);
            if *val > most {
                most = *val;
                top = cell;
            }
        });
        Some((
            top.clone(),                                        // top val
            freq.len() as f64,                                  // unique
            *first_index.get(&top.as_string()).unwrap() as f64, // top val idx
            most as f64,                                        // top count
        ))
    }
    pub fn unique(&self) -> usize {
        self.values
            .iter()
            .map(|cell| cell.as_string())
            .collect::<HashSet<String>>()
            .len()
    }
    pub fn coalesce(&self) -> Option<&Cell> {
        self.values.iter().find(|cell| !cell.is_null())
    }
    pub fn non_null(&self) -> usize {
        self.values.iter().map(|cell| !cell.is_null()).count()
    }
}

fn median(sorted_set: &[Cell]) -> Option<f64> {
    let med_idx = sorted_set.len() as f64 / 2.0;
    let med = if med_idx.floor() == med_idx {
        sorted_set
            .get(med_idx as usize)?
            .add_int(sorted_set.get(med_idx as usize - 1)?)?
            .div_float(2.0)?
    } else {
        sorted_set.get(med_idx.floor() as usize)?.to_float()
    };
    Some(med.to_float_val())
}
fn quartiles(sorted_set: &Vec<Cell>) -> Option<(f64, f64, f64)> {
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
            type_check: cell_to_type_check(&self.typed),
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
