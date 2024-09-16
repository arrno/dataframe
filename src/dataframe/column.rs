use crate::{cell::*, dataframe::Dataframe};

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
    // numeric only
    pub fn describe(&self) -> Dataframe {
        // if not numeric do: [count, unique, top, freq]
        let mut df = Dataframe::new(None);
        df.add_col::<u64>("count", vec![self.values.len() as u64])
            .unwrap();
        let mut min = match self.values.get(0) {
            Some(cell) => cell,
            None => &self.typed().zero(),
        };
        let mut max = min;
        let mut total = min.clone();
        self.values.iter().for_each(|cell| {
            if cell < min {
                min = cell
            }
            if cell > max {
                max = cell
            }
            if let Some(c) = total.add(cell) {
                total = c
            }
        });
        df.add_cell_col(
            "mean".to_string(),
            vec![total.div_float(self.values.len() as f64).unwrap()],
        )
        .unwrap();
        // std
        df.add_cell_col("min".to_string(), vec![min.clone()])
            .unwrap();
        // 25%
        // 50%
        // 75%
        df.add_cell_col("max".to_string(), vec![max.clone()])
            .unwrap();
        df
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
