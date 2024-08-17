use crate::cell::*;

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
}

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
