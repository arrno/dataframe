use std::collections::HashMap;

use crate::{
    cell::{self, Cell, ToCell},
    column::Col,
    dataframe::Dataframe,
    dataslice::DataSlice,
    util::Error,
};

#[derive(Eq, PartialEq, Hash)]
enum Reducer {
    Count,
    Sum,
    Prod,
    Mean,
    Min,
    Max,
    Top,
    Unique,
    Coalesce,
    NonNull,
}
struct Select {
    column_name: String,
    reducer: Reducer,
}
pub struct DataGroup<'a> {
    dataslice: DataSlice<'a>,
    by: String,
    selects: Vec<Select>,
}

struct ReduceRouter(HashMap<Reducer, fn(&Col) -> cell::Cell>);

fn count(col: &Col) -> Cell {
    Cell::Uint(col.count() as u64)
}
fn sum(col: &Col) -> Cell {
    col.sum().unwrap().to_cell()
}
fn prod(col: &Col) -> Cell {
    col.product().unwrap().to_cell()
}
fn mean(col: &Col) -> Cell {
    col.mean().unwrap().to_cell()
}
fn min(col: &Col) -> Cell {
    col.min().unwrap().to_cell()
}
fn max(col: &Col) -> Cell {
    col.max().unwrap().to_cell()
}
fn top(col: &Col) -> Cell {
    match col.top() {
        Some((cell, _, _, _)) => cell,
        None => col.typed().null(),
    }
}
fn unique(col: &Col) -> Cell {
    cell::Cell::Uint(col.unique() as u64)
}
fn coalesce(col: &Col) -> Cell {
    col.coalesce().unwrap().clone()
}
fn non_null(col: &Col) -> Cell {
    cell::Cell::Uint(col.non_null() as u64)
}

impl ReduceRouter {
    pub fn new() -> ReduceRouter {
        let mut map = HashMap::new();
        map.insert(Reducer::Count, count as fn(&Col) -> cell::Cell);
        map.insert(Reducer::Sum, sum as fn(&Col) -> cell::Cell);
        map.insert(Reducer::Prod, prod as fn(&Col) -> cell::Cell);
        map.insert(Reducer::Mean, mean as fn(&Col) -> cell::Cell);
        map.insert(Reducer::Min, min as fn(&Col) -> cell::Cell);
        map.insert(Reducer::Max, max as fn(&Col) -> cell::Cell);
        map.insert(Reducer::Top, top as fn(&Col) -> cell::Cell);
        map.insert(Reducer::Unique, unique as fn(&Col) -> cell::Cell);
        map.insert(Reducer::Coalesce, coalesce as fn(&Col) -> cell::Cell);
        map.insert(Reducer::NonNull, non_null as fn(&Col) -> cell::Cell);
        ReduceRouter(map)
    }
}

impl<'a> DataGroup<'a> {
    pub fn new(df: DataSlice<'a>, by: String) -> Result<Self, Error> {
        match df.columns().iter().find(|col| col.name() == by) {
            Some(_) => Ok(DataGroup {
                dataslice: df,
                by: by,
                selects: vec![],
            }),
            None => Err(Error::new("Groupby column not found".to_string())),
        }
    }
    pub fn select(&'a mut self, column: &str, reducer: Reducer) -> Result<(), Error> {
        let select = match self
            .dataslice
            .columns()
            .iter()
            .map(|col| col.name())
            .find(|name| name == &column)
        {
            Some(name) => Select {
                column_name: name.to_string(),
                reducer: reducer,
            },
            None => return Err(Error::new("Column not found".to_string())),
        };
        self.selects.push(select);
        Ok(())
    }
    pub fn collect(self) -> Dataframe {
        let red_rout = ReduceRouter::new();
        let name_indices = self
            .dataslice
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name(), i))
            .collect::<HashMap<&str, usize>>();
        Dataframe::from_rows(
            self.selects
                .iter()
                .map(|s| s.column_name.as_str())
                .collect(),
            self.dataslice
                .chunk_by(&self.by)
                .unwrap()
                .iter()
                .map(|(_, df)| {
                    self.selects
                        .iter()
                        .map(|select| {
                            let col = &df.columns()
                                [*name_indices.get(select.column_name.as_str()).unwrap()];
                            red_rout.0.get(&select.reducer).unwrap()(col)
                        })
                        .collect::<Vec<Cell>>()
                })
                .collect::<Vec<Vec<Cell>>>(),
        )
        .unwrap()
    }
}
