use std::collections::HashMap;

use crate::{
    cell::{self, Cell, ToCell},
    column::Col,
    dataframe::Dataframe,
    dataslice::DataSlice,
    util::Error,
};

#[derive(Eq, PartialEq, Hash)]
pub enum Reducer {
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
impl Reducer {
    pub fn display(&self) -> String {
        match self {
            Self::Count => "c".to_string(),
            Self::Sum => "+".to_string(),
            Self::Prod => "*".to_string(),
            Self::Mean => "m".to_string(),
            Self::Min => "_".to_string(),
            Self::Max => "^".to_string(),
            Self::Top => "t".to_string(),
            Self::Unique => "u".to_string(),
            Self::Coalesce => "&".to_string(),
            Self::NonNull => "!".to_string(),
        }
    }
}
struct Select {
    column_name: String,
    reducer: Reducer,
}
pub struct DataGroup<'a> {
    slice: DataSlice<'a>,
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
    fn new() -> ReduceRouter {
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
    pub fn new(df: DataSlice<'a>, by: String) -> Self {
        DataGroup {
            slice: df,
            by: by,
            selects: vec![],
        }
    }
    pub fn select(mut self, column: &str, reducer: Reducer) -> Self {
        self.selects.push(Select {
            column_name: column.to_string(),
            reducer: reducer,
        });
        self
    }
    pub fn collect(self) -> Result<Dataframe, Error> {
        let rd_router = ReduceRouter::new();
        let name_indices = self
            .slice
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name(), i))
            .collect::<HashMap<&str, usize>>();
        Dataframe::from_string_rows(
            self.selects
                .iter()
                .map(|s| format!("{}\\ {}", s.reducer.display(), s.column_name.as_str()))
                .collect(),
            self.slice
                .chunk_by(&self.by)?
                .iter()
                .map(|df| {
                    self.selects
                        .iter()
                        .map(|select| {
                            if let Some(idx) = name_indices.get(select.column_name.as_str()) {
                                rd_router.0.get(&select.reducer).unwrap()(&df.columns()[*idx])
                            } else {
                                Cell::Null(Box::new(Cell::Float(0.0)))
                            }
                        })
                        .collect::<Vec<Cell>>()
                })
                .collect::<Vec<Vec<Cell>>>(),
        )
    }
}
