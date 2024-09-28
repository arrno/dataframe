use std::collections::HashMap;

use crate::{
    cell::{self, Cell, ToCell},
    col::Col,
    dataframe::Dataframe,
    dataslice::DataSlice,
    util::Error,
};

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

// struct Transformer {
//     Reducers: HashMap<Reducer, dyn FnOnce(Col) -> Cell>
// };

// impl Transformer {
//     pub fn new() -> Transformer {
//         let mut map = HashMap::new()
//         map.insert();
//         Transformer{
//             Reducers: map
//         }
//     }
// }

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
                            match &select.reducer {
                                Reducer::Count => cell::Cell::Uint(col.count() as u64),
                                Reducer::Max => col.max().unwrap().to_cell(),
                                Reducer::Min => col.min().unwrap().to_cell(),
                                Reducer::Sum => col.sum().unwrap().to_cell(),
                                Reducer::Mean => col.mean().unwrap().to_cell(),
                                Reducer::Top => match col.top() {
                                    Some((cell, _, _, _)) => cell,
                                    None => col.typed().null(),
                                },
                                Reducer::Unique => cell::Cell::Uint(col.unique() as u64),
                                Reducer::Coalesce => col.coalesce().unwrap().clone(),
                                Reducer::NonNull => cell::Cell::Uint(col.non_null() as u64),
                                Reducer::Prod => col.product().unwrap().to_cell(),
                            }
                        })
                        .collect::<Vec<Cell>>()
                })
                .collect::<Vec<Vec<Cell>>>(),
        )
        .unwrap()
    }
}
