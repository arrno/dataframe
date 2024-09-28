use crate::{column::Col, dataframe::Dataframe, util::Error};

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
struct Select<'a> {
    Col: &'a Col,
    Reducer: Reducer,
}
pub struct DataGroup<'a> {
    dataframe: Dataframe,
    by: String,
    selects: Vec<Select<'a>>,
}

impl<'a> DataGroup<'a> {
    pub fn new(df: Dataframe, by: String) -> Self {
        DataGroup {
            dataframe: df,
            by: by,
            selects: vec![],
        }
    }
    pub fn select(&'a mut self, column: &str, reducer: Reducer) -> Result<(), Error> {
        let select = match self
            .dataframe
            .columns()
            .iter()
            .find(|col| col.name() == column)
        {
            Some(col) => Select {
                Col: col,
                Reducer: reducer,
            },
            None => return Err(Error::new("Column not found.".to_string())),
        };
        self.selects.push(select);
        Ok(())
    }
    pub fn collect(&self) -> Dataframe {
        Dataframe::new(None)
    }
}
