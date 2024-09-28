use crate::{dataframe::Dataframe, dataslice::DataSlice, util::Error};

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
    pub fn collect(&self) -> Dataframe {
        // chunk df via by column
        // each by column is a row
        // each select is a column for the reduced chunk val
        Dataframe::new(None)
    }
}
