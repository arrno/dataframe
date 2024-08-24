use crate::cell::*;

pub trait ToRow {
    fn to_row(self) -> Vec<Cell>;
}

#[macro_export]
macro_rules! row {
    ( $( $x:expr),* ) => {
        {
            let mut temp_vec: Vec<Cell> = Vec::new();
            $(
                temp_vec.push($x.into());
            )*
            temp_vec
        }
    };
}
