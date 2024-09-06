use crate::cell::*;

pub trait ToRow {
    fn to_row(&self) -> Vec<Cell>;
    fn labels(&self) -> Vec<String>;
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

// impl ToRow for MyRow {
//     fn to_row(&self) -> Vec<Cell> {
//         vec![self.name.as_str().into(), self.age.into(), self.val.into()]
//     }
//     fn labels(&self) -> Vec<String> {
//         vec!["name".to_string(), "age".to_string(), "val".to_string()]
//     }
// }
