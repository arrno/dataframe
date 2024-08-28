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

// #[macro_export]
// macro_rules! zoom_and_enhance {
//     (struct $name:ident { $($fname:ident : $ftype:ty),* }) => {
//         struct $name {
//             $($fname : $ftype),*
//         }

//         impl $name {
//             fn field_names() -> &'static [&'static str] {
//                 static NAMES: &'static [&'static str] = &[$(stringify!($fname)),*];
//                 NAMES
//             }
//         }

//         impl ToRow for $name {
//             fn to_row(self) -> Vec<Cell> {

//             }
//         }
//     }
// }
