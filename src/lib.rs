pub mod cell;
mod column;
pub mod dataframe;
mod dataslice;
pub mod expression;
mod format;
mod join;
mod row;
mod util;

// use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
// use std::collections::HashMap;

// use crate::cell::*;
// use crate::dataframe::*;
// use crate::expression::*;
// use crate::row::*;

// pub mod datatable {
//     use crate::dataframe::*;
// }

// pub fn main() {
//     let df = Dataframe::from_rows(
//         vec!["Name", "Age", "Score"],
//         vec![
//             row!("Jasper", 10, 89),
//             row!("Jake", 20, 11),
//             row!("Susan", 44, 27),
//             row!("Sally", 72, 109),
//         ],
//     );
// }

// fn do_dataframe() {
//     let mut df = Dataframe::new(String::from("Raw Data"));
//     df.add_col("nums".to_string(), Vec::from([0, 1, 2, 3, 4, 5, 6, 7, 8]))
//         .unwrap();
//     df.add_col(
//         "more nums".to_string(),
//         Vec::from([9, 10, 11, 12, 13, 14, 15, 16, 17]),
//     )
//     .unwrap();
//     df.add_opt_col(
//         "the best nums".to_string(),
//         Vec::from([
//             Some(-10),
//             None,
//             Some(200),
//             Some(400),
//             Some(777),
//             Some(-289),
//             Some(7),
//             Some(12),
//             Some(902),
//         ]),
//     )
//     .unwrap();
//     df.add_col(
//         "strangs".to_string(),
//         Vec::from([
//             "woop!".to_string(),
//             "Hello".to_string(),
//             "dope man".to_string(),
//             "cool boi".to_string(),
//             "wspwspwsp".to_string(),
//             ":-)".to_string(),
//             "Who's that daddy?".to_string(),
//             "Snarg".to_string(),
//             "NaNaNaN".to_string(),
//         ]),
//     )
//     .unwrap();

//     df.col_mut("nums".to_string())
//         .unwrap()
//         .iter_mut()
//         .for_each(|c| {
//             if let Cell::Int(x) = c {
//                 *x += 2
//             }
//         });
//     df.head().unwrap();

//     let f_df = df
//         .filter(Exp::And(And::new(vec![
//             Exp::ExpU(ExpU::new("nums".to_string(), Op::Gt, 3)),
//             Exp::ExpU(ExpU::new("the best nums".to_string(), Op::NotNull, 0)),
//         ])))
//         .unwrap();
//     f_df.print();

//     let col_slice = f_df.col_slice(["nums", "strangs"].into()).unwrap();
//     col_slice.print();

//     let mut second_df = Dataframe::new(String::from("Raw Data"));
//     second_df
//         .add_col("nums".to_string(), Vec::from([0, 1, 2, 3, 4, 5, 6, 7, 8]))
//         .unwrap();
//     second_df
//         .add_col(
//             "sweet strangs".to_string(),
//             Vec::from([
//                 "sweet".to_string(),
//                 "sweetie".to_string(),
//                 "dulce".to_string(),
//                 "sugar pop".to_string(),
//                 "honey-comb".to_string(),
//                 "candy coat".to_string(),
//                 "sugar".to_string(),
//                 "caramel".to_string(),
//                 "syrup".to_string(),
//             ]),
//         )
//         .unwrap();

//     let mut third_df = Dataframe::new(String::from("Raw Data"));
//     third_df
//         .add_col("nums".to_string(), Vec::from([9, 10]))
//         .unwrap();
//     third_df
//         .add_col(
//             "sweet strangs".to_string(),
//             Vec::from(["corn".to_string(), "grain".to_string()]),
//         )
//         .unwrap();
//     second_df.concat(third_df).unwrap();

//     let join_df = df.join(&second_df, "nums").unwrap();
//     join_df.print();

//     let df = Dataframe::from_to_rows(
//         vec!["one", "two", "three"],
//         vec![
//             Person("Jasper".to_string(), 10, 89),
//             Person("Jake".to_string(), 20, 11),
//             Person("Susan".to_string(), 44, 27),
//             Person("Sally".to_string(), 72, 109),
//         ],
//     );

//     // TEMNO

//     let mut df = Dataframe::new(String::from("Raw Data"));
//     df.add_col(
//         "strangs".to_string(),
//         Vec::from([
//             "sugar".to_string(),
//             "sweets".to_string(),
//             "candy pop".to_string(),
//             "caramel".to_string(),
//             "chocolate".to_string(),
//             ":-)".to_string(),
//             "Who's that daddy?".to_string(),
//             "Snarg".to_string(),
//             "NaNaNaN".to_string(),
//         ]),
//     )
//     .unwrap();
//     df.add_col("nums".to_string(), Vec::from([0, 1, 2, 3, 4, 5, 6, 7, 8]))
//         .unwrap();
//     df.add_opt_col(
//         "null nums".to_string(),
//         Vec::from([
//             Some(-10),
//             None,
//             Some(200),
//             Some(400),
//             Some(777),
//             Some(-289),
//             Some(7),
//             Some(12),
//             Some(902),
//         ]),
//     )
//     .unwrap();
//     df.print();
//     df.sort("nums", SortOrder::Desc).unwrap();
//     df.print();
// }

// fn Person(name: String, age: u32, size: i64) -> MyStruct {
//     MyStruct { name, age, size }
// }
// struct MyStruct {
//     name: String,
//     age: u32,
//     size: i64,
// }

// impl ToRow for MyStruct {
//     fn to_row(self) -> Vec<Cell> {
//         vec![self.name.into(), self.age.into(), self.size.into()]
//     }
// }
