mod cell;
mod dataframe;
mod expression;
mod format;
mod join;
mod util;

use crate::cell::*;
use crate::dataframe::*;
use crate::expression::*;

pub fn main() {
    let mut df = Dataframe::new(String::from("Raw Data"));
    df.add_col("nums".to_string(), Vec::from([0, 1, 2, 3, 4, 5, 6, 7, 8]))
        .unwrap();
    df.add_col(
        "more nums".to_string(),
        Vec::from([9, 10, 11, 12, 13, 14, 15, 16, 17]),
    )
    .unwrap();
    df.add_opt_col(
        "the best nums".to_string(),
        Vec::from([
            Some(-10),
            None,
            Some(200),
            Some(400),
            Some(777),
            Some(-289),
            Some(7),
            Some(12),
            Some(902),
        ]),
    )
    .unwrap();
    df.add_col(
        "strangs".to_string(),
        Vec::from([
            "woop!".to_string(),
            "Hello".to_string(),
            "dope man".to_string(),
            "cool boi".to_string(),
            "wspwspwsp".to_string(),
            ":-)".to_string(),
            "Who's that daddy?".to_string(),
            "Snarg".to_string(),
            "NaNaNaN".to_string(),
        ]),
    )
    .unwrap();

    df.col_mut("nums".to_string())
        .unwrap()
        .iter_mut()
        .for_each(|c| {
            if let Cell::Int(x) = c {
                *x += 2
            }
        });
    df.head().unwrap();

    let f_df = df
        .filter(Exp::And(And::new(vec![
            Exp::ExpU(ExpU::new("nums".to_string(), Op::Gt, 3)),
            Exp::ExpU(ExpU::new("the best nums".to_string(), Op::NotNull, 0)),
        ])))
        .unwrap();
    f_df.print();
}
