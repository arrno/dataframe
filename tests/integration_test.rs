use dataframe_macros::ToRow;
use serde::Deserialize;

use dataframe::cell::*;
use dataframe::dataframe::*;
use dataframe::expression::*;
use dataframe::row;
use dataframe::row::ToRow;

fn generic_dataframe() -> Dataframe {
    Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(4, "Sally", 23, 700, true),
            row!(1, "Jasper", 41, 900, false),
            row!(5, "Jake", 33, 1200, true),
            row!(2, "Susie", 27, 200, true),
            row!(3, "Spruce", 24, 800, false),
        ],
    )
    .unwrap()
}
fn dataframe_extension() -> Dataframe {
    Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(6, "Sasha", 33, 1600, false),
            row!(7, "Jane", 24, 700, true),
            row!(8, "Jerry", 39, 400, true),
        ],
    )
    .unwrap()
}
fn option_dataframe() -> Dataframe {
    Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(6, "Sasha", None::<i64>, 1600, Some(false)),
            row!(7, "Jane", Some(24), 700, None::<bool>),
            row!(8, "Jerry", None::<i64>, 400, Some(true)),
        ],
    )
    .unwrap()
}
fn alt_dataframe() -> Dataframe {
    Dataframe::from_rows(
        vec!["id", "snack", "count"],
        vec![
            row!(1, "Apple", 1),
            row!(2, "Pretzels", 12),
            row!(2, "Banana", 1),
            row!(3, "Peanut", 20),
            row!(4, "Banana", 1),
            row!(5, "Chips", 12),
            row!(6, "Orange", 1),
        ],
    )
    .unwrap()
}
fn time_dataframe() -> Dataframe {
    Dataframe::from_rows(
        vec!["id", "name", "at"],
        vec![
            row!(2, "Noon", Timestamp(2024, 8, 26, 12, 15, 0)),
            row!(3, "Night", Timestamp(2024, 8, 26, 22, 45, 0)),
            row!(1, "Morning", Timestamp(2024, 8, 26, 8, 5, 0)),
        ],
    )
    .unwrap()
}

#[test]
fn slice_dataframe() {
    let df = generic_dataframe();
    // Slice by row
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(1, "Jasper", 41, 900, false),
            row!(5, "Jake", 33, 1200, true),
            row!(2, "Susie", 27, 200, true),
        ],
    )
    .unwrap();
    assert_eq!(df.slice(1, 4).unwrap().to_dataframe(), expected_df);

    // Slice by column
    let expected_df = Dataframe::from_rows(
        vec!["name", "age", "registered"],
        vec![
            row!("Sally", 23, true),
            row!("Jasper", 41, false),
            row!("Jake", 33, true),
            row!("Susie", 27, true),
            row!("Spruce", 24, false),
        ],
    )
    .unwrap();
    assert_eq!(
        df.col_slice(["name", "age", "registered"].into())
            .unwrap()
            .to_dataframe(),
        expected_df
    );
}

#[test]
fn apply_dataframe() {
    let mut df = dataframe_extension();
    df.col_mut("id").unwrap().iter_mut().for_each(|cell| {
        if let Cell::Int(val) = cell {
            *val *= 2
        }
    });
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(12, "Sasha", 33, 1600, false),
            row!(14, "Jane", 24, 700, true),
            row!(16, "Jerry", 39, 400, true),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);
}

#[test]
fn filter_dataframe() {
    // complex expressions
    let df = generic_dataframe()
        .filter(or(vec![
            and(vec![exp("id", gt(), 2), exp("score", lt(), 1000)]),
            exp("registered", eq(), false),
        ]))
        .unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(4, "Sally", 23, 700, true),
            row!(1, "Jasper", 41, 900, false),
            row!(3, "Spruce", 24, 800, false),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    // multi filter same col
    let df = generic_dataframe()
        .filter(and(vec![exp("id", gt(), 2), exp("id", lt(), 4)]))
        .unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![row!(3, "Spruce", 24, 800, false)],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    // mod
    let df = generic_dataframe().filter(exp("id", modl(2), 0)).unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(4, "Sally", 23, 700, true),
            row!(2, "Susie", 27, 200, true),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    // regex
    let df = generic_dataframe()
        .filter(exp("name", regx(), "^J"))
        .unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(1, "Jasper", 41, 900, false),
            row!(5, "Jake", 33, 1200, true),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);
}

#[test]
fn concat_dataframe() {
    let mut df = generic_dataframe();
    let concat_df = dataframe_extension();
    df.concat(concat_df).unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(4, "Sally", 23, 700, true),
            row!(1, "Jasper", 41, 900, false),
            row!(5, "Jake", 33, 1200, true),
            row!(2, "Susie", 27, 200, true),
            row!(3, "Spruce", 24, 800, false),
            row!(6, "Sasha", 33, 1600, false),
            row!(7, "Jane", 24, 700, true),
            row!(8, "Jerry", 39, 400, true),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);
}

#[test]
fn join_dataframe() {
    let df = generic_dataframe();
    let result_df = df.join(&alt_dataframe(), ("id", "id")).unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered", "snack", "count"],
        vec![
            row!(4, "Sally", 23, 700, true, "Banana", 1),
            row!(1, "Jasper", 41, 900, false, "Apple", 1),
            row!(5, "Jake", 33, 1200, true, "Chips", 12),
            row!(2, "Susie", 27, 200, true, "Pretzels", 12),
            row!(2, "Susie", 27, 200, true, "Banana", 1),
            row!(3, "Spruce", 24, 800, false, "Peanut", 20),
        ],
    )
    .unwrap();
    assert_eq!(result_df, expected_df);
}

#[test]
fn sort_dataframe() {
    let mut df = generic_dataframe();
    df.sort("id", desc()).unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(5, "Jake", 33, 1200, true),
            row!(4, "Sally", 23, 700, true),
            row!(3, "Spruce", 24, 800, false),
            row!(2, "Susie", 27, 200, true),
            row!(1, "Jasper", 41, 900, false),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    // asc by timestamp
    let mut df = time_dataframe();
    df.sort("at", asc()).unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "at"],
        vec![
            row!(1, "Morning", Timestamp(2024, 8, 26, 8, 5, 0)),
            row!(2, "Noon", Timestamp(2024, 8, 26, 12, 15, 0)),
            row!(3, "Night", Timestamp(2024, 8, 26, 22, 45, 0)),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);
}

#[test]
fn opt_dataframe() {
    // Not Null
    let mut df = option_dataframe();
    let df = df.filter(exp("age", neq(), None::<i64>)).unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![row!(7, "Jane", Some(24), 700, None::<bool>)],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    // Is Null
    let mut df = option_dataframe();
    let df = df.filter(exp("age", eq(), None::<i64>)).unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(6, "Sasha", None::<i64>, 1600, Some(false)),
            row!(8, "Jerry", None::<i64>, 400, Some(true)),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);
}

#[derive(Deserialize, ToRow)]
struct MyRow {
    name: String,
    age: i64,
    val: bool,
}

#[test]
fn csv_dataframe() {
    let df = Dataframe::from_csv::<MyRow>("./tests/test.csv").unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["name", "age", "val"],
        vec![
            row!("Jake", 23, true),
            row!("Sally", 44, false),
            row!("Jasper", 61, true),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);
}
