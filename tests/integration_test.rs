use dataframe::cell::*;
use dataframe::dataframe::*;
use dataframe::expression::*;
use dataframe::row;

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
            row!(3, "Peanut", 20),
            row!(4, "Banana", 1),
            row!(5, "Chips", 12),
        ],
    )
    .unwrap()
}
#[test]
fn slice_dataframe() {
    let mut df = generic_dataframe();
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

// fn apply_dataframe() {}
fn filter_dataframe() {
    let mut df = generic_dataframe();
    df = df
        .filter(ExpAnd(vec![Exp("nums", Gt(), 3), Exp("nums", Lt(), 7)]))
        .unwrap();
    df.print();
}
// fn concat_dataframe() {}
// fn join_dataframe() {}
// fn sort_dataframe() {}
