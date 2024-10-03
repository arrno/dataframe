use rowboat::dataframe::*;
use serde::Deserialize;
use std::collections::HashMap;

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
fn alt_dataframe_sparse() -> Dataframe {
    Dataframe::from_rows(
        vec!["id", "snack", "count"],
        vec![
            row!(1, "Apple", 1),
            row!(2, "Pretzels", 12),
            row!(2, "Banana", 1),
            row!(5, "Chips", 12),
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

    let mut_df = generic_dataframe();
    assert_eq!(mut_df.cell(2, "age").unwrap(), &33.to_cell());
}

#[test]
fn apply_dataframe() {
    let mut df = dataframe_extension();
    df.col_mut("id")
        .unwrap()
        .apply(|cell| {
            if let Cell::Int(val) = cell {
                *val *= 2
            }
        })
        .unwrap();
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
    df.update_val(2, "age", |cell| {
        if let Cell::Int(val) = cell {
            *val += 2;
        }
    })
    .unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(12, "Sasha", 33, 1600, false),
            row!(14, "Jane", 24, 700, true),
            row!(16, "Jerry", 41, 400, true),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);
    df.set_val(0, "name", "NEWNAME").unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(12, "NEWNAME", 33, 1600, false),
            row!(14, "Jane", 24, 700, true),
            row!(16, "Jerry", 41, 400, true),
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
            and(vec![exp("id", Gt, 2), exp("score", Lt, 1000)]),
            exp("registered", Eq, false),
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
        .filter(and(vec![exp("id", Gt, 2), exp("id", Lt, 4)]))
        .unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![row!(3, "Spruce", 24, 800, false)],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    // mod
    let df = generic_dataframe().filter(exp("id", Mod(2), 0)).unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(4, "Sally", 23, 700, true),
            row!(2, "Susie", 27, 200, true),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    let df = generic_dataframe()
        .filter(not(exp("id", Mod(2), 0)))
        .unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(1, "Jasper", 41, 900, false),
            row!(5, "Jake", 33, 1200, true),
            row!(3, "Spruce", 24, 800, false),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    // regex
    let df = generic_dataframe()
        .filter(exp("name", Regex, "^J"))
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
fn extend_dataframe() {
    let mut df = generic_dataframe();
    df.add_col(
        "age is even",
        df.column("age")
            .unwrap()
            .values()
            .iter()
            .map(|cell| match cell {
                Cell::Int(score) => Some(score % 2 == 0),
                _ => None::<bool>,
            })
            .collect(),
    )
    .unwrap();

    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered", "age is even"],
        vec![
            row!(4, "Sally", 23, 700, true, false),
            row!(1, "Jasper", 41, 900, false, false),
            row!(5, "Jake", 33, 1200, true, false),
            row!(2, "Susie", 27, 200, true, false),
            row!(3, "Spruce", 24, 800, false, true),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    df.add_col(
        "id and age odd",
        df.col_slice(["id", "age"].into())
            .unwrap()
            .iter()
            .map(|row| {
                let id_odd = match row.get("id").unwrap() {
                    Cell::Int(v) => v % 2 != 0,
                    _ => false,
                };
                let score_odd = match row.get("age").unwrap() {
                    Cell::Int(v) => v % 2 != 0,
                    _ => false,
                };
                id_odd && score_odd
            })
            .collect(),
    )
    .unwrap();

    let expected_df = Dataframe::from_rows(
        vec![
            "id",
            "name",
            "age",
            "score",
            "registered",
            "age is even",
            "id and age odd",
        ],
        vec![
            row!(4, "Sally", 23, 700, true, false, false),
            row!(1, "Jasper", 41, 900, false, false, true),
            row!(5, "Jake", 33, 1200, true, false, true),
            row!(2, "Susie", 27, 200, true, false, false),
            row!(3, "Spruce", 24, 800, false, true, false),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    df.add_row(row![4, "Sparce", 26, 850, true, true, false])
        .unwrap();
    assert_eq!(
        df,
        Dataframe::from_rows(
            vec![
                "id",
                "name",
                "age",
                "score",
                "registered",
                "age is even",
                "id and age odd"
            ],
            vec![
                row!(4, "Sally", 23, 700, true, false, false),
                row!(1, "Jasper", 41, 900, false, false, true),
                row!(5, "Jake", 33, 1200, true, false, true),
                row!(2, "Susie", 27, 200, true, false, false),
                row!(3, "Spruce", 24, 800, false, true, false),
                row![4, "Sparce", 26, 850, true, true, false],
            ],
        )
        .unwrap()
    );
    df.add_row(row![4, "Sparce", 26, 850, true, None::<bool>, false])
        .unwrap();
    assert_eq!(
        df,
        Dataframe::from_rows(
            vec![
                "id",
                "name",
                "age",
                "score",
                "registered",
                "age is even",
                "id and age odd"
            ],
            vec![
                row!(4, "Sally", 23, 700, true, false, false),
                row!(1, "Jasper", 41, 900, false, false, true),
                row!(5, "Jake", 33, 1200, true, false, true),
                row!(2, "Susie", 27, 200, true, false, false),
                row!(3, "Spruce", 24, 800, false, true, false),
                row![4, "Sparce", 26, 850, true, true, false],
                row![4, "Sparce", 26, 850, true, None::<bool>, false],
            ],
        )
        .unwrap()
    );
}

#[test]
fn join_dataframe() {
    let df = generic_dataframe();
    let result_df = df.join(&alt_dataframe_sparse(), "id", "id").unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered", "snack", "count"],
        vec![
            row!(1, "Jasper", 41, 900, false, "Apple", 1),
            row!(5, "Jake", 33, 1200, true, "Chips", 12),
            row!(2, "Susie", 27, 200, true, "Pretzels", 12),
            row!(2, "Susie", 27, 200, true, "Banana", 1),
        ],
    )
    .unwrap();
    assert_eq!(result_df, expected_df);

    let df = generic_dataframe();
    let result_df = df.left_join(&alt_dataframe_sparse(), "id", "id").unwrap();
    let expected_df = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered", "snack", "count"],
        vec![
            row!(4, "Sally", 23, 700, true, None::<String>, None::<i64>),
            row!(1, "Jasper", 41, 900, false, "Apple", 1),
            row!(5, "Jake", 33, 1200, true, "Chips", 12),
            row!(2, "Susie", 27, 200, true, "Pretzels", 12),
            row!(2, "Susie", 27, 200, true, "Banana", 1),
            row!(3, "Spruce", 24, 800, false, None::<String>, None::<i64>),
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
fn into_sort_dataframe() {
    let df = Dataframe::from_rows(
        vec!["one", "two", "three"],
        vec![
            row!("B", "A", "C"),
            row!("A", "C", "C"),
            row!("A", "B", "C"),
            row!("A", "B", "A"),
            row!("B", "C", "A"),
            row!("A", "C", "B"),
            row!("B", "C", "B"),
            row!("B", "A", "B"),
        ],
    )
    .unwrap()
    .into_sort()
    .sort("one", asc())
    .sort("two", asc())
    .sort("three", asc())
    .collect()
    .unwrap();

    let expected = Dataframe::from_rows(
        vec!["one", "two", "three"],
        vec![
            row!("A", "B", "A"),
            row!("A", "B", "C"),
            row!("A", "C", "B"),
            row!("A", "C", "C"),
            row!("B", "A", "B"),
            row!("B", "A", "C"),
            row!("B", "C", "A"),
            row!("B", "C", "B"),
        ],
    )
    .unwrap();
    assert_eq!(df, expected);
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
fn from_structs() {
    let df = Dataframe::from_structs(vec![
        MyRow {
            name: "Jake".to_string(),
            age: 23,
            val: true,
        },
        MyRow {
            name: "Sally".to_string(),
            age: 44,
            val: false,
        },
        MyRow {
            name: "Jasper".to_string(),
            age: 61,
            val: true,
        },
    ])
    .unwrap();
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

    df.to_csv("./tests/new_test.csv").unwrap();
}

#[test]
fn iterrows() {
    let df = dataframe_extension();
    df.iter().enumerate().for_each(|(i, row)| match i {
        0 => {
            let mut map: HashMap<String, &Cell> = HashMap::new();
            let name = "Sasha".to_cell();
            map.insert(String::from("id"), &Cell::Int(6));
            map.insert(String::from("name"), &name);
            map.insert(String::from("age"), &Cell::Int(33));
            map.insert(String::from("score"), &Cell::Int(1600));
            map.insert(String::from("registered"), &Cell::Bool(false));
            assert_eq!(row, map);
        }
        1 => {
            let mut map: HashMap<String, &Cell> = HashMap::new();
            let name = "Jane".to_cell();
            map.insert(String::from("id"), &Cell::Int(7));
            map.insert(String::from("name"), &name);
            map.insert(String::from("age"), &Cell::Int(24));
            map.insert(String::from("score"), &Cell::Int(700));
            map.insert(String::from("registered"), &Cell::Bool(true));
            assert_eq!(row, map);
        }
        2 => {
            let mut map: HashMap<String, &Cell> = HashMap::new();
            let name = "Jerry".to_cell();
            map.insert(String::from("id"), &Cell::Int(8));
            map.insert(String::from("name"), &name);
            map.insert(String::from("age"), &Cell::Int(39));
            map.insert(String::from("score"), &Cell::Int(400));
            map.insert(String::from("registered"), &Cell::Bool(true));
        }
        _ => {
            panic!("dataframe iter index out of bounds.")
        }
    });

    df.into_iter().enumerate().for_each(|(i, row)| match i {
        0 => {
            let mut map: HashMap<String, Cell> = HashMap::new();
            map.insert(String::from("id"), Cell::Int(6));
            map.insert(String::from("name"), "Sasha".to_cell());
            map.insert(String::from("age"), Cell::Int(33));
            map.insert(String::from("score"), Cell::Int(1600));
            map.insert(String::from("registered"), Cell::Bool(false));
            assert_eq!(row, map);
        }
        1 => {
            let mut map: HashMap<String, Cell> = HashMap::new();
            map.insert(String::from("id"), Cell::Int(7));
            map.insert(String::from("name"), "Jane".to_cell());
            map.insert(String::from("age"), Cell::Int(24));
            map.insert(String::from("score"), Cell::Int(700));
            map.insert(String::from("registered"), Cell::Bool(true));
            assert_eq!(row, map);
        }
        2 => {
            let mut map: HashMap<String, Cell> = HashMap::new();
            map.insert(String::from("id"), Cell::Int(8));
            map.insert(String::from("name"), "Jerry".to_cell());
            map.insert(String::from("age"), Cell::Int(39));
            map.insert(String::from("score"), Cell::Int(400));
            map.insert(String::from("registered"), Cell::Bool(true));
        }
        _ => {
            panic!("dataframe iter index out of bounds.")
        }
    });

    // chunk
    assert_eq!(
        generic_dataframe()
            .iter_chunk(2)
            .map(|df| df)
            .collect::<Vec<Dataframe>>(),
        vec![
            Dataframe::from_rows(
                vec!["id", "name", "age", "score", "registered"],
                vec![
                    row!(4, "Sally", 23, 700, true),
                    row!(1, "Jasper", 41, 900, false),
                ],
            )
            .unwrap(),
            Dataframe::from_rows(
                vec!["id", "name", "age", "score", "registered"],
                vec![
                    row!(5, "Jake", 33, 1200, true),
                    row!(2, "Susie", 27, 200, true),
                ],
            )
            .unwrap(),
            Dataframe::from_rows(
                vec!["id", "name", "age", "score", "registered"],
                vec![row!(3, "Spruce", 24, 800, false),],
            )
            .unwrap()
        ]
    )
}

#[test]
fn errors() {
    // shape
    let mut df = generic_dataframe();
    let result = df.add_col("new", vec![1, 2]);
    match result {
        Ok(_) => panic!("Shape err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Invalid col length".to_string()),
    }
    let result = df.add_col("id", vec![1, 2, 3, 4, 5]);
    match result {
        Ok(_) => panic!("Unique col err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Col names must be unique".to_string()),
    }
    let result = df.add_row(row![1, "Sally"]);
    match result {
        Ok(_) => panic!("Shape err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Invalid row length".to_string()),
    }
    match Dataframe::from_rows(
        vec!["name", "age", "val"],
        vec![row!("Jake", 23, true), row!("Sally", 44)],
    ) {
        Ok(_) => panic!("Shape err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Inconsistent data shape".to_string()),
    }
    let mut df = generic_dataframe();
    match df.concat(alt_dataframe()) {
        Ok(_) => panic!("Concat shape err not detected"),
        Err(err) => assert_eq!(
            err.to_string(),
            "Concat against mismatched dataframes".to_string()
        ),
    }
    match df.join(&generic_dataframe(), "id", "id") {
        Ok(_) => panic!("Join unique err not detected"),
        Err(err) => assert_eq!(
            err.to_string(),
            "Join dataframe columns are not unique".to_string()
        ),
    }
    match df.column("unknown") {
        Ok(_) => panic!("Missing col err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Column not found".to_string()),
    }
    match df.col_mut("unknown") {
        Ok(_) => panic!("Missing col err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Column not found".to_string()),
    }
}

#[test]
fn drop() {
    let mut df = generic_dataframe();
    df.drop_cols(["name", "registered"].into());
    let expected_df = Dataframe::from_rows(
        vec!["id", "age", "score"],
        vec![
            row!(4, 23, 700),
            row!(1, 41, 900),
            row!(5, 33, 1200),
            row!(2, 27, 200),
            row!(3, 24, 800),
        ],
    )
    .unwrap();
    assert_eq!(df, expected_df);

    let mut df = generic_dataframe();
    let expected_df = Dataframe::from_rows(
        vec!["name", "registered"],
        vec![
            row!("Sally", true),
            row!("Jasper", false),
            row!("Jake", true),
            row!("Susie", true),
            row!("Spruce", false),
        ],
    )
    .unwrap();
    df.retain_cols(["name", "registered"].into());
    assert_eq!(df, expected_df);
}

#[test]
fn describe() {
    let es: Vec<u32> = vec![];
    let ds = Col::new("val".to_string(), es.clone()).describe();
    assert_eq!(
        ds,
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 0.0),
                row!("mean", None::<f64>),
                row!("std", None::<f64>),
                row!("min", None::<f64>),
                row!("25%", None::<f64>),
                row!("50%", None::<f64>),
                row!("75%", None::<f64>),
                row!("max", None::<f64>),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap(),
    );
    assert_eq!(
        Col::new("val".to_string(), vec![1]).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 1.0),
                row!("mean", 1.0),
                row!("std", None::<f64>),
                row!("min", 1.0),
                row!("25%", None::<f64>),
                row!("50%", None::<f64>),
                row!("75%", None::<f64>),
                row!("max", 1.0),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap()
    );
    assert_eq!(
        Col::new("val".to_string(), (1..=2).collect::<Vec<u32>>()).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 2.0),
                row!("mean", 1.5),
                row!("std", 0.5),
                row!("min", 1.0),
                row!("25%", None::<f64>),
                row!("50%", None::<f64>),
                row!("75%", None::<f64>),
                row!("max", 2.0),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap()
    );
    assert_eq!(
        Col::new("val".to_string(), (1..=4).collect::<Vec<u32>>()).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 4.0),
                row!("mean", 2.5),
                row!("std", 1.12),
                row!("min", 1.0),
                row!("25%", 1.5),
                row!("50%", 2.5),
                row!("75%", 3.5),
                row!("max", 4.0),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap()
    );
    assert_eq!(
        Col::new("val".to_string(), (1..=5).collect::<Vec<u32>>()).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 5.0),
                row!("mean", 3.0),
                row!("std", 1.41),
                row!("min", 1.0),
                row!("25%", 1.5),
                row!("50%", 3.0),
                row!("75%", 4.5),
                row!("max", 5.0),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap()
    );
    assert_eq!(
        Col::new("val".to_string(), (1..=6).collect::<Vec<u32>>()).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 6.0),
                row!("mean", 3.5),
                row!("std", 1.71),
                row!("min", 1.0),
                row!("25%", 2.0),
                row!("50%", 3.5),
                row!("75%", 5.0),
                row!("max", 6.0),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap()
    );
    assert_eq!(
        Col::new("val".to_string(), (1..=7).collect::<Vec<u32>>()).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 7.0),
                row!("mean", 4.0),
                row!("std", 2.0),
                row!("min", 1.0),
                row!("25%", 2.0),
                row!("50%", 4.0),
                row!("75%", 6.0),
                row!("max", 7.0),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap()
    );
    assert_eq!(
        Col::new("val".to_string(), (1..=8).collect::<Vec<u32>>()).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 8.0),
                row!("mean", 4.5),
                row!("std", 2.29),
                row!("min", 1.0),
                row!("25%", 2.5),
                row!("50%", 4.5),
                row!("75%", 6.5),
                row!("max", 8.0),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap()
    );
    assert_eq!(
        Col::new("val".to_string(), (1..=9).collect::<Vec<u32>>()).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 9.0),
                row!("mean", 5.0),
                row!("std", 2.58),
                row!("min", 1.0),
                row!("25%", 2.5),
                row!("50%", 5.0),
                row!("75%", 7.5),
                row!("max", 9.0),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap()
    );
    assert_eq!(
        Col::new("val".to_string(), (1..=10).collect::<Vec<u32>>()).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 10.0),
                row!("mean", 5.5),
                row!("std", 2.87),
                row!("min", 1.0),
                row!("25%", 3.0),
                row!("50%", 5.5),
                row!("75%", 8.0),
                row!("max", 10.0),
                row!("unique", None::<f64>),
                row!("top idx", None::<f64>),
                row!("freq", None::<f64>),
            ],
        )
        .unwrap()
    );
    assert_eq!(
        Col::new("val".to_string(), vec!["A", "B", "C", "B", "C", "B", "Z"]).describe(),
        Dataframe::from_rows(
            vec!["::", "val"],
            vec![
                row!("count", 7.0),
                row!("mean", None::<f64>),
                row!("std", None::<f64>),
                row!("min", None::<f64>),
                row!("25%", None::<f64>),
                row!("50%", None::<f64>),
                row!("75%", None::<f64>),
                row!("max", None::<f64>),
                row!("unique", 4.0),
                row!("top idx", 1.0),
                row!("freq", 3.0),
            ],
        )
        .unwrap()
    );
}

#[test]
fn group() {
    let df = Dataframe::from_rows(
        vec!["name", "department", "salary", "age"],
        vec![
            row!("Jasper", "Sales", 100, 29),
            row!("James", "Marketing", 200, 44),
            row!("Susan", "Sales", 300, 65),
            row!("Jane", "Marketing", 400, 47),
            row!("Sam", "Sales", 100, 55),
            row!("Sally", "Engineering", 200, 30),
        ],
    )
    .unwrap();
    df.to_slice()
        .chunk_by("department")
        .unwrap()
        .into_iter()
        .enumerate()
        .for_each(|(i, chunk)| match i {
            0 => assert_eq!(
                chunk,
                Dataframe::from_rows(
                    vec!["name", "department", "salary", "age"],
                    vec![
                        row!("Jasper", "Sales", 100, 29),
                        row!("Susan", "Sales", 300, 65),
                        row!("Sam", "Sales", 100, 55),
                    ],
                )
                .unwrap()
            ),
            1 => assert_eq!(
                chunk,
                Dataframe::from_rows(
                    vec!["name", "department", "salary", "age"],
                    vec![
                        row!("James", "Marketing", 200, 44),
                        row!("Jane", "Marketing", 400, 47),
                    ],
                )
                .unwrap()
            ),
            _ => assert_eq!(
                chunk,
                Dataframe::from_rows(
                    vec!["name", "department", "salary", "age"],
                    vec![row!("Sally", "Engineering", 200, 30),],
                )
                .unwrap()
            ),
        });
    let grouped = df
        .group_by("department")
        .select("department", Coalesce, "department")
        .select("name", Count, "count")
        .select("salary", Max, "max salary")
        .select("salary", Min, "min salary")
        .select("age", Mean, "average age")
        .to_dataframe()
        .unwrap();
    assert_eq!(
        grouped,
        Dataframe::from_rows(
            vec![
                "department",
                "count",
                "max salary",
                "min salary",
                "average age"
            ],
            vec![
                row!("Sales", 3 as u32, 300.0, 100.0, 49.67),
                row!("Marketing", 2 as u32, 400.0, 200.0, 45.5),
                row!("Engineering", 1 as u32, 200.0, 200.0, 30.0),
            ],
        )
        .unwrap()
    )
}

#[derive(Deserialize)]
struct SneakyType {
    name: String,
    age: i64,
    val: bool,
}
impl ToRow for SneakyType {
    fn to_row(&self) -> Vec<Cell> {
        if self.age < 40 {
            vec![self.name.as_str().into(), self.age.into(), self.val.into()]
        } else {
            vec![
                self.name.as_str().into(),
                self.age.into(),
                format!("{}", self.val).into(),
            ]
        }
    }
    fn labels(&self) -> Vec<String> {
        vec!["name".to_string(), "age".to_string(), "val".to_string()]
    }
}
#[test]
fn mismatched_types() {
    // Not exposed: add_cell_col / set_columns
    // Type enforced by generic: add_col
    // Redundant: from_csv uses from_rows
    match generic_dataframe().add_row(row!(4, "Sally", 23, 700, "true")) {
        Ok(_) => panic!("Type err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Invalid col types".to_string()),
    }
    let result = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(4, "Sally", 23, 700, true),
            row!(1, "Jasper", 41, "900", false),
            row!(5, "Jake", 33, 1200, true),
            row!(2, "Susie", 27, 200, true),
            row!(3, "Spruce", 24, 800, false),
        ],
    );
    match result {
        Ok(_) => panic!("Type err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Inconsistent col types".to_string()),
    }
    let result = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(4, "Sally", 23, 700, None::<String>),
            row!(1, "Jasper", 41, 900, false),
            row!(5, "Jake", 33, 1200, true),
            row!(2, "Susie", 27, 200, true),
            row!(3, "Spruce", 24, 800, false),
        ],
    );
    match result {
        Ok(_) => panic!("Type err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Inconsistent col types".to_string()),
    }
    let result = Dataframe::from_rows(
        vec!["id", "name", "age", "score", "registered"],
        vec![
            row!(4, "Sally", 23, 700, true),
            row!(1, "Jasper", 41, 900, false),
            row!(5, "Jake", 33, 1200, true),
            row!(None::<bool>, "Susie", 27, 200, true),
            row!(3, "Spruce", 24, 800, false),
        ],
    );
    match result {
        Ok(_) => panic!("Type err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Inconsistent col types".to_string()),
    }

    let result = Dataframe::from_structs(vec![
        SneakyType {
            name: "Jake".to_string(),
            age: 23,
            val: true,
        },
        SneakyType {
            name: "Sally".to_string(),
            age: 44,
            val: false,
        },
        SneakyType {
            name: "Jasper".to_string(),
            age: 61,
            val: true,
        },
    ]);
    match result {
        Ok(_) => panic!("Type err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Inconsistent col types".to_string()),
    }
    // apply
    let result = generic_dataframe()
        .col_mut("id")
        .unwrap()
        .apply(|cell| *cell = Cell::Str("hello".to_string()));
    match result {
        Ok(_) => panic!("Type err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Invalid cell type".to_string()),
    }

    let result = generic_dataframe().set_val(0, "id", 2.2);
    match result {
        Ok(_) => panic!("Type err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Invalid cell type".to_string()),
    }
    let result = generic_dataframe().set_val(0, "unknown", 2.2);
    match result {
        Ok(_) => panic!("Missing column err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Column not found".to_string()),
    }

    let result = generic_dataframe().update_val(0, "id", |cell| *cell = Cell::Float(4.4));
    match result {
        Ok(_) => panic!("Type err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Invalid cell type".to_string()),
    }
    let result = generic_dataframe().update_val(0, "unknown", |cell| *cell = Cell::Int(4));
    match result {
        Ok(_) => panic!("Missing column err not detected"),
        Err(err) => assert_eq!(err.to_string(), "Column not found".to_string()),
    }
}

#[test]
fn index_errors() {
    let result = generic_dataframe().set_val(100, "id", 2);
    match result {
        Ok(_) => panic!("Index error not detected"),
        Err(err) => assert_eq!(err.to_string(), "Index out of bounds".to_string()),
    }
    let result = generic_dataframe().update_val(100, "id", |cell| *cell = Cell::Int(4));
    match result {
        Ok(_) => panic!("Index error not detected"),
        Err(err) => assert_eq!(err.to_string(), "Index out of bounds".to_string()),
    }
    let df = generic_dataframe();
    let result = df.cell(100, "id");
    assert_eq!(result, None);
}
