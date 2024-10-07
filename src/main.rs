use rowboat::dataframe::*;

fn main() {
    let df = Dataframe::from_rows(
        vec!["strangs", "nums", "null nums", "time"],
        vec![
            row!("sugar", 0, Some(-10), Timestamp(2024, 08, 01, 10, 10, 10)),
            row!(
                "sweets",
                1,
                None::<i64>,
                Timestamp(2024, 08, 01, 10, 10, 10)
            ),
            row!(
                "candy pop",
                2,
                Some(200),
                Timestamp(2024, 08, 01, 10, 10, 10)
            ),
            row!("caramel", 3, Some(400), Timestamp(2024, 08, 01, 10, 10, 10)),
            row!(
                "chocolate",
                4,
                Some(777),
                Timestamp(2024, 08, 01, 10, 10, 10)
            ),
        ],
    )
    .unwrap();
    df.print();
    df.iter_sql("my_table").for_each(|(query, args)| {
        println!("{query}");
        println!("{:?}", args);
    });
}
