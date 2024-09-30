use rowboat::dataframe::*;

fn main() {
    let df = Dataframe::from_rows(
        vec!["strangs", "nums", "null nums"],
        vec![
            row!("sugar", 2, Some(-10)),
            row!("sugar", 1, None::<i64>),
            row!("candy", 7, Some(200)),
            row!("sugar", 3, Some(400)),
            row!("candy", 9, Some(777)),
        ],
    )
    .unwrap();
    df.print();
}
