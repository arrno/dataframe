use rowboat::dataframe::*;

fn main() {
    let df = Dataframe::from_rows(
        vec!["strangs", "nums", "null nums"],
        vec![
            row!("sugar", 0, Some(-10)),
            row!("sweets", 1, None::<i64>),
            row!("candy pop", 2, Some(200)),
            row!("caramel", 3, Some(400)),
            row!("chocolate", 4, Some(777)),
        ],
    )
    .unwrap();
    df.print();
}
