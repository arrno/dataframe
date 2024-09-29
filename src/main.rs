use rowboat::dataframe::*;
use rowboat::group::Reducer::*;

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
    df.to_slice()
        .chunk_by("state")
        .unwrap()
        .iter()
        .for_each(|chunk| chunk.print());
    let grouped = df
        .group_by("department")
        .select("department", Coalesce)
        .select("username", Count)
        .select("salary", Max)
        .select("age", Mean)
        .collect()
        .unwrap();
    grouped.print();
}
