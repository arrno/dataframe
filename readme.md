# Dataframe in rust 🦀
```
+-----------+------+-----------+
| strangs   | nums | null nums |
+-----------+------+-----------+
| sugar     |    0 |       -10 |
| sweets    |    1 |      Null |
| candy pop |    2 |       200 |
| caramel   |    3 |       400 |
| chocolate |    4 |       777 |
+-----------+------+-----------+
```

## Create
**Create from rows**
using the `row!` macro
```rust
let df = Dataframe::from_rows(
    vec!["id", "name", "score", "val"],
    vec![
        row!(1, "Sally", 23, true),
        row!(2, "Jasper", 41, false),
        row!(3, "Jake", 33, true),
    ],
)
.unwrap();
```
**Create from csv**

With ToRow proc-macro
```rust
#[derive(Deserialize, ToRow)]
struct MyRow {
    name: String,
    score: i64,
    val: bool,
}

let df = Dataframe::from_csv::<MyRow>("./tests/test.csv").unwrap();
```
Or implement ToRow manually
```rust
impl ToRow for MyRow {
    fn to_row(&self) -> Vec<Cell> {
        vec![self.name.as_str().into(), self.age.into(), self.val.into()]
    }
    fn labels(&self) -> Vec<String> {
        vec!["name".to_string(), "age".to_string(), "val".to_string()]
    }
}
```
**With null values**
```rust
let df = Dataframe::from_rows(
    vec!["name", "age", "score", "val"],
    vec![
        row!("Sasha", None::<i64>, 160, Some(false)),
        row!("Jane", Some(24), 70, None::<bool>),
        row!("Jerry", None::<i64>, 40, Some(true)),
    ],
)
.unwrap();
```
**With timestamp**
```rust
let df = Dataframe::from_rows(
    vec!["id", "label", "at"],
    vec![
        row!(2, "Noon", Timestamp(2024, 8, 26, 12, 15, 0)),
        row!(3, "Night", Timestamp(2024, 8, 26, 22, 45, 0)),
        row!(1, "Morning", Timestamp(2024, 8, 26, 8, 5, 0)),
    ],
)
.unwrap();
```
**Supported types**
- String
- Int
- Uint
- Float
- Bool
- Option/Null
- chrono::NaiveDateTime

## Display
```rust
df.print();
```
## Extend
**Add column**
```rust
df.add_col("new column", vec![2, 4, 6]).unwrap();
```
**Add row**
```rust
df.add_row(vec!["Jane", 44, true]).unwrap();
```
**Concat**
```rust
df.concat(
    Dataframe::from_rows(
        vec!["id", "name", "score", "val"],
        vec![
            row!(4, "Sam", 23, true),
            row!(5, "Julie", 41, false),
            row!(6, "Jill", 33, true),
        ],
    )
    .unwrap(),
)
.unwrap();
```
**Join**
```rust
// join(other_df, (left_col, right_col))
let result_df = df
    .join(
        &Dataframe::from_rows(
            vec!["user_id", "score", "rate"],
            vec![
                row!(1, 700, 0.4),
                row!(2, 400, 0.7),
                row!(3, 900, 0.6),
            ],
        )
        .unwrap(),
        ("id", "user_id"),
    )
    .unwrap();
```
## Slice
**By index**
```rust
// to_dataframe copies DataSlice into new Dataframe
df.slice(1, 4).unwrap().to_dataframe();
```
**By column**
```rust
df.col_slice(["name", "age"].into())
    .unwrap()
    .to_dataframe();
```
## Filter
**Simple**
```rust
let df = df.filter(exp("age", neq(), None::<i64>)).unwrap();
```
**Complex**

Nest as many or/add/exp as needed
```rust
let df = df
    .filter(or(vec![
        and(vec![exp("id", gt(), 2), exp("score", lt(), 1000)]),
        exp("val", eq(), false),
    ]))
    .unwrap();
```
Supported expression operations:
- `eq()` equal
- `neq()` not equal
- `gt()` greater than
- `lt()` less than
- `gte()` greater or equal than
- `lte()` less or equal than
- `modl(i: i64)` mod `i` is
- `regx()` matches regex

## Mutate
```rust
df.col_mut("id").unwrap().iter_mut().for_each(|cell| {
    if let Cell::Int(val) = cell {
        *val *= 2
    }
});
```
## Sort
```rust
// sort by, sort dir [asc() | desc()]
df.sort("at", asc()).unwrap();
```
