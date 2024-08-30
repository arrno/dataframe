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
```rust
#[derive(Serialize, Deserialize)]
struct MyRow {
    name: String,
    score: i64,
    val: bool,
}

impl ToRow for MyRow {
    fn to_row(&self) -> Vec<Cell> {
        vec![self.name.as_str().into(), self.age.into(), self.val.into()]
    }
    fn labels(&self) -> Vec<String> {
        vec!["name".to_string(), "age".to_string(), "val".to_string()]
    }
}

let df = Dataframe::from_csv::<MyRow>("./tests/test.csv").unwrap();
```
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
```
**By column**
```rust
```
## Filter
**Simple**
```rust
```
**Complex**
Nest as many or/add/exp as needed
```rust
```
## Mutate
```rust
```
## Sort
```rust
```
