# Rowboat 🛶 
## Dataframe in rust 🦀
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

## Import
```rust
use rowboat::dataframe::*;
```
## Create
**From rows**

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
**From csv**

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

**From structs**

Create from a `Vec<T>` where `T` implements `ToRow`
```rust
#[derive(ToRow)]
struct MyRow {
    name: String,
    score: i64,
    val: bool,
}

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

- `Int(i64)`
- `Uint(u64)`
- `Str(String)`
- `Bool(bool)`
- `Float(f64)`
- `DateTime(chrono::NaiveDateTime)`
- `Null(Box<Cell>)`

## Display
**All**
```rust
df.print();
```
**Head**
```rust
df.head(5);
```
**Tail**
```rust
df.tail(5);
```
## Metadata 
**Info**

Print shape and types
```rust
df.info();
// DF Info
// Shape: 3_col x 5_row
// Columns: strangs <Str>, nums <Int>, null nums <Int>
```
**Describe**
```rust
df.describe().print();
```
Creates a describe df and prints it:
```
+---------+---------+------+-----------+
| ::      | strangs | nums | null nums |
+---------+---------+------+-----------+
| count   |       5 |    5 |         5 |
| mean    |    Null |    2 |    341.75 |
| std     |    Null | 1.41 |    301.15 |
| min     |    Null |    0 |       -10 |
| 25%     |    Null |  0.5 |        95 |
| 50%     |    Null |    2 |       300 |
| 75%     |    Null |  3.5 |     588.5 |
| max     |    Null |    4 |       777 |
| unique  |       5 | Null |      Null |
| top idx |       0 | Null |      Null |
| freq    |       1 | Null |      Null |
+---------+---------+------+-----------+
```
**Column names**
```rust
df.col_names();
```
## Extend
**Add column**
```rust
df.add_col("new column", vec![2, 4, 6]).unwrap();
```
**Add row**
```rust
df.add_row(row!("Jane", 44, true)).unwrap();
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

Inner
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

Left
```rust
let result_df = df
    .left_join(
        &Dataframe::from_rows(
            vec!["user_id", "score", "rate"],
            vec![row!(1, 700, 0.4)],
        )
        .unwrap(),
        ("nums", "user_id"),
    )
    .unwrap();
```

**More on columns**

Copy/update an existing column into a new column
```rust
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
```

Create a column derived from multiple source column values
```rust
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
**Get cell**
```rust
// (row_index, col_name)
let cell = df.cell(1, "score").unwrap();
```

## Reshape
**Drop columns**

Drop specified columns
```rust
df.drop_cols(["name", "registered"].into());
```
**Retain columns**

Drop all columns other than those specified
```rust
df.retain_cols(["name", "registered"].into());
```

**Rename column**
```rust
// returns true if successful
if df.rename_col("strangs", "Strings") {
    println!("Success!");
} else {
    println!("Column not found.")
}
```

## Filter
Operation enum variants:
- `Eq` equal
- `Neq` not equal
- `Gt` greater than
- `Lt` less than
- `GtEq` greater or equal than
- `LtEq` less or equal than
- `Mod(i64)` mod `i` is
- `Regex` matches regex

**Simple**
```rust
// where age val is not null
let df = df.filter(exp("age", Neq, None::<i64>)).unwrap();
```
**Complex**

Nest as many and/or/not/exp as needed
```rust
let df = df
    .filter(or(vec![
        and(vec![exp("id", Gt, 2), exp("score", Lt, 1000)]),
        exp("val", Eq, false),
    ]))
    .unwrap();
```

**Negate**

Wrap any expression in `not()` to inverse the result
```rust
// filter odd values
let df = df.filter(not(exp("age", Mod(2), 0))).unwrap();
```
## Mutate
**By column**
```rust
df.col_mut("id").unwrap().iter_mut().for_each(|cell| {
    if let Cell::Int(val) = cell {
        *val *= 2
    }
});
```
**By cell**
```rust
if let Cell::Int(val) = df.cell_mut((2, "age")).unwrap() {
    *val += 2;
}
```
## Sort
**Simple**
```rust
// sort by, sort dir [asc() | desc()]
df.sort("at", asc()).unwrap();
```
**Complex**

Use this method for multi column sorting
```rust
let sorted = df
    .into_sort()
    .sort("one", asc())
    .sort("two", asc())
    .sort("three", asc())
    .collect()
    .unwrap();
```
## Iterate
**Iter**
```rust
let unames = df
    .iter()
    .map(|row| match row.get("username") {
        Some(Cell::Str(val)) => val,
        _ => "None",
    })
    .collect::<Vec<&str>>();
```
**Into iter**

A consuming `df.into_iter()` is also available

**Iter chunk**
```rust
df.iter_chunk(2).for_each(|chunk| chunk.print());
```
## Group by
**Reducer enum variants**

`Variant` : "Displayed as"
- `Count` : "c"
- `Sum` : "+"
- `Prod` : "*"
- `Mean` : "m"
- `Min` : "_"
- `Max` : "^"
- `Top` : "t"
- `Unique` : "u"
- `Coalesce` : "&"
- `NonNull` : "!"

**Query**

Group df by common `group_by` values then do selects to reduce data groups into a new dataframe
```rust
let grouped_df = df
    .group_by("department")
    .select("department", Coalesce) // easiest way to get grouped col
    .select("name", Count)
    .select("salary", Max)
    .select("age", Mean)
    .to_dataframe()
    .unwrap();
```
Above query transforms this raw data:
```
+--------+-------------+--------+-----+
| name   | department  | salary | age |
+--------+-------------+--------+-----+
| Jasper | Sales       |    100 |  29 |
| James  | Marketing   |    200 |  44 |
| Susan  | Sales       |    300 |  65 |
| Jane   | Marketing   |    400 |  47 |
| Sam    | Sales       |    100 |  55 |
| Sally  | Engineering |    200 |  30 |
+--------+-------------+--------+-----+
```
Into this new dataframe:
```
+---------------+---------+-----------+--------+
| &\ department | c\ name | ^\ salary | m\ age |
+---------------+---------+-----------+--------+
| Sales         |       3 |       300 |  49.67 |
| Marketing     |       2 |       400 |   45.5 |
| Engineering   |       1 |       200 |     30 |
+---------------+---------+-----------+--------+
```
**Grouped chunks**

Group df by common `chunk_by` values into a `Vec<Dataframe>`
```rust
df.to_slice()
    .chunk_by("State")
    .unwrap()
    .iter()
    .for_each(|chunk| chunk.print());
```
## Store
**To csv**
```rust
df.to_csv("./tests/test.csv").unwrap();
```
## Examples
For more examples, see `./tests/integration_test.rs`