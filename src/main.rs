use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
struct MyErr {
    reason: String,
}
impl fmt::Display for MyErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Oh No! {}", self.reason)
    }
}

#[derive(PartialEq, Clone)]
enum Cell {
    Int(i64),
    Uint(u32),
    Str(String),
    Null,
}
impl Cell {
    pub fn zero(&self) -> Self {
        match self {
            Cell::Int(_) => Cell::Int(0),
            Cell::Uint(_) => Cell::Uint(0),
            Cell::Str(_) => Cell::Str(String::new()),
            Cell::Null => Cell::Null,
        }
    }
}

trait ToCell {
    fn ref_to_cell(&self) -> Cell;
    fn to_cell(self) -> Cell;
}

impl ToCell for u32 {
    fn to_cell(self) -> Cell {
        Cell::Uint(self)
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::Uint(self.clone())
    }
}

impl ToCell for i32 {
    fn to_cell(self) -> Cell {
        Cell::Int(self.into())
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::Int(self.clone().into())
    }
}

impl ToCell for i64 {
    fn to_cell(self) -> Cell {
        Cell::Int(self)
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::Int(self.clone())
    }
}

struct Col {
    name: String,
    values: Vec<Cell>,
    typed: Cell,
}

impl Col {
    pub fn new<T>(name: String, set: Vec<T>) -> Self
    where
        T: ToCell,
    {
        let mut z = Cell::Null;
        if set.len() > 0 {
            z = set[0].ref_to_cell().zero();
        }
        Col {
            name: name,
            values: set.into_iter().map(|val| val.to_cell()).collect(), // should validate all types match
            typed: z,
        }
    }
}

struct Dataframe {
    title: String,
    columns: Vec<Col>,
}

impl Dataframe {
    pub fn new(title: String) -> Self {
        Dataframe {
            title: title,
            columns: vec![],
        }
    }

    pub fn add_col<T>(&mut self, name: String, set: Vec<T>) -> Result<(), MyErr>
    where
        T: ToCell,
    {
        if self.columns.len() > 0 && self.columns[0].values.len() != set.len() {
            return Err(MyErr {
                reason: String::from("Invalid col length"),
            });
        }
        self.columns.push(Col::new(name, set));
        Ok(())
    }

    pub fn add_row<T>(&mut self, set: Vec<T>) -> Result<(), MyErr>
    where
        T: ToCell,
    {
        if set.len() != self.columns.len() {
            return Err(MyErr {
                reason: String::from("Invalid col length"),
            });
        }
        for (i, col) in self.columns.iter().enumerate() {
            if col.values.len() > 0 && col.values[0].zero() != set[i].ref_to_cell().zero() {
                return Err(MyErr {
                    reason: String::from("Invalid col types"),
                });
            }
        }
        for i in 0..set.len() {
            self.columns[i].values.push(set[i].ref_to_cell());
        }
        Ok(())
    }

    pub fn filter<T>(&mut self, exp: Exp<T>)
    where
        T: ToCell,
    {
        // filter df by exp against col
    }

    pub fn slice<T>(&mut self, start: usize, stop: usize) -> Self
    where
        T: ToCell,
    {
        Dataframe {
            title: self.title.clone(),
            columns: self
                .columns
                .iter()
                .map(|col| Col {
                    name: col.name.clone(),
                    typed: col.typed.clone(),
                    values: col
                        .values
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i >= start && *i < stop) // Should short circuit
                        .map(|tup| tup.1.clone())
                        .collect(),
                })
                .collect(),
        }
    }

    pub fn display(&self) {
        println!("[DATAFRAME]")
    }
}

struct Exp<T: ToCell> {
    target: String,
    op: Op,
    value: T,
}

impl<T: ToCell> Exp<T> {
    pub fn new(target: String, op: Op, value: T) -> Self {
        Exp { target, op, value }
    }
}

enum Op {
    Eq,
    Neq,
    Gt,
    Lt,
}

pub fn main() {
    let mut df = Dataframe::new(String::from("Raw Data"));
    df.add_col("nums".to_string(), Vec::from([0, 1, 2, 3, 4, 5, 6, 7, 8]));
    df.display();
}
