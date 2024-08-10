use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt;

const MAX_CELL_DISPLAY: usize = 20;

#[derive(Debug, Clone)]
struct MyErr {
    reason: String,
}
impl fmt::Display for MyErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Oh No! {}", self.reason)
    }
}

enum Join {
    Left,
    Right,
    Inner,
    Union,
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
    pub fn as_string(&self) -> String {
        match self {
            Cell::Int(x) => format!("{x}"),
            Cell::Uint(x) => format!("{x}"),
            Cell::Str(x) => format!("{x}"),
            Cell::Null => String::from("Null"),
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

impl ToCell for String {
    fn to_cell(self) -> Cell {
        Cell::Str(self)
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::Str(self.clone())
    }
}

impl<T: ToCell> ToCell for Option<T> {
    fn to_cell(self) -> Cell {
        match self {
            Some(val) => val.to_cell(),
            None => Cell::Null,
        }
    }
    fn ref_to_cell(&self) -> Cell {
        match self {
            Some(val) => val.ref_to_cell(),
            None => Cell::Null,
        }
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

    pub fn from_csv() {} // TODO
    pub fn to_csv() {} // TODO
    pub fn join() {} // TODO

    pub fn col_mut(&mut self, name: String) -> Option<&mut Col> {
        self.columns.iter_mut().find(|col| col.name == name)
    }

    pub fn col_map(&self) -> HashMap<String, &Vec<Cell>> {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), &c.values))
            .collect()
    }

    pub fn add_col<T>(&mut self, name: String, set: Vec<T>) -> Result<(), MyErr>
    where
        T: ToCell,
    {
        let l = self.length();
        if l != 0 && l != set.len() {
            return Err(MyErr {
                reason: String::from("Invalid col length"),
            });
        }
        for col in self.columns.iter() {
            if col.name == name {
                return Err(MyErr {
                    reason: String::from("Col names must be unique"),
                });
            }
        }
        self.columns.push(Col::new(name, set));
        Ok(())
    }

    pub fn add_opt_col<T>(&mut self, name: String, set: Vec<Option<T>>) -> Result<(), MyErr>
    where
        T: ToCell,
    {
        let l = self.length();
        if l != 0 && l != set.len() {
            return Err(MyErr {
                reason: String::from("Invalid col length"),
            });
        }
        for col in self.columns.iter() {
            if col.name == name {
                return Err(MyErr {
                    reason: String::from("Col names must be unique"),
                });
            }
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

    pub fn add_opt_row<T>(&mut self, set: Vec<Option<T>>) -> Result<(), MyErr>
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

    pub fn filter(&mut self, exp: ExpU) -> Result<Self, MyErr> {
        let filter_col = match self.columns.iter().find(|col| col.name == exp.target) {
            Some(col) => col,
            None => {
                return Err(MyErr {
                    reason: "Target not found".to_string(),
                })
            }
        };

        let filter_set: Vec<bool> = filter_col
            .values
            .iter()
            .map(|val| match exp {
                _ => true, // TODO
            })
            .collect();

        Ok(Dataframe {
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
                        .filter(|(i, _)| filter_set[*i])
                        .map(|(_, c)| c.clone())
                        .collect(),
                })
                .collect(),
        })
    }

    pub fn filter_complex(&mut self, mut exp: Exp) -> Result<Self, MyErr> {
        let col_map = self.col_map();
        let filter_set = (0..self.length())
            .map(|i| {
                let val_map: HashMap<String, &Cell> =
                    col_map.iter().map(|(k, v)| (k.to_owned(), &v[i])).collect();
                exp.evaluate(&val_map)
            })
            .collect::<Vec<bool>>();

        Ok(Dataframe {
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
                        .filter(|(i, _)| filter_set[*i])
                        .map(|(_, c)| c.clone())
                        .collect(),
                })
                .collect(),
        })
    }

    pub fn slice(&self, start: usize, stop: usize) -> Result<Self, MyErr> {
        if start >= stop || stop > self.length() {
            return Err(MyErr {
                reason: "Invalid slice params".to_string(),
            });
        }
        Ok(Dataframe {
            title: self.title.clone(),
            columns: self
                .columns
                .iter()
                .map(|col| Col {
                    name: col.name.clone(),
                    typed: col.typed.clone(),
                    values: col.values[start..stop].to_vec(),
                })
                .collect(),
        })
    }

    fn length(&self) -> usize {
        if self.columns.len() == 0 {
            return 0;
        }
        self.columns[0].values.len()
    }

    pub fn head(&self) -> Result<(), MyErr> {
        // Slice head
        let head_df = self.slice(0, min(5, self.length()))?;
        let mut col_lengths: Vec<usize> = head_df
            .columns
            .iter()
            .map(|col| min(MAX_CELL_DISPLAY, col.name.len()))
            .collect();
        // Calc col sizes
        head_df.columns.iter().enumerate().for_each(|(i, col)| {
            col.values.iter().for_each(|val| {
                col_lengths[i] = min(MAX_CELL_DISPLAY, max(col_lengths[i], val.as_string().len()))
            })
        });
        // Make sep
        let sep = (0..col_lengths.len())
            .map(|i| {
                let s = "-".to_string().repeat(col_lengths[i]);
                format!("+{s}")
            })
            .collect::<Vec<String>>()
            .join("");
        // Do print
        println!("{sep}+");
        head_df
            .columns
            .iter()
            .enumerate()
            .for_each(|(i, col)| print!("|{}", pad_string(&col.name, col_lengths[i])));
        print!("|\n");
        println!("{sep}+");
        for row in 0..min(5, head_df.length()) {
            for col in 0..col_lengths.len() {
                print!(
                    "|{}",
                    pad_string(
                        &head_df.columns[col].values[row].as_string(),
                        col_lengths[col]
                    )
                );
            }
            print!("|\n")
        }
        println!("{sep}+");
        Ok(())
    }
}

fn pad_string(s: &str, w: usize) -> String {
    if s.len() == w {
        return s.to_string();
    } else if s.len() > w {
        return s[..w].to_string();
    }
    let spaces = " ".to_string().repeat(w - s.len());
    return format!("{s}{spaces}");
}

struct ExpU {
    target: String,
    op: Op,
    value: Cell,
}

impl ExpU {
    pub fn eval(&self, against: &Cell) -> bool {
        match &self.value {
            Cell::Int(v) => {
                if let Cell::Int(a) = against {
                    v == a
                } else {
                    false
                }
            }
            Cell::Uint(v) => {
                if let Cell::Uint(a) = against {
                    v == a
                } else {
                    false
                }
            }
            Cell::Str(v) => {
                if let Cell::Str(a) = against {
                    v == a
                } else {
                    false
                }
            }
            Cell::Null => {
                if let Cell::Null = against {
                    true
                } else {
                    false
                }
            }
        }
    }
}

struct Or {
    vexp: Vec<Exp>,
}
struct And {
    vexp: Vec<Exp>,
}
enum Exp {
    Or(Or),
    And(And),
    ExpU(ExpU),
}

impl Exp {
    pub fn evaluate(&self, against: &HashMap<String, &Cell>) -> bool {
        match self {
            Self::ExpU(ex) => match against.get(&ex.target) {
                Some(x) => ex.eval(x),
                None => false,
            },
            Self::Or(ex) => match ex.vexp.iter().find(|e| e.evaluate(against)) {
                Some(_) => true,
                _ => false,
            },
            Self::And(ex) => ex.vexp.iter().all(|e| e.evaluate(against)),
        }
    }
    // Ideally, we flatten, check truth vals, update truthy by ref, then evaluate structured expression
    pub fn flatten(&mut self) -> Vec<&mut ExpU> {
        match self {
            Self::ExpU(ex) => vec![ex],
            Self::Or(ex) => ex.vexp.iter_mut().map(|e| e.flatten()).flatten().collect(),
            Self::And(ex) => ex.vexp.iter_mut().map(|e| e.flatten()).flatten().collect(),
        }
    }
}

impl ExpU {
    pub fn new<T: ToCell>(target: String, op: Op, value: T) -> Self {
        ExpU {
            target: target,
            op: op,
            value: value.to_cell(),
        }
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
    df.add_col("nums".to_string(), Vec::from([0, 1, 2, 3, 4, 5, 6, 7, 8]))
        .unwrap();
    df.add_col(
        "more nums".to_string(),
        Vec::from([9, 10, 11, 12, 13, 14, 15, 16, 17]),
    )
    .unwrap();
    df.add_opt_col(
        "the best nums".to_string(),
        Vec::from([
            Some(-10),
            None,
            Some(200),
            Some(400),
            Some(777),
            Some(-289),
            Some(7),
            Some(12),
            Some(902),
        ]),
    )
    .unwrap();
    df.add_col(
        "strangs".to_string(),
        Vec::from([
            "woop!".to_string(),
            "Hello".to_string(),
            "dope man".to_string(),
            "cool boi".to_string(),
            "wspwspwsp".to_string(),
            ":-)".to_string(),
            "Who's that daddy?".to_string(),
            "Snarg".to_string(),
            "NaNaNaN".to_string(),
        ]),
    )
    .unwrap();
    df.head().unwrap();

    df.col_mut("nums".to_string())
        .unwrap()
        .values
        .iter_mut()
        .for_each(|c| {
            if let Cell::Int(x) = c {
                *x += 2
            }
        });
    df.head().unwrap();
}
