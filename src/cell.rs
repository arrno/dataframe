use chrono::{NaiveDate, NaiveDateTime};

#[derive(PartialEq, Clone, PartialOrd, Debug)]
pub enum Cell {
    Int(i64),
    Uint(u64),
    Str(String),
    Bool(bool),
    Float(f64),
    DateTime(NaiveDateTime),
    Null(Box<Cell>),
}
impl Cell {
    pub fn zero(&self) -> Self {
        match self {
            Cell::Int(_) => Cell::Int(0),
            Cell::Uint(_) => Cell::Uint(0),
            Cell::Str(_) => Cell::Str(String::new()),
            Cell::Bool(_) => Cell::Bool(false),
            Cell::Float(_) => Cell::Float(0.0),
            Cell::DateTime(_) => Cell::DateTime(
                NaiveDate::from_ymd_opt(30, 4, 3)
                    .unwrap()
                    .and_hms_opt(15, 0, 0)
                    .unwrap(),
            ),
            Cell::Null(cell) => cell.zero(),
        }
    }
    pub fn as_string(&self) -> String {
        match self {
            Cell::Int(x) => format!("{x}"),
            Cell::Uint(x) => format!("{x}"),
            Cell::Str(x) => format!("{x}"),
            Cell::Bool(x) => format!("{x}"),
            Cell::Float(x) => format!("{x}"),
            Cell::DateTime(x) => format!("{x}"),
            Cell::Null(_) => String::from("Null"),
        }
    }
    pub fn type_string(&self) -> String {
        match self {
            Cell::Int(_) => String::from("Int"),
            Cell::Uint(_) => String::from("Uint"),
            Cell::Str(_) => String::from("Str"),
            Cell::Bool(_) => String::from("Bool"),
            Cell::Float(_) => String::from("Float"),
            Cell::DateTime(_) => String::from("DateTime"),
            Cell::Null(v) => format!("Null({})", v.type_string()),
        }
    }
    pub fn is_num(&self) -> bool {
        match self {
            Cell::Int(_) => true,
            Cell::Uint(_) => true,
            _ => false,
        }
    }
}

pub trait ToCell {
    fn ref_to_cell(&self) -> Cell;
    fn to_cell(self) -> Cell;
}

impl ToCell for u32 {
    fn to_cell(self) -> Cell {
        Cell::Uint(self.into())
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::Uint(self.clone().into())
    }
}

impl ToCell for u64 {
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

impl ToCell for f32 {
    fn to_cell(self) -> Cell {
        Cell::Float(self.into())
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::Float(self.clone().into())
    }
}

impl ToCell for f64 {
    fn to_cell(self) -> Cell {
        Cell::Float(self)
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::Float(self.clone())
    }
}

impl ToCell for bool {
    fn to_cell(self) -> Cell {
        Cell::Bool(self)
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::Bool(self.clone())
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
impl ToCell for &str {
    fn to_cell(self) -> Cell {
        Cell::Str(self.to_string())
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::Str(self.to_string())
    }
}

impl ToCell for NaiveDateTime {
    fn to_cell(self) -> Cell {
        Cell::DateTime(self)
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::DateTime(self.clone())
    }
}

impl<T: ToCell + Default + Clone> ToCell for Option<T> {
    fn to_cell(self) -> Cell {
        match self {
            Some(val) => val.to_cell(),
            None => Cell::Null(Box::new(self.unwrap_or_default().to_cell())),
        }
    }
    fn ref_to_cell(&self) -> Cell {
        match self {
            Some(val) => val.ref_to_cell(),
            None => Cell::Null(Box::new(self.clone().unwrap_or_default().to_cell())),
        }
    }
}

impl<T: ToCell> From<T> for Cell {
    fn from(val: T) -> Self {
        val.to_cell()
    }
}
