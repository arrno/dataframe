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
    pub fn null(&self) -> Self {
        match self {
            Cell::Int(_) => Cell::Null(Box::new(Cell::Int(0))),
            Cell::Uint(_) => Cell::Null(Box::new(Cell::Uint(0))),
            Cell::Str(_) => Cell::Null(Box::new(Cell::Str(String::new()))),
            Cell::Bool(_) => Cell::Null(Box::new(Cell::Bool(false))),
            Cell::Float(_) => Cell::Null(Box::new(Cell::Float(0.0))),
            Cell::DateTime(_) => Cell::Null(Box::new(Cell::DateTime(
                NaiveDate::from_ymd_opt(30, 4, 3)
                    .unwrap()
                    .and_hms_opt(15, 0, 0)
                    .unwrap(),
            ))),
            Cell::Null(_) => self.clone(),
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
    pub fn to_sql(&self) -> String {
        match self {
            Cell::Int(x) => format!("{x}"),
            Cell::Uint(x) => format!("{x}"),
            Cell::Str(x) => format!("'{x}'"),
            Cell::Bool(x) => format!("{x}"),
            Cell::Float(x) => format!("{x}"),
            Cell::DateTime(x) => format!("'{x}'"),
            Cell::Null(_) => String::from("NULL"),
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
            Cell::Float(_) => true,
            _ => false,
        }
    }
    pub fn is_null(&self) -> bool {
        match self {
            Cell::Null(_) => true,
            _ => false,
        }
    }
    pub fn add(&self, with: &Self) -> Option<Self> {
        match self {
            Cell::Int(val) => {
                if let Cell::Int(with_val) = with {
                    Some(Cell::Int(val + with_val))
                } else {
                    None
                }
            }
            Cell::Uint(val) => {
                if let Cell::Uint(with_val) = with {
                    Some(Cell::Uint(val + with_val))
                } else {
                    None
                }
            }
            Cell::Float(val) => {
                if let Cell::Float(with_val) = with {
                    Some(Cell::Float(val + with_val))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    pub fn div_float(&self, with: f64) -> Option<Self> {
        if with == 0.0 {
            return None;
        }
        match self {
            Cell::Int(val) => Some(Cell::Float(*val as f64 / with)),
            Cell::Uint(val) => Some(Cell::Float(*val as f64 / with)),
            Cell::Float(val) => Some(Cell::Float(*val / with)),
            _ => None,
        }
    }
    pub fn add_int(&self, with: &Self) -> Option<Self> {
        match self {
            Cell::Int(val) => match with {
                Cell::Int(with_val) => Some(Cell::Int(val + with_val)),
                Cell::Uint(with_val) => Some(Cell::Int(val + *with_val as i64)),
                Cell::Float(with_val) => Some(Cell::Int(val + *with_val as i64)),
                _ => None,
            },
            Cell::Uint(val) => match with {
                Cell::Int(with_val) => Some(Cell::Int(*val as i64 + with_val)),
                Cell::Uint(with_val) => Some(Cell::Int(*val as i64 + *with_val as i64)),
                Cell::Float(with_val) => Some(Cell::Int(*val as i64 + *with_val as i64)),
                _ => None,
            },
            Cell::Float(val) => match with {
                Cell::Int(with_val) => Some(Cell::Int(*val as i64 + with_val)),
                Cell::Uint(with_val) => Some(Cell::Int(*val as i64 + *with_val as i64)),
                Cell::Float(with_val) => Some(Cell::Int(*val as i64 + *with_val as i64)),
                _ => None,
            },
            _ => None,
        }
    }
    pub fn to_float(&self) -> Cell {
        match self {
            Cell::Int(val) => Cell::Float(*val as f64),
            Cell::Uint(val) => Cell::Float(*val as f64),
            Cell::Float(val) => Cell::Float(*val),
            _ => Cell::Null(Box::new(Cell::Float(0.0))),
        }
    }
    pub fn to_float_val(&self) -> f64 {
        match self {
            Cell::Float(val) => *val,
            Cell::Int(val) => *val as f64,
            Cell::Uint(val) => *val as f64,
            _ => 0.0,
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

pub struct Timestamp(pub i32, pub u32, pub u32, pub u32, pub u32, pub u32);

impl ToCell for Timestamp {
    fn to_cell(self) -> Cell {
        Cell::DateTime(
            NaiveDate::from_ymd_opt(self.0, self.1, self.2)
                .unwrap()
                .and_hms_opt(self.3, self.4, self.5)
                .unwrap(),
        )
    }
    fn ref_to_cell(&self) -> Cell {
        Cell::DateTime(
            NaiveDate::from_ymd_opt(self.0, self.1, self.2)
                .unwrap()
                .and_hms_opt(self.3, self.4, self.5)
                .unwrap(),
        )
    }
}

pub fn cell_is_int(cell: &Cell) -> bool {
    match cell {
        Cell::Int(_) => true,
        Cell::Null(inner) => cell_is_int(inner),
        _ => false,
    }
}
pub fn cell_is_uint(cell: &Cell) -> bool {
    match cell {
        Cell::Uint(_) => true,
        Cell::Null(inner) => cell_is_uint(inner),
        _ => false,
    }
}
pub fn cell_is_str(cell: &Cell) -> bool {
    match cell {
        Cell::Str(_) => true,
        Cell::Null(inner) => cell_is_str(inner),
        _ => false,
    }
}
pub fn cell_is_bool(cell: &Cell) -> bool {
    match cell {
        Cell::Bool(_) => true,
        Cell::Null(inner) => cell_is_bool(inner),
        _ => false,
    }
}
pub fn cell_is_float(cell: &Cell) -> bool {
    match cell {
        Cell::Float(_) => true,
        Cell::Null(inner) => cell_is_float(inner),
        _ => false,
    }
}
pub fn cell_is_date_time(cell: &Cell) -> bool {
    match cell {
        Cell::DateTime(_) => true,
        Cell::Null(inner) => cell_is_date_time(inner),
        _ => false,
    }
}
pub fn cell_is_null(cell: &Cell) -> bool {
    if let Cell::Null(_) = cell {
        true
    } else {
        false
    }
}

pub fn cell_to_type_check(cell: &Cell) -> fn(&Cell) -> bool {
    match cell {
        Cell::Int(_) => cell_is_int,
        Cell::Uint(_) => cell_is_uint,
        Cell::Str(_) => cell_is_str,
        Cell::Bool(_) => cell_is_bool,
        Cell::Float(_) => cell_is_float,
        Cell::DateTime(_) => cell_is_date_time,
        Cell::Null(inner_cell) => cell_to_type_check(&inner_cell),
    }
}

pub fn null_int() -> Cell {
    Cell::Null(Box::new(Cell::Int(0)))
}
pub fn null_uint() -> Cell {
    Cell::Null(Box::new(Cell::Uint(0)))
}
pub fn null_str() -> Cell {
    Cell::Null(Box::new(Cell::Str("".to_string())))
}
pub fn null_bool() -> Cell {
    Cell::Null(Box::new(Cell::Bool(false)))
}
pub fn null_float() -> Cell {
    Cell::Null(Box::new(Cell::Float(0.0)))
}
pub fn null_date() -> Cell {
    Cell::Null(Box::new(Cell::DateTime(
        NaiveDate::from_ymd_opt(30, 4, 3)
            .unwrap()
            .and_hms_opt(15, 0, 0)
            .unwrap(),
    )))
}
