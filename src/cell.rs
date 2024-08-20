#[derive(PartialEq, Clone)]
pub enum Cell {
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
    pub fn type_string(&self) -> String {
        match self {
            Cell::Int(_) => String::from("Int"),
            Cell::Uint(_) => String::from("Uint"),
            Cell::Str(_) => String::from("Str"),
            Cell::Null => String::from("Null"),
        }
    }
    pub fn is_num(&self) -> bool {
        match self {
            Cell::Int(x) => true,
            Cell::Uint(x) => true,
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

impl<T: ToCell> From<T> for Cell {
    fn from(val: T) -> Self {
        val.to_cell()
    }
}
