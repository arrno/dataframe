use core::fmt;

pub const MAX_CELL_DISPLAY: usize = 20;
pub const MAX_COL_DISPLAY: usize = 8;

#[derive(Debug, Clone)]
pub struct Error {
    reason: String,
}
impl Error {
    pub fn new(reason: String) -> Self {
        Error { reason }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.reason)
    }
}

pub fn pad_string(s: &str, w: usize, left: bool) -> String {
    if s.len() == w {
        return s.to_string();
    } else if s.len() > w {
        return format!("{}..", s[..(w - 2)].to_string());
    }
    let spaces = " ".to_string().repeat(w - s.len());
    match left {
        false => format!("{s}{spaces}"),
        true => format!("{spaces}{s}"),
    }
}
