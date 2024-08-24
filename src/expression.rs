use crate::cell::*;
use std::collections::HashMap;

pub struct ExpU {
    target: String,
    op: Op,
    value: Cell,
}

impl ExpU {
    pub fn new<T>(target: String, op: Op, val: T) -> Self
    where
        T: ToCell,
    {
        ExpU {
            target: target,
            op: op,
            value: val.to_cell(),
        }
    }
    pub fn target(&self) -> &String {
        &self.target
    }

    pub fn eval(&self, against: &Cell) -> bool {
        match &self.value {
            Cell::Int(v) => {
                if let Cell::Int(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => a > v,
                        Op::Lt => a < v,
                        Op::IsNull => false,
                        Op::NotNull => true,
                    }
                } else {
                    false
                }
            }
            Cell::Uint(v) => {
                if let Cell::Uint(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => a > v,
                        Op::Lt => a < v,
                        Op::IsNull => false,
                        Op::NotNull => true,
                    }
                } else {
                    false
                }
            }
            Cell::Str(v) => {
                if let Cell::Str(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => a > v,
                        Op::Lt => a < v,
                        Op::IsNull => false,
                        Op::NotNull => true,
                    }
                } else {
                    false
                }
            }
            Cell::Bool(v) => {
                if let Cell::Bool(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => *v && !a,
                        Op::Lt => *a && !v,
                        Op::IsNull => false,
                        Op::NotNull => true,
                    }
                } else {
                    false
                }
            }
            Cell::Float(v) => {
                if let Cell::Float(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => a > v,
                        Op::Lt => a < v,
                        Op::IsNull => false,
                        Op::NotNull => true,
                    }
                } else {
                    false
                }
            }
            Cell::DateTime(v) => {
                if let Cell::DateTime(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => a > v,
                        Op::Lt => a < v,
                        Op::IsNull => false,
                        Op::NotNull => true,
                    }
                } else {
                    false
                }
            }
            Cell::Null => {
                if let Cell::Null = against {
                    match self.op {
                        Op::IsNull => true,
                        _ => false,
                    }
                } else {
                    false
                }
            }
        }
    }
}

pub struct Or {
    vexp: Vec<Exp>,
}
impl Or {
    pub fn new(vexp: Vec<Exp>) -> Self {
        Or { vexp }
    }
}
pub struct And {
    vexp: Vec<Exp>,
}

impl And {
    pub fn new(vexp: Vec<Exp>) -> Self {
        And { vexp }
    }
}
pub enum Exp {
    Or(Or),
    And(And),
    ExpU(ExpU),
}

pub fn ExpAnd(vexp: Vec<Exp>) -> Exp {
    Exp::And(And::new(vexp))
}
pub fn ExpOr(vexp: Vec<Exp>) -> Exp {
    Exp::Or(Or::new(vexp))
}
pub fn Exp<T: ToCell>(target: &str, op: Op, val: T) -> Exp {
    Exp::ExpU(ExpU::new(target.to_string(), op, val))
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
    pub fn flatten(&mut self) -> Vec<&mut ExpU> {
        match self {
            Self::ExpU(ex) => vec![ex],
            Self::Or(ex) => ex.vexp.iter_mut().map(|e| e.flatten()).flatten().collect(),
            Self::And(ex) => ex.vexp.iter_mut().map(|e| e.flatten()).flatten().collect(),
        }
    }
}

pub enum Op {
    Eq,
    Neq,
    Gt,
    Lt,
    IsNull,
    NotNull,
}

pub fn Eq() -> Op {
    Op::Eq
}
pub fn Neq() -> Op {
    Op::Neq
}
pub fn Gt() -> Op {
    Op::Gt
}
pub fn Lt() -> Op {
    Op::Lt
}
pub fn IsNull() -> Op {
    Op::IsNull
}
pub fn NotNull() -> Op {
    Op::NotNull
}
