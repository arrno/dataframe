use crate::cell::*;
use regex::Regex;
use std::collections::HashMap;

#[derive(Debug)]
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
                        Op::GtEq => a >= v,
                        Op::LtEq => a <= v,
                        Op::Mod(i) => a % i == *v,
                        Op::Regex => false,
                    }
                } else {
                    match self.op {
                        Op::Neq => true,
                        _ => false,
                    }
                }
            }
            Cell::Uint(v) => {
                if let Cell::Uint(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => a > v,
                        Op::Lt => a < v,
                        Op::GtEq => a >= v,
                        Op::LtEq => a <= v,
                        Op::Mod(i) => *a as i64 % i == *v as i64,
                        Op::Regex => false,
                    }
                } else {
                    match self.op {
                        Op::Neq => true,
                        _ => false,
                    }
                }
            }
            Cell::Str(v) => {
                if let Cell::Str(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => a > v,
                        Op::Lt => a < v,
                        Op::GtEq => a >= v,
                        Op::LtEq => a <= v,
                        Op::Mod(_) => false,
                        Op::Regex => {
                            let re = match Regex::new(v) {
                                Ok(r) => r,
                                Err(_) => return true, // if not valid regex, do not filter
                            };
                            if let Some(_) = re.captures(a) {
                                true
                            } else {
                                false
                            }
                        }
                    }
                } else {
                    match self.op {
                        Op::Neq => true,
                        _ => false,
                    }
                }
            }
            Cell::Bool(v) => {
                if let Cell::Bool(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => !v && *a,
                        Op::Lt => !a && *v,
                        Op::GtEq => v == a || !v && *a,
                        Op::LtEq => v == a || !a && *v,
                        Op::Mod(_) => false,
                        Op::Regex => false,
                    }
                } else {
                    match self.op {
                        Op::Neq => true,
                        _ => false,
                    }
                }
            }
            Cell::Float(v) => {
                if let Cell::Float(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => a > v,
                        Op::Lt => a < v,
                        Op::GtEq => a >= v,
                        Op::LtEq => a <= v,
                        Op::Mod(i) => *a % i as f64 == *v,
                        Op::Regex => false,
                    }
                } else {
                    match self.op {
                        Op::Neq => true,
                        _ => false,
                    }
                }
            }
            Cell::DateTime(v) => {
                if let Cell::DateTime(a) = against {
                    match self.op {
                        Op::Eq => v == a,
                        Op::Neq => v != a,
                        Op::Gt => a > v,
                        Op::Lt => a < v,
                        Op::GtEq => a >= v,
                        Op::LtEq => a <= v,
                        Op::Mod(_) => false,
                        Op::Regex => false,
                    }
                } else {
                    match self.op {
                        Op::Neq => true,
                        _ => false,
                    }
                }
            }
            Cell::Null(_) => {
                if let Cell::Null(_) = against {
                    match self.op {
                        Op::Eq => true,
                        _ => false,
                    }
                } else {
                    match self.op {
                        Op::Neq => true,
                        _ => false,
                    }
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
    Not(Box<Exp>),
}

pub fn and(vexp: Vec<Exp>) -> Exp {
    Exp::And(And::new(vexp))
}
pub fn or(vexp: Vec<Exp>) -> Exp {
    Exp::Or(Or::new(vexp))
}
pub fn exp<T: ToCell>(target: &str, op: Op, val: T) -> Exp {
    Exp::ExpU(ExpU::new(target.to_string(), op, val))
}
pub fn not(exp: Exp) -> Exp {
    Exp::Not(Box::new(exp))
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
            Self::Not(ex) => !ex.evaluate(against),
        }
    }
}

#[derive(Debug)]
pub enum Op {
    Eq,
    Neq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Regex,
    Mod(i64),
}

pub fn eq() -> Op {
    Op::Eq
}
pub fn neq() -> Op {
    Op::Neq
}
pub fn gt() -> Op {
    Op::Gt
}
pub fn lt() -> Op {
    Op::Lt
}
pub fn gte() -> Op {
    Op::GtEq
}
pub fn lte() -> Op {
    Op::LtEq
}
pub fn modl(i: i64) -> Op {
    Op::Mod(i)
}
pub fn regx() -> Op {
    Op::Regex
}
