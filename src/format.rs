use crate::util::*;
use crate::DataSlice;
use std::cmp::{max, min};

pub trait formatter {
    fn print(&self);
}

pub struct Formatter {
    max_cell_display: usize,
}

impl Formatter {
    pub fn new(max_cell: usize) -> Self {
        Formatter {
            max_cell_display: max_cell,
        }
    }

    fn lengths(&self, df: &DataSlice) -> Vec<usize> {
        df.columns()
            .iter()
            .map(|col| min(self.max_cell_display, col.name().len()))
            .collect()
    }
    fn sep(&self, lengths: &Vec<usize>) -> String {
        (0..lengths.len())
            .map(|i| {
                let s = "-".to_string().repeat(lengths[i]);
                format!("+{s}")
            })
            .collect::<Vec<String>>()
            .join("")
    }

    fn do_print(&self, df: &DataSlice, lengths: &Vec<usize>) {
        let sep = self.sep(&lengths);
        println!("{sep}+");
        df.columns()
            .iter()
            .enumerate()
            .for_each(|(i, col)| print!("|{}", pad_string(col.name(), lengths[i])));
        print!("|\n");
        println!("{sep}+");
        for row in 0..df.length() {
            for col in 0..lengths.len() {
                print!(
                    "|{}",
                    pad_string(&df.columns()[col].values()[row].as_string(), lengths[col])
                );
            }
            print!("|\n")
        }
        println!("{sep}+");
    }

    pub fn print(&self, df: &DataSlice) {
        let mut lengths = self.lengths(df);
        // Calc col sizes
        df.columns().iter().enumerate().for_each(|(i, col)| {
            col.values().iter().for_each(|val| {
                lengths[i] = min(
                    self.max_cell_display,
                    max(lengths[i], val.as_string().len()),
                )
            })
        });
        self.do_print(df, &lengths);
    }
}
