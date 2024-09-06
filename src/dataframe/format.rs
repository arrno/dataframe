use crate::dataslice::*;
use crate::util::*;
use std::cmp::{max, min};

// pub trait Formatter {
//     fn print(&self);
// }

pub struct TableFormatter {
    max_cell_display: usize,
}

impl TableFormatter {
    pub fn new() -> Self {
        TableFormatter {
            max_cell_display: MAX_CELL_DISPLAY,
        }
    }

    fn lengths(&self, df: &DataSlice) -> Vec<usize> {
        let mut lengths = df
            .columns()
            .iter()
            .map(|col| min(self.max_cell_display, col.name().len()))
            .collect::<Vec<usize>>();
        df.columns().iter().enumerate().for_each(|(i, col)| {
            col.values().iter().for_each(|val| {
                lengths[i] = min(
                    self.max_cell_display,
                    max(lengths[i], val.as_string().len()),
                )
            })
        });
        lengths
    }
    fn sep(&self, lengths: &Vec<usize>) -> String {
        (0..lengths.len())
            .map(|i| {
                let s = "-".to_string().repeat(lengths[i] + 2);
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
            .for_each(|(i, col)| print!("| {} ", pad_string(col.name(), lengths[i], false)));
        print!("|\n");
        println!("{sep}+");
        for row in 0..df.length() {
            for col in 0..lengths.len() {
                print!(
                    "| {} ",
                    pad_string(
                        &df.columns()[col].values()[row].as_string(),
                        lengths[col],
                        df.columns()[col].typed().is_num()
                    )
                );
            }
            print!("|\n")
        }
        println!("{sep}+");
    }

    pub fn print(&self, df: &DataSlice) {
        let lengths = self.lengths(df);
        self.do_print(df, &lengths);
    }
}
