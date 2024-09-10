use crate::dataslice::*;
use crate::util::*;
use std::cmp::{max, min};
use std::collections::HashSet;

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

    fn do_print(&self, df: &DataSlice, lengths: &Vec<usize>, abbreviated: bool) {
        let sep = self.sep(&lengths);
        if abbreviated {
            println!("{sep}+---");
        } else {
            println!("{sep}+");
        }
        df.columns()
            .iter()
            .enumerate()
            .for_each(|(i, col)| print!("| {} ", pad_string(col.name(), lengths[i], false)));
        if abbreviated {
            print!("| ..\n");
        } else {
            print!("|\n");
        }
        if abbreviated {
            println!("{sep}+---");
        } else {
            println!("{sep}+");
        }
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
            if abbreviated {
                print!("| ..\n");
            } else {
                print!("|\n");
            }
        }
        if abbreviated {
            println!("{sep}+---");
        } else {
            println!("{sep}+");
        }
    }

    pub fn print(&self, df: &DataSlice) {
        match df.columns().len() {
            0..=MAX_COL_DISPLAY => {
                let lengths = self.lengths(df);
                self.do_print(df, &lengths, false);
            }
            _ => {
                let abbr_df = &df
                    .col_slice(
                        df.col_names()[0..8]
                            .iter()
                            .map(|st| *st)
                            .collect::<HashSet<&str>>(),
                    )
                    .unwrap();
                let lengths = self.lengths(abbr_df);
                self.do_print(abbr_df, &lengths, true);
            }
        }
    }
}
