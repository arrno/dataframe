use std::cmp::Ordering;
use std::collections::HashMap;

use crate::dataframe::Dataframe;
use crate::util;

#[derive(Debug)]
pub enum SortOrder {
    Asc,
    Desc,
}
pub fn asc() -> SortOrder {
    SortOrder::Asc
}
pub fn desc() -> SortOrder {
    SortOrder::Desc
}

#[derive(Debug)]
pub struct Sort {
    by: String,
    dir: SortOrder,
}
pub struct DataSort {
    dataframe: Dataframe,
    instructions: Vec<Sort>,
}

impl DataSort {
    pub fn new(df: Dataframe) -> Self {
        DataSort {
            dataframe: df,
            instructions: vec![],
        }
    }
    pub fn sort(self, by: &str, order: SortOrder) -> Self {
        let mut new_sort = DataSort {
            dataframe: self.dataframe,
            instructions: self.instructions,
        };
        if let Some(_) = new_sort
            .dataframe
            .col_names()
            .iter()
            .find(|name| name == &&by)
        {
            new_sort.instructions.push(Sort {
                by: by.to_string(),
                dir: order,
            });
        }
        new_sort
    }
    pub fn collect(self) -> Result<Dataframe, util::Error> {
        let (labels, mut rows) = self.dataframe.to_rows();
        let label_indexes = labels
            .iter()
            .enumerate()
            .map(|(i, l)| (l, i))
            .collect::<HashMap<&String, usize>>();
        if self.instructions.len() > 0 {
            rows.sort_by(|cur, prev| {
                let mut ord = Ordering::Greater;
                for inst in self.instructions.iter() {
                    let label_idx = label_indexes.get(&inst.by).unwrap();
                    let (cur_cell, prev_cell) =
                        (cur.get(*label_idx).unwrap(), prev.get(*label_idx).unwrap());
                    if cur_cell != prev_cell {
                        ord = match inst.dir {
                            SortOrder::Asc => {
                                if cur_cell > prev_cell {
                                    Ordering::Greater
                                } else {
                                    Ordering::Less
                                }
                            }
                            SortOrder::Desc => {
                                if cur_cell > prev_cell {
                                    Ordering::Less
                                } else {
                                    Ordering::Greater
                                }
                            }
                        };
                        break;
                    }
                }
                ord
            });
        }
        Dataframe::from_rows(labels.iter().map(|l| l.as_str()).collect(), rows)
    }
}
