use chrono::NaiveDateTime;
use rowboat::dataframe::*;
use serde::Deserialize;

#[derive(Deserialize, ToRow)]
struct Employee {
    id: u32,
    first_name: String,
    last_name: String,
    start_date: String,
    salary: u32,
    department_id: u32,
    active: bool,
}
#[derive(Deserialize, ToRow)]
struct Department {
    id: u32,
    department_name: String,
    budget: u32,
    capacity: u32,
}
pub fn main() {
    // Load data
    let mut dep_df = Dataframe::from_csv::<Department>("./tests/example/department.csv").unwrap();
    let mut emp_df = Dataframe::from_csv::<Employee>("./tests/example/employee.csv").unwrap();

    // Make date-time column from string timestamps
    emp_df
        .add_col(
            "start_date_time",
            emp_df
                .col_values("start_date")
                .unwrap()
                .iter()
                .map(|date_cell| {
                    if let Cell::Str(val) = date_cell {
                        Some(NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%SZ").unwrap())
                    } else {
                        None
                    }
                })
                .collect(),
        )
        .unwrap();

    // Drop string timestamps
    emp_df.drop_cols(["start_date"].into());

    // Make full_name column
    emp_df
        .add_col(
            "full_name",
            emp_df
                .col_slice(["first_name", "last_name"].into())
                .unwrap()
                .iter()
                .map(|row| {
                    match (
                        row.get("first_name").unwrap(),
                        row.get("last_name").unwrap(),
                    ) {
                        (Cell::Str(first), Cell::Str(last)) => format!("{first} {last}"),
                        _ => String::from(""),
                    }
                })
                .collect(),
        )
        .unwrap();

    // Sort by department and start date
    emp_df = emp_df
        .into_sort()
        .sort("department_id", Desc)
        .sort("start_date_time", Desc)
        .collect()
        .unwrap();

    // Display
    dep_df.print();
    emp_df.print();

    // Rename id column for uniqueness and join dataframes together
    dep_df.rename_col("id", "department_id").unwrap();
    let join_df = emp_df
        .join(&dep_df, "department_id", "department_id")
        .unwrap();

    // Filter out non active employee rows
    let mut filter_df = join_df.filter(exp("active", Eq, true)).unwrap();

    // Reshape
    filter_df.retain_cols(
        [
            "id",
            "full_name",
            "start_date_time",
            "salary",
            "department_name",
            "budget",
        ]
        .into(),
    );

    // Display
    filter_df.print();

    // Group by departments
    let mut grouped_df = filter_df
        .group_by("department_name")
        .select("department_name", Coalesce, "department")
        .select("budget", Coalesce, "budget")
        .select("full_name", Count, "employee_count")
        .select("salary", Sum, "expense")
        .to_dataframe()
        .unwrap();

    // Add an over_budget column
    grouped_df
        .add_col(
            "over_budget",
            grouped_df
                .col_slice(["budget", "expense"].into())
                .unwrap()
                .iter()
                .map(
                    |row| match (row.get("budget").unwrap(), row.get("expense").unwrap()) {
                        (Cell::Uint(budget), Cell::Float(expense)) => *expense > *budget as f64,
                        _ => false,
                    },
                )
                .collect(),
        )
        .unwrap();

    // Sort by budget
    grouped_df.sort("budget", Desc).unwrap();

    // Display
    grouped_df.print();

    // Filter and display rows that are over budget
    grouped_df
        .filter(exp("over_budget", Eq, true))
        .unwrap()
        .print();
}
