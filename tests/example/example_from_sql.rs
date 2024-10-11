use rowboat::dataframe::*;
use serde::Deserialize;
use sqlx::mysql::MySqlPoolOptions;

// Using rowboat to extend/duplicate a SQL table

#[derive(Deserialize, sqlx::FromRow, ToRow)]
struct User {
    id: i32,
    first_name: String,
    last_ame: String,
    city: String,
}

pub async fn main() -> Result<(), sqlx::Error> {
    // Make SQL connection
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect("mysql://root:password@localhost/mydb")
        .await?;

    // Query table into Vec<User>
    let users = sqlx::query_as::<_, User>("SELECT * FROM user WHERE id < ?")
        .bind(100)
        .fetch_all(&pool)
        .await
        .unwrap();

    // Make df from structs
    let df = Dataframe::from_structs(users).unwrap();
    df.print();

    // Repeat the table rows x3
    (0..3).for_each(|_| {
        let clone_df = df.to_slice().to_dataframe();
        df.concat(clone_df).unwrap();
    });

    // Get non id column names
    let columns = HashSet::from_iter(df.col_names().into_iter().filter(|&name| name != "id"));

    // Make query from dataframe slice
    let (query_string, args) = df.col_slice(columns).unwrap().to_sql("user");

    // Prep query and bind args
    let mut query = sqlx::query(&query_string);
    for arg in args.iter() {
        query = query.bind(arg);
    }

    // Execute
    query.execute(&pool).await.unwrap();

    Ok(())
}
