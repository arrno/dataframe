use rowboat::dataframe::*;
use serde::Deserialize;
use sqlx::mysql::MySqlPoolOptions;

#[derive(Deserialize, sqlx::FromRow, ToRow)]
struct User {
    id: i32,
    first_name: String,
    last_ame: String,
    city: String,
}

pub async fn main() -> Result<(), sqlx::Error> {
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect("mysql://root:password@localhost/mydb")
        .await?;

    let users = sqlx::query_as::<_, User>("SELECT * FROM user WHERE id < ?")
        .bind(100)
        .fetch_all(&pool)
        .await
        .unwrap();

    let df = Dataframe::from_structs(users).unwrap();
    df.print();

    Ok(())
}
