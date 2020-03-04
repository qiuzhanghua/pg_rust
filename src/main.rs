use postgres::{Config, NoTls};
use r2d2::PooledConnection;
use r2d2_postgres::PostgresConnectionManager;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;

fn main() -> std::result::Result<(), Box<dyn Error>> {
    dotenv::dotenv().ok();
    let db_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let config = Config::from_str(&db_url)?;
    let manager = PostgresConnectionManager::new(config, NoTls);
    let pool = Arc::new(r2d2::Pool::builder().max_size(4).build(manager)?);
    let mut conn = pool.get()?;
    for row in conn.query("select * from test", &[])? {
        let v: String = row.get(1);
        println!("{:?}", v);
    }
    Ok(())
}

pub fn query_databases(
    conn: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
) -> std::result::Result<Vec<String>, Box<dyn Error>> {
    Ok(conn
        .query(r##"SELECT datname FROM pg_database;"##, &[])?
        .iter()
        .map(|row| row.get(0))
        .collect())
}

pub fn query_tables(
    conn: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
) -> std::result::Result<Vec<String>, Box<dyn Error>> {
    Ok(conn
        .query(
            r##"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"##,
            &[],
        )?
        .iter()
        .map(|row| row.get(0))
        .collect())
}

/// column_name, data_type, character_maximum_length, is_nullable
pub fn query_columns(
    conn: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
    table_name: &str,
) -> std::result::Result<Vec<(String, String, Option<i32>, bool)>, Box<dyn Error>> {
    if table_name.contains(' ') {
        // 小心SQL注入问题
        return Err(Box::<dyn Error>::from("table name error")); // sample for sql injection
    };
    let qr = conn.query(
        r##"SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
        FROM information_schema.columns WHERE table_schema = 'public' and table_name = $1;"##,
        &[&table_name],
    )?;

    let cols = qr
        .iter()
        .map(|row| {
            let column_name: &str = row.get(0);
            let data_type: &str = row.get(1);
            let character_maximum_length = match row.try_get::<usize, i32>(2) {
                Ok(v) => Some(v),
                Err(_e) => None,
            };
            let is_nullable = match row.get::<usize, &str>(3) {
                "NO" => false,
                _ => true,
            };
            (
                column_name.to_string(),
                data_type.to_string(),
                character_maximum_length,
                is_nullable,
            )
        })
        .collect();
    Ok(cols)
}

#[cfg(test)]
mod tests {
    use crate::{query_columns, query_databases, query_tables};
    use postgres::{Config, NoTls};
    use r2d2_postgres::PostgresConnectionManager;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn test_databases() {
        dotenv::dotenv().ok();
        let db_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let config = Config::from_str(&db_url).unwrap();
        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Arc::new(r2d2::Pool::builder().max_size(4).build(manager).unwrap());
        let mut conn = pool.get().unwrap();
        let dbs = query_databases(&mut conn);
        if let Ok(x) = dbs {
            println!("{:?}", x);
        };
    }

    #[test]
    fn test_tables() {
        dotenv::dotenv().ok();
        let db_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let config = Config::from_str(&db_url).unwrap();
        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Arc::new(r2d2::Pool::builder().max_size(4).build(manager).unwrap());
        let mut conn = pool.get().unwrap();
        let table_names = query_tables(&mut conn);
        if let Ok(x) = table_names {
            println!("{:?}", x);
        } else {
            assert_eq!(false, table_names.is_err())
        }
    }

    #[test]
    fn test_columns() {
        dotenv::dotenv().ok();
        let db_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let config = Config::from_str(&db_url).unwrap();
        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Arc::new(r2d2::Pool::builder().max_size(4).build(manager).unwrap());
        let mut conn = pool.get().unwrap();
        let table_names = query_columns(&mut conn, "users");
        if let Ok(x) = table_names {
            println!("{:?}", x);
        } else {
            assert_eq!(false, table_names.is_err())
        }
    }
}
