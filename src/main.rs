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
    for row in conn.query("select *  from test", &[])? {
        let v: String = row.get(1);
        println!("{:?}", v);
    }
    Ok(())
}

pub fn query_databases(
    conn: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
) -> std::result::Result<Vec<String>, Box<dyn Error>> {
    let qr = conn.query(r##"SELECT datname FROM pg_database;"##, &[])?;
    let mut dbs = Vec::<String>::new();
    for row in qr {
        dbs.push(row.get(0));
    }
    Ok(dbs)
}

#[cfg(test)]
mod tests {
    use super::query_databases;
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
}
