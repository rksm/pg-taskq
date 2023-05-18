use sqlx::{postgres::PgPoolOptions, Error, Pool, Postgres};

pub async fn connection_pool(db_url: impl AsRef<str>) -> Result<Pool<Postgres>, Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(db_url.as_ref())
        .await?;
    Ok(pool)
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

use anyhow::Result;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Payload {
    Add(PayloadValue, PayloadValue),
    Mul(PayloadValue, PayloadValue),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum PayloadValue {
    Num(f64),
    Result(uuid::Uuid),
}

impl PayloadValue {
    pub fn value(self, children: &[pg_taskq::Task]) -> Result<f64, String> {
        match self {
            PayloadValue::Num(n) => Ok(n),
            PayloadValue::Result(id) => {
                let child = children
                    .iter()
                    .find(|c| c.id == id)
                    .ok_or_else(|| format!("child task {id} not found"))?;
                Ok(child
                    .result_cloned()?
                    .ok_or_else(|| format!("child has no result ({id}) "))?)
            }
        }
    }
}
