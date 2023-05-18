use anyhow::Result;
use clap::Parser;
use futures::FutureExt;
use sqlx::{Pool, Postgres};

use crate::shared::{Payload, PayloadValue};

#[derive(Parser)]
pub struct Args {
    #[clap(long, short, action)]
    keep_tasks: bool,
}

pub async fn run(db_url: &str, args: Args) -> Result<()> {
    tracing::info!("step 1: setup");
    let (_builder, pool, tables) = setup(db_url).await.expect("setup");

    tracing::info!("step 2: starting tasks");
    let mut root_task = start_tasks(pool.clone(), tables.clone())
        .await
        .expect("start tasks");

    tracing::info!("step 3: waiting for tasks");

    let waiting = if args.keep_tasks {
        root_task.wait_until_done(&pool, &tables, None).boxed()
    } else {
        root_task
            .wait_until_done_and_delete(&pool, &tables, None)
            .boxed()
    };
    waiting.await.expect("wait for tasks");

    tracing::info!("____-------=== done ===-------____");

    Ok(())
}

async fn setup(
    db_uri: &str,
) -> Result<(pg_setup::PostgresDB, Pool<Postgres>, pg_taskq::TaskTables)> {
    // ensure that the postgres db exists (will create it if not)
    tracing::info!("setup db");
    let builder = {
        pg_setup::PostgresDBBuilder::new(db_uri)
            .keep_db()
            .start()
            .await?
    };

    // ensure that the tables used for the task queue exist
    tracing::info!("setup taskq tables");
    let pool = crate::shared::connection_pool(&db_uri).await?;
    let tables = pg_taskq::TaskTableBuilder::new()
        .base_name("foo_tasks")
        .build();
    tables.create(&pool).await.expect("create tables");

    Ok((builder, pool, tables))
}

async fn start_tasks(pool: Pool<Postgres>, tables: pg_taskq::TaskTables) -> Result<pg_taskq::Task> {
    let mut tx = pool.begin().await?;
    let a_id = uuid::Uuid::new_v4();
    let b_id = uuid::Uuid::new_v4();
    let c_id = uuid::Uuid::new_v4();
    let a = pg_taskq::TaskBuilder::new("add")
        .with_id(a_id)
        .with_request(Payload::Add(
            PayloadValue::Result(b_id),
            PayloadValue::Result(c_id),
        ))?
        .build(&mut *tx, &tables)
        .await?;
    let _b = pg_taskq::TaskBuilder::new("mul")
        .with_id(b_id)
        .with_parent(a_id)
        .with_request(Payload::Mul(PayloadValue::Num(2.0), PayloadValue::Num(3.0)))?
        .build(&mut *tx, &tables)
        .await?;
    let _c = pg_taskq::TaskBuilder::new("add")
        .with_id(c_id)
        .with_request(Payload::Mul(PayloadValue::Num(5.0), PayloadValue::Num(4.0)))?
        .with_parent(a.id)
        .build(&mut *tx, &tables)
        .await?;
    tx.commit().await?;
    Ok(a)
}
