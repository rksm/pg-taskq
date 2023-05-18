use std::pin::Pin;

use futures::{Future, FutureExt};
use pg_setup::PostgresDBBuilder;
use pg_taskq::{TaskBuilder, TaskTableBuilder, Worker};
use sqlx::{Pool, Postgres};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn simple_task() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .parse_lossy("info,pg-taskq=trace,simple_task=trace,sqlx=warn"),
        )
        .init();

    const KEEP: bool = false;

    with_db(KEEP, |pool| {
        Box::pin(async move {
            let tables = TaskTableBuilder::new().base_name("foo_tasks").build();
            assert!(tables.exists(&pool).await?);

            let tables = tables.clone();
            let (stop_tx, stop_rx) = tokio::sync::broadcast::channel(1);

            let worker_task = {
                let pool = pool.clone();
                let tables = tables.clone();
                let types = vec!["Fooo", "Barr"];
                tokio::spawn(Worker::start(
                    pool.clone(),
                    Box::new(tables.clone()),
                    stop_rx,
                    types,
                    move |mut task| {
                        let pool = pool.clone();
                        let tables = tables.clone();
                        async move {
                            tracing::info!("processing task {task:?}");
                            task.in_progress = false;
                            task.done = true;
                            task.save(&pool, &tables).await.map_err(|e| e.to_string())
                        }
                        .boxed()
                    },
                ))
            };

            let mut parent_task = {
                let mut tx = pool.begin().await?;
                let a = TaskBuilder::new("Barr").build(&mut *tx, &tables).await?;
                let b = TaskBuilder::new("Fooo")
                    .with_parent(a.id)
                    .build(&mut *tx, &tables)
                    .await?;
                let _c = TaskBuilder::new("Fooo")
                    .with_parent(b.id)
                    .build(&mut *tx, &tables)
                    .await?;
                tx.commit().await?;
                a
            };

            parent_task.wait_until_done(&pool, &tables, None).await?;
            // let tasks = parent_task.with_children(&pool, &tables).await?;

            tracing::debug!("stopping worker...");
            let _ = stop_tx.send(());

            worker_task.await??;

            Ok(())
        })
    })
    .await?;

    Ok(())
}

async fn with_db(
    keep: bool,
    cb: impl FnOnce(Pool<Postgres>) -> Pin<Box<dyn Future<Output = Result<()>>>>,
) -> Result<()> {
    let pg_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL env var");

    // create the db
    let db = {
        let db = PostgresDBBuilder::new(pg_url);
        let db = if keep { db.keep_db() } else { db };
        db.start().await?
    };

    // create the task tables & run the test
    let result = {
        tracing::info!("connecting");
        let pool = db.pool().await?;

        tracing::info!("creating tables");
        let tables = TaskTableBuilder::new().base_name("foo_tasks").build();
        tables.create(&pool).await.expect("create tables");

        let result = cb(pool.clone()).await;

        tracing::info!("done");

        if !keep {
            tables.drop(&pool).await.expect("drop tables");
        }

        result
    };

    result?;

    Ok(())
}
