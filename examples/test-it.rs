#[macro_use]
extern crate tracing;

use futures::FutureExt;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

use psql_tasks_rs::{
    setup::{TaskTableBuilder, TaskTables},
    task::Task,
    task_type::TaskTypeString,
    worker::Worker,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn connection_pool(db_url: impl AsRef<str>) -> Result<Pool<Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(db_url.as_ref())
        .await?;
    Ok(pool)
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .parse_lossy("debug,psql_tasks_rs=trace,sqlx=warn"),
        )
        .init();
    dotenv::dotenv().expect(".env");

    let db = std::env::var("DATABASE_URL").unwrap();
    let pool = connection_pool(&db).await?;

    let tables = TaskTableBuilder::new().base_name("foo_tasks").build();
    tables.create(&pool).await.expect("create tables");

    let result = run(pool.clone(), tables.clone()).await;

    tables.drop(&pool).await.expect("drop tables");

    result
}

async fn run(pool: Pool<Postgres>, tables: TaskTables) -> Result<()> {
    let (stop_tx, stop_rx) = tokio::sync::broadcast::channel(1);

    let worker_task = {
        let pool = pool.clone();
        let tables = tables.clone();
        let types = ["Fooo", "Barr"].map(TaskTypeString::from).into();
        tokio::spawn(Worker::start(
            pool.clone(),
            Box::new(tables.clone()),
            stop_rx,
            types,
            move |mut task| {
                let pool = pool.clone();
                let tables = tables.clone();
                async move {
                    info!("processing task {task:?}");
                    task.in_progress = false;
                    task.done = true;
                    task.save(&pool, &tables).await?;
                    Ok(())
                }
                .boxed()
            },
        ))
    };

    let mut parent_task = {
        let mut tx = pool.begin().await?;
        let parent =
            Task::create_task(&mut *tx, &tables, TaskTypeString::from("Barr"), (), None).await?;
        Task::create_task(
            &mut *tx,
            &tables,
            TaskTypeString::from("Fooo"),
            (),
            Some(parent.id),
        )
        .await?;
        tx.commit().await?;
        parent
    };

    parent_task.wait_until_done(&pool, &tables, None).await?;
    let tasks = parent_task.with_children(&pool, &tables).await?;

    debug!("stopping worker...");
    let _ = stop_tx.send(());

    worker_task.await??;

    Ok(())
}
