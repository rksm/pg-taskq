use futures::FutureExt;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

use psql_tasks_rs::{
    setup::TaskSetup, task::Task, task_type::TaskTypeString, worker::Worker, Error,
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
                .parse_lossy("debug,trace=psql_tasks_rs,sqlx=warn"),
        )
        .init();
    dotenv::dotenv().expect(".env");

    let db = std::env::var("DATABASE_URL").unwrap();
    let pool = connection_pool(&db).await?;

    // TaskSetup.cleanup(&pool).await?;

    let setup = TaskSetup::build(&pool).await?;
    let result = run(pool.clone()).await;
    setup.cleanup(&pool).await?;
    result
}

async fn run(pool: Pool<Postgres>) -> Result<()> {
    let (stop_tx, stop_rx) = tokio::sync::broadcast::channel(1);

    let worker_task = {
        let pool = pool.clone();
        let types = vec![TaskTypeString::from("Fooo")];
        tokio::spawn(Worker::start(pool, stop_rx, types, move |task| {
            async move {
                tracing::info!("processing task {task:?}");
                Ok(())
            }
            .boxed()
        }))
    };

    let create_task = tokio::spawn(async move {
        Task::create_task(TaskTypeString::from("Fooo"), (), &pool, None).await?;
        Ok::<_, Error>(())
    });

    std::thread::sleep(std::time::Duration::from_secs(1));

    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        tracing::debug!("stopping worker...");
        let _ = stop_tx.send(());
    });

    let (_, _) = tokio::try_join!(create_task, worker_task)?;

    Ok(())
}
