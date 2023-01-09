use pg_test_utilities::{PostgresDB, PostgresDBBuilder};

#[tokio::test]
async fn simple_task() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .parse_lossy("info,pg-taskq=trace,simple_task=trace"),
        )
        .init();

    run().await?;

    Ok(())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    const PG_URL: &str = "postgres://robert@localhost:5432/pg_taskq_test_simple_task";
    let db = PostgresDBBuilder::new(PG_URL).start().await?;
    // let db = PostgresDBBuilder::new(PG_URL).keep_db().start().await?;

    tracing::info!("connecting");

    let con = db.pool().await?;

    tracing::info!("done");

    Ok(())
}
