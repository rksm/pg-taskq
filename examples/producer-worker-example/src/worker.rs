use anyhow::Result;
use futures::FutureExt;

use clap::Parser;

use crate::shared::Payload;

#[derive(Parser)]
pub struct Args {
    #[clap(long = "type", short, value_enum)]
    task_type: TaskType,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum TaskType {
    Add,
    Mul,
    Both,
}

pub async fn run(db_url: &str, args: Args) -> Result<()> {
    tracing::info!("step 1: create task table");

    let pool = crate::shared::connection_pool(&db_url).await?;
    let tables = pg_taskq::TaskTableBuilder::new()
        .base_name("foo_tasks")
        .build();

    pg_taskq::fixup_stale_tasks(&pool, &tables, std::time::Duration::from_secs(60 * 60)).await?;

    tracing::info!("step 2: starting worker");

    let (stop_tx, stop_rx) = tokio::sync::broadcast::channel(1);

    let worker_task = {
        let pool = pool.clone();
        let tables = tables.clone();
        let types = match args.task_type {
            TaskType::Add => vec!["add"],
            TaskType::Mul => vec!["mul"],
            TaskType::Both => vec!["add", "mul"],
        };
        tokio::spawn(pg_taskq::Worker::start(
            pool.clone(),
            Box::new(tables.clone()),
            stop_rx,
            types,
            move |mut task| {
                let pool = pool.clone();
                let tables = tables.clone();
                async move {
                    tracing::info!("processing task {task:?}");

                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

                    let children = task.children(&pool, &tables, false).await?;
                    let req = task.request::<Payload>()?;
                    let result = match req {
                        Payload::Add(a, b) => {
                            let a = a.value(&children)?;
                            let b = b.value(&children)?;
                            a + b
                        }
                        Payload::Mul(a, b) => {
                            let a = a.value(&children)?;
                            let b = b.value(&children)?;
                            a * b
                        }
                    };

                    task.fullfill(&pool, &tables, Some(result.into()), None)
                        .await?;

                    Ok(())
                }
                .boxed()
            },
        ))
    };

    ctrlc::set_handler(move || {
        tracing::info!("received ctrl-c, stopping worker");
        stop_tx.send(()).unwrap();
    })?;

    worker_task.await??;

    tracing::info!("done");

    Ok(())
}
