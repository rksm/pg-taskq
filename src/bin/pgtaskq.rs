use clap::Parser;
use pg_taskq::TaskTableProvider;

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    cmd: SubCommand,

    #[clap(
        long = "db",
        help = "The postgres database connection to use, in the foramt postgres://user:pass@host:port/dbname",
        env = "PGTASKQ_DB"
    )]
    database_url: String,

    #[clap(
        long,
        action,
        default_value = "false",
        help = "If the database referenced by PGTASKQ_DB does not exist, attempt to create it"
    )]
    create_db: bool,

    #[clap(long, help = "The postgres schema name", default_value = "public")]
    schema: String,

    #[clap(
        long,
        short = 'n',
        help = "The base table name for postgres tables and views",
        env = "PGTASKQ_TABLES"
    )]
    base_name: String,
}

#[derive(Parser)]
enum SubCommand {
    Install(Install),
    Uninstall(Uninstall),

    List(List),
    Delete(Delete),
    Fixup(Fixup),

    Submit(Submit),
    Fullfill(Fullfill),
}

#[derive(Parser)]
#[clap(about = "Creates tables, views, triggers, functions for the task queue")]
struct Install {}

#[derive(Parser)]
#[clap(about = "Removes tables, views, triggers, functions for the task queue")]
struct Uninstall {}

#[derive(Parser)]
#[clap(about = "Lists tasks")]
struct List {
    #[clap(long, value_parser = humantime::parse_duration, help = "The base table name for postgres tables and views")]
    newer: Option<std::time::Duration>,

    #[clap(long, default_value = "false", help = "Only list tasks not done")]
    only_pending: bool,
}

#[derive(Parser)]
#[clap(about = "Delete tasks from the queue")]
struct Delete {
    #[clap()]
    id: uuid::Uuid,

    #[clap(
        long,
        default_value = "false",
        action,
        help = "Force delete task, even if not done"
    )]
    force: bool,
}

#[derive(Parser)]
#[clap(about = "Submit a new task to the queue")]
struct Submit {
    #[clap(long)]
    parent: Option<uuid::Uuid>,

    #[clap(long = "type", short = 't')]
    task_type: String,

    #[clap(long, short, help = "The request JSON to be processed.")]
    request: Option<serde_json::Value>,

    #[clap(
        long,
        short,
        action,
        default_value = "false",
        help = "Wait for task to complete"
    )]
    wait: bool,

    #[clap(
        long,
        action,
        default_value = "false",
        help = "Delete the task after it completes (implies --wait)",
        requires = "wait"
    )]
    delete: bool,
}

#[derive(Parser)]
#[clap(about = "Mark a task as done")]
struct Fullfill {
    #[clap()]
    id: uuid::Uuid,

    #[clap(long, short, help = "The result JSON.")]
    result: Option<serde_json::Value>,

    #[clap(long, short, help = "The error JSON.", conflicts_with = "result")]
    error: Option<serde_json::Value>,
}

#[derive(Parser)]
#[clap(
    about = "Any task that is marked as in_progress but has not been updated since duration will be marked as not in_progress"
)]
struct Fixup {
    #[clap(value_parser = humantime::parse_duration)]
    duration: std::time::Duration,
}

#[tokio::main]
async fn main() {
    let level = if cfg!(debug_assertions) {
        "debug,pg_taskq=debug,sqlx=warn"
    } else {
        "info,pg_taskq=debug,sqlx=warn"
        // "info,pg_taskq=info,sqlx=warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::builder().parse_lossy(level))
        .init();

    let args = Args::parse();

    if args.create_db {
        pg_setup::PostgresDBBuilder::new(&args.database_url)
            .keep_db()
            .start()
            .await
            .expect("unable to create database");
    }

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&args.database_url)
        .await
        .expect("connect to database");

    let tables = pg_taskq::TaskTableBuilder::new()
        .base_name(args.base_name)
        .schema_name(args.schema)
        .build();

    match args.cmd {
        SubCommand::Install(_cmd) => {
            tables.create(&pool).await.expect("create tables");
        }

        SubCommand::Uninstall(_cmd) => {
            tables.drop(&pool).await.expect("drop tables");
        }

        SubCommand::List(cmd) => {
            let tables: &dyn TaskTableProvider = &tables;
            let table = tables.tasks_table_full_name();

            let List {
                newer,
                only_pending,
            } = cmd;

            let mut where_clause = Vec::new();
            if only_pending {
                where_clause.push("done = false".to_string());
            }

            if let Some(newer) = newer {
                where_clause.push(format!(
                    "updated_at >= now() - interval '{}'",
                    newer.as_secs()
                ));
            }

            let sql = format!("SELECT * FROM {table} ORDER BY updated_at DESC");
            let tasks: Vec<pg_taskq::Task> = sqlx::query_as(&sql)
                .fetch_all(&pool)
                .await
                .expect("fetch tasks");

            {
                use prettytable::{Cell, Row, Table};

                let mut table = Table::new();

                let header = Row::new(vec![
                    Cell::new("id"),
                    Cell::new("parent"),
                    Cell::new("created_at"),
                    Cell::new("updated_at"),
                    Cell::new("task_type"),
                    Cell::new("request"),
                    Cell::new("result"),
                    Cell::new("error"),
                    Cell::new("in_progress"),
                    Cell::new("done"),
                ]);
                table.set_titles(header);

                for task in tasks {
                    let created_at_secs = (chrono::Utc::now() - task.created_at).num_seconds();
                    let updated_at_secs = (chrono::Utc::now() - task.updated_at).num_seconds();
                    let relative_created_at = humantime::format_duration(
                        std::time::Duration::from_secs(created_at_secs as u64),
                    );
                    let relative_updated_at = humantime::format_duration(
                        std::time::Duration::from_secs(updated_at_secs as u64),
                    );

                    table.add_row(Row::new(vec![
                        Cell::new(&task.id.to_string()),
                        Cell::new(&task.parent.map(|id| id.to_string()).unwrap_or_default()),
                        Cell::new(&format!("{}\n{} ago", task.created_at, relative_created_at)),
                        Cell::new(&format!("{}\n{} ago", task.updated_at, relative_updated_at)),
                        Cell::new(&task.task_type),
                        Cell::new(
                            &task
                                .request
                                .map(|r| {
                                    serde_json::to_string_pretty(&r).unwrap_or_else(|_| {
                                        "<Unable to JSON parse request>".to_string()
                                    })
                                })
                                .unwrap_or_default(),
                        ),
                        Cell::new(&task.result.map(|r| r.to_string()).unwrap_or_default()),
                        Cell::new(&task.error.map(|e| e.to_string()).unwrap_or_default()),
                        Cell::new(&task.in_progress.to_string()),
                        Cell::new(&task.done.to_string()),
                    ]));
                }

                table.printstd();
            }
        }

        SubCommand::Delete(cmd) => {
            let tables: &dyn TaskTableProvider = &tables;

            let task = pg_taskq::Task::load(&pool, tables, cmd.id)
                .await
                .expect("load task");

            match task {
                None => {
                    println!("Task with id {} not found", cmd.id);
                    std::process::exit(1);
                }
                Some(task) => {
                    if !task.done && !cmd.force {
                        println!(
                            "Task with id {} is not done, use --force to delete anyway",
                            cmd.id
                        );
                        std::process::exit(1);
                    }

                    task.delete(&pool, tables).await.expect("delete task");
                }
            }
        }

        SubCommand::Fixup(cmd) => {
            pg_taskq::fixup_stale_tasks(&pool, &tables, cmd.duration)
                .await
                .expect("fixup tasks");
        }

        SubCommand::Submit(cmd) => {
            let Submit {
                parent,
                task_type,
                request,
                wait,
                delete,
            } = cmd;

            let mut task =
                pg_taskq::Task::create_task(&pool, &tables, None, task_type, request, parent)
                    .await
                    .expect("create task");

            println!("Created task with id {}", task.id);

            if wait {
                task.wait_until_done(&pool, &tables, Some(std::time::Duration::from_secs(10)))
                    .await
                    .expect("wait for task");

                task.update(&pool, &tables).await.expect("update task");

                let mut failed = false;
                match (&task.error, &task.result) {
                    (Some(error), _) => {
                        failed = true;
                        let pretty = serde_json::to_string_pretty(&error)
                            .unwrap_or_else(|_| "<Unable to JSON parse error>".to_string());
                        println!("Task failed with error {}", pretty);
                    }
                    (_, Some(result)) => {
                        let pretty = serde_json::to_string_pretty(&result)
                            .unwrap_or_else(|_| "<Unable to JSON parse result>".to_string());
                        println!("Task completed with result {}", pretty);
                    }
                    _ => {
                        println!("Task completed with no result");
                    }
                }

                if delete {
                    task.delete(&pool, &tables).await.expect("delete task");
                }

                if failed {
                    std::process::exit(1);
                }
            }
        }

        SubCommand::Fullfill(cmd) => {
            let Fullfill { id, result, error } = cmd;

            let mut task = pg_taskq::Task::load(&pool, &tables, id)
                .await
                .expect("load task")
                .expect("task not found");

            task.fullfill(&pool, &tables, result, error)
                .await
                .expect("fullfill task");
        }
    }
}
