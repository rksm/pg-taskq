mod producer;
mod shared;
mod worker;

use clap::Parser;

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    cmd: SubCommand,

    #[clap(
        long,
        env = "EXAMPLE_DATABASE_URL",
        help = "The database connection to use for the example"
    )]
    database_url: String,
}

#[derive(Parser)]
enum SubCommand {
    #[clap(name = "worker")]
    Worker(worker::Args),
    #[clap(name = "producer")]
    Producer(producer::Args),
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder().parse_lossy("debug,pg_taskq=trace,sqlx=warn"),
        )
        .init();

    let args = Args::parse();
    let db_url = args.database_url;
    match args.cmd {
        SubCommand::Worker(args) => worker::run(&db_url, args)
            .await
            .expect("failed to run worker"),
        SubCommand::Producer(args) => producer::run(&db_url, args)
            .await
            .expect("failed to run producer"),
    }
}
