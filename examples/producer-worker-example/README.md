This is a cli utility that shows how to submit tasks and setup task workers for pg-taskq.

It supports three subcommands:

```
Usage: producer-worker-example --database-url <DATABASE_URL> <COMMAND>

Commands:
  worker      
  producer    
  supervisor  
  help        Print this message or the help of the given subcommand(s)

Options:
      --database-url <DATABASE_URL>  The database connection to use for the example [env: EXAMPLE_DATABASE_URL=]
  -h, --help                         Print help
```

Set the env var `EXAMPLE_DATABASE_URL` to something like `postgresql://postgres@localhost:5432/pgtaskq_example_producer_worker`.

Then for a simple example:

```shell
$ cargo run -- producer
$ cargo run -- worker --type add
$ cargo run -- worker --type mul
```

This will spawn one producer (that submits tasks of type `add` and `mul`) and two workers (one for each type).

To spawn multiple producers and workers:

```shell
cargo run -- supervisor
```
