set dotenv-load

export RUST_BACKTRACE := "1"
export RUST_LOG := "trace"

default:
    just --list

install:
    cargo install --path . --bin pgtaskq

test:
    cargo test -- --nocapture

example *args='':
    cargo run --example {{args}}

example-producer:
    bash -c 'cd examples/producer-worker-example && cargo run -- producer'

example-worker *args='':
    bash -c 'cd examples/producer-worker-example && cargo run -- worker {{args}}'

# spawns producers and workers
example-supervisor *args='':
    bash -c 'cd examples/producer-worker-example && cargo run -- supervisor {{args}}'

example-tasks:
    psql ${EXAMPLE_DATABASE_URL} -c "SELECT * FROM foo_tasks;"

clean:
    psql ${EXAMPLE_DATABASE_URL} -c "DELETE FROM foo_tasks;"

# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

outdated:
    cargo outdated -d 1
    bash -c 'cd examples/producer-worker-example && cargo outdated -d 1'
