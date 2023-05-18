set dotenv-load

export RUST_BACKTRACE := "1"
export RUST_LOG := "trace"

default:
    just --list

test:
    cargo test -- --nocapture

example *args='':
    cargo run --example {{args}}


example-producer:
    bash -c 'cd examples/producer-worker-example && cargo run -- producer'

example-worker *args='':
    bash -c 'cd examples/producer-worker-example && cargo run -- worker {{args}}'

example-tasks:
    psql ${EXAMPLE_DATABASE_URL} -c "SELECT * FROM foo_tasks;"

clean:
    psql ${EXAMPLE_DATABASE_URL} -c "DELETE FROM foo_tasks;"

# bash -c 'cd examples/producer-worker-example && cargo run --bin producer'
# bash -c 'cd examples/producer-worker-example && cargo run --bin worker'

# DATABASE_URL=postgresql://postgres:postgres@localhost:5432/psql_tasks_rs_test_1
# EXAMPLE_DATABASE_URL

# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

outdated:
    cargo outdated -d 1
    bash -c 'cd examples/producer-worker-example && cargo outdated -d 1'
