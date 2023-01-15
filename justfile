export RUST_BACKTRACE := "1"
export RUST_LOG := "trace"

default:
    just --list

test:
    cargo test -- --nocapture

example *args='':
    cargo run --example {{args}}

