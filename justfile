export RUST_BACKTRACE := "1"

default:
    just --list

test:
    RUST_LOG=trace cargo test -- --nocapture
