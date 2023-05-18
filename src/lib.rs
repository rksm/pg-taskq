//! pg-taskq is a simple postgres-based distributed task queue. It is:
//!
//! - pluggable: install it under a custom schema with custom table names, easily uninstall it again
//! - simple: in postgres there is 1 table, 1 view, 2 plpgsql functions. In Rust there is a task and worker interface
//! - async: using tokio
//! - two-way: task can easily wait on being processed, producing output
//! - hierarchical: tasks can have sub-tasks that get automatically processed bottom-up
//!
//! I made this to scratch my own itch to have a flexible, persistent and
//! distributed task queue for various long-running processing tasks without
//! having to maintain additional services and infrastructure. This thing is
//! likely not production ready nor is it battle tested — use at your own risk.
//!
//! For a worker-producer example see [this project](./examples/producer-worker-example/).

#[macro_use]
extern crate tracing;

mod error;
mod tables;
mod task;
mod task_type;
mod worker;

pub use error::{Error, Result};
pub use tables::{TaskTableBuilder, TaskTableProvider, TaskTables};
pub use task::{Task, TaskBuilder};
pub use task_type::TaskType;
pub use worker::Worker;
