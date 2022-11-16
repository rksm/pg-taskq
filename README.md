__Please note: This is currently work in progress. The basics should work fine though I'm still cleaning up the implementation and extending the interface a littel. I'll soon publish a version to crates.io.__

----------

# pg-taskq

pg-taskq is a simple postgres-based distributed task queue. It is:

- pluggable: install it under a custom schema with custom table names, easily uninstall it again
- simple: in postgres there is 1 table, 1 view, 2 plpgsql functions. In Rust there is a task and worker interface
- async: using tokio
- two-way: task can easily wait on being processed
- hierarchical: tasks can have sub-tasks that get automatically processed bottom-up

I made this to scratch my own itch to have a flexible, persistent and
distributed task queue for various long-running processing tasks without
having to maintain additional services and infrastructure. This thing is
likely not production ready nor is it battle tested — use at your own risk.
