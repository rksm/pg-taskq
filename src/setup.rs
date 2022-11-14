use sqlx::{Pool, Postgres, Result};

pub struct TaskSetup;

impl TaskSetup {
    pub async fn build(pool: &Pool<Postgres>) -> Result<Self> {
        let stmts = [
            "
CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    parent UUID REFERENCES tasks(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    task_type TEXT NOT NULL,
    request JSONB DEFAULT NULL,
    result JSONB DEFAULT NULL,
    error JSONB DEFAULT NULL,
    in_progress BOOLEAN DEFAULT FALSE,
    done BOOLEAN DEFAULT FALSE
);",
            "
CREATE FUNCTION tasks_notify(task_id uuid)
RETURNS VOID AS $$
BEGIN
PERFORM pg_notify('tasks_queue', task_id::text);
END;
$$ LANGUAGE plpgsql;
",
            "
CREATE FUNCTION tasks_notify_done(task_id uuid)
RETURNS VOID AS $$
BEGIN
PERFORM pg_notify('tasks_queue_done', task_id::text);
END;
$$ LANGUAGE plpgsql;
",
            "
CREATE VIEW tasks_ready AS (
  -- Select tasks with children. The in_progress field will be false if the task
  -- itself has in_progress set to false and all children tasks are done.
  WITH tasks_with_children as (
    SELECT parent.id,
           parent.parent,
           parent.created_at,
           parent.updated_at,
           parent.task_type,
           parent.request,
           parent.result,
           parent.error,
           (parent.in_progress OR bool_or(not dep_tasks.done)) AS in_progress,
           parent.done
    FROM tasks dep_tasks
    JOIN tasks parent ON parent.id = dep_tasks.parent
    GROUP BY parent.id),
  -- any tasks that is not a tasks_with_children
  leaf_tasks AS (
    SELECT *
    FROM tasks t
    WHERE t.id NOT IN (SELECT id FROM tasks_with_children))
  SELECT tasks.*
  FROM (SELECT * FROM leaf_tasks UNION SELECT * FROM tasks_with_children) AS tasks
  WHERE in_progress = false AND done = false AND error IS NULL);
",
        ];

        for stmt in stmts {
            sqlx::query(stmt).execute(pool).await?;
        }

        Ok(Self)
    }

    pub async fn cleanup(self, pool: &Pool<Postgres>) -> Result<()> {
        let stmts = [
            "DROP VIEW tasks_ready",
            "DROP FUNCTION tasks_notify(uuid)",
            "DROP FUNCTION tasks_notify_done(uuid)",
            "DROP TABLE tasks",
        ];

        for stmt in stmts {
            sqlx::query(stmt).execute(pool).await?;
        }

        tracing::debug!("cleanup for task setup done");

        Ok(())
    }
}
