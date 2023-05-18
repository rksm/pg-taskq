use chrono::prelude::*;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Acquire, PgExecutor, Pool, Postgres};
use std::collections::HashSet;
use std::time::Instant;
use uuid::Uuid;

use crate::{Error, Result, TaskTableProvider, TaskType};

#[derive(Default)]
pub struct TaskBuilder {
    task_type: String,
    req: Option<Value>,
    parent: Option<Uuid>,
    id: Option<Uuid>,
}

impl TaskBuilder {
    pub fn new(task_type: impl TaskType) -> Self {
        Self {
            task_type: task_type.to_string(),
            ..Default::default()
        }
    }

    pub fn with_request(mut self, req: impl Serialize) -> Result<Self> {
        self.req = Some(serde_json::to_value(req)?);
        Ok(self)
    }

    #[must_use]
    pub fn with_parent(mut self, parent: Uuid) -> Self {
        self.parent = Some(parent);
        self
    }

    #[must_use]
    pub fn with_id(mut self, id: Uuid) -> Self {
        self.id = Some(id);
        self
    }

    pub async fn build<'a, DB, P>(self, db: DB, tables: &P) -> Result<Task>
    where
        P: TaskTableProvider,
        DB: Acquire<'a, Database = Postgres>,
    {
        let Self {
            task_type,
            req,
            parent,
            id,
        } = self;
        Task::create_task(db, tables, id, task_type, req, parent).await
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Task {
    pub id: Uuid,
    pub parent: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub task_type: String,
    pub request: Option<Value>,
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub in_progress: bool,
    pub done: bool,
}

impl Task {
    #[instrument(level = "trace", skip(db, tables, req))]
    async fn create_task<'a, P, DB>(
        db: DB,
        tables: &P,
        id: Option<Uuid>,
        task_type: String,
        req: Option<Value>,
        parent: Option<Uuid>,
    ) -> Result<Self>
    where
        P: TaskTableProvider,
        DB: Acquire<'a, Database = Postgres>,
    {
        let id = id.unwrap_or_else(Uuid::new_v4);
        let req = Some(serde_json::to_value(req)?);

        let mut tx = db.begin().await?;

        let table = tables.tasks_table_full_name();
        let notify_fn = tables.tasks_notify_fn_full_name();

        let sql = format!(
            "
INSERT INTO {table} (id, task_type, request, parent, in_progress, done)
VALUES ($1, $2, $3, $4, false, false)
RETURNING *
"
        );
        let task: Self = sqlx::query_as(&sql)
            .bind(id)
            .bind(task_type.to_string())
            .bind(req)
            .bind(parent)
            .fetch_one(&mut *tx)
            .await?;

        let sql = format!("SELECT {notify_fn}($1)");
        sqlx::query(&sql).bind(task.id).execute(&mut *tx).await?;

        tx.commit().await?;

        debug!("created task {task_type:?} {id}");

        Ok(task)
    }

    pub async fn load(
        db: impl PgExecutor<'_>,
        tables: &dyn TaskTableProvider,
        id: Uuid,
    ) -> Result<Option<Self>> {
        let table = tables.tasks_table_full_name();
        let sql = format!("SELECT * FROM {table} WHERE id = $1");
        Ok(sqlx::query_as(&sql).bind(id).fetch_optional(db).await?)
    }

    pub async fn with_children(
        self,
        db: impl PgExecutor<'_>,
        tables: &dyn TaskTableProvider,
        recursive: bool,
    ) -> Result<Vec<Self>> {
        Self::load_children(db, tables, self.id, true, recursive).await
    }

    pub async fn children(
        &self,
        db: impl PgExecutor<'_>,
        tables: &dyn TaskTableProvider,
        recursive: bool,
    ) -> Result<Vec<Self>> {
        Self::load_children(db, tables, self.id, false, recursive).await
    }

    #[instrument(level = "trace", skip(db, tables))]
    pub async fn load_children(
        db: impl PgExecutor<'_>,
        tables: &dyn TaskTableProvider,
        id: Uuid,
        include_self: bool,
        recursive: bool,
    ) -> Result<Vec<Self>> {
        let table = tables.tasks_table_full_name();
        let sql = if recursive {
            let where_clause = if include_self { "" } else { "WHERE t.id != $1" };
            format!(
                "
WITH RECURSIVE tasks_and_subtasks(id, parent) AS (
     SELECT t.* FROM {table} t
     WHERE t.id = $1
     UNION ALL
     SELECT child_task.*
     FROM tasks_and_subtasks t, {table} child_task
     WHERE t.id = child_task.parent
)
SELECT * FROM tasks_and_subtasks {where_clause}
"
            )
        } else {
            let self_condition = if include_self { "OR t.id = $1" } else { "" };
            format!("SELECT * FROM {table} WHERE parent = $1 {self_condition}")
        };

        Ok(sqlx::query_as(&sql).bind(id).fetch_all(db).await?)
    }

    #[instrument(level = "trace", skip(db, tables))]
    pub async fn load_any_waiting(
        db: impl PgExecutor<'_>,
        tables: &dyn TaskTableProvider,
        allowed_types: &[impl TaskType],
    ) -> Result<Option<Self>> {
        let allowed_types = allowed_types
            .iter()
            .map(|ea| ea.to_string())
            .collect::<Vec<_>>();
        let table = tables.tasks_table_full_name();
        let table_ready = tables.tasks_ready_view_full_name();
        let sql = format!(
            "
UPDATE {table}
SET updated_at = NOW(),
    in_progress = true
WHERE id = (SELECT id
            FROM {table}
            WHERE task_type = ANY($1) AND id IN (SELECT id FROM {table_ready})
            LIMIT 1
            FOR UPDATE SKIP LOCKED)
RETURNING *;
"
        );
        let task: Option<Self> = sqlx::query_as(&sql)
            .bind(allowed_types)
            .fetch_optional(db)
            .await?;

        Ok(task)
    }

    #[instrument(level = "trace", skip(db, tables))]
    pub async fn load_waiting(
        id: Uuid,
        db: impl PgExecutor<'_>,
        tables: &dyn TaskTableProvider,
        allowed_types: &[impl TaskType],
    ) -> Result<Option<Self>> {
        let allowed_types = allowed_types
            .iter()
            .map(|ea| ea.to_string())
            .collect::<Vec<_>>();
        let table = tables.tasks_table_full_name();
        let table_ready = tables.tasks_ready_view_full_name();
        let sql = format!(
            "
UPDATE {table}
SET updated_at = NOW(),
    in_progress = true
WHERE id = (SELECT id
            FROM {table}
            WHERE id = $1 AND task_type = ANY($2) AND id IN (SELECT id FROM {table_ready})
            LIMIT 1
            FOR UPDATE SKIP LOCKED)
RETURNING *;
"
        );
        let task: Option<Self> = sqlx::query_as(&sql)
            .bind(sqlx::types::Uuid::from_u128(id.as_u128()))
            .bind(allowed_types)
            .fetch_optional(db)
            .await?;

        Ok(task)
    }

    /// Sets the error value for the task
    #[instrument(level = "trace", skip(db, tables))]
    pub async fn set_error(
        id: Uuid,
        db: impl Acquire<'_, Database = Postgres>,
        tables: &dyn TaskTableProvider,
        error: Value,
    ) -> Result<()> {
        let mut tx = db.begin().await?;

        let table = tables.tasks_table_full_name();
        let sql = format!(
            "
UPDATE {table}
SET updated_at = NOW(),
    error = $2,
    in_progress = false
WHERE id = $1"
        );

        sqlx::query(&sql)
            .bind(id)
            .bind(error)
            .execute(&mut *tx)
            .await?;

        // if self.done {
        //     debug!("notifying about task done {}", self.id);
        //     let notify_fn = tables.tasks_notify_done_fn_full_name();
        //     let sql = format!("SELECT {notify_fn}($1)");
        //     sqlx::query(&sql).bind(self.id).execute(&mut *tx).await?;
        // } else {
        //     debug!("notifying about task ready again {}", self.id);
        //     let notify_fn = tables.tasks_notify_done_fn_full_name();
        //     let sql = format!("SELECT {notify_fn}($1)");
        //     sqlx::query(&sql).bind(self.id).execute(&mut *tx).await?;
        // }

        tx.commit().await?;

        Ok(())
    }

    /// Saves current state in DB
    #[instrument(level = "trace", skip(self, db,tables), fields(id=%self.id))]
    pub async fn save(
        &self,
        db: impl Acquire<'_, Database = Postgres>,
        tables: &dyn TaskTableProvider,
    ) -> Result<()> {
        let mut tx = db.begin().await?;

        let table = tables.tasks_table_full_name();
        let sql = format!(
            "
UPDATE {table}
SET updated_at = NOW(),
    request = $2,
    result = $3,
    error = $4,
    in_progress = $5,
    done = $6
WHERE id = $1"
        );

        sqlx::query(&sql)
            .bind(self.id)
            .bind(&self.request)
            .bind(&self.result)
            .bind(&self.error)
            .bind(self.in_progress)
            .bind(self.done)
            .execute(&mut *tx)
            .await?;

        if self.done {
            debug!("notifying about task done {}", self.id);
            let notify_fn = tables.tasks_notify_done_fn_full_name();
            let sql = format!("SELECT {notify_fn}($1)");
            sqlx::query(&sql).bind(self.id).execute(&mut *tx).await?;
        } else {
            debug!("notifying about task ready again {}", self.id);
            let notify_fn = tables.tasks_notify_done_fn_full_name();
            let sql = format!("SELECT {notify_fn}($1)");
            sqlx::query(&sql).bind(self.id).execute(&mut *tx).await?;
        }

        tx.commit().await?;

        Ok(())
    }

    /// Queries state of this task from DB and updates self.
    #[instrument(level = "trace", skip(self, db,tables), fields(id=%self.id))]
    pub async fn update(
        &mut self,
        db: impl PgExecutor<'_>,
        tables: &dyn TaskTableProvider,
    ) -> Result<()> {
        info!("UPDATING {}", self.id);
        let table = tables.tasks_table_full_name();
        let sql = format!("SELECT * FROM {table} WHERE id = $1");
        let me: Self = sqlx::query_as(&sql).bind(self.id).fetch_one(db).await?;

        let _ = std::mem::replace(self, me);

        Ok(())
    }

    /// Deletes this and all child tasks (recursively) from the DB. Call this
    /// when the task is done.
    #[instrument(level = "trace", skip(self, db,tables), fields(id=%self.id))]
    pub async fn delete(
        &self,
        db: impl Acquire<'_, Database = Postgres>,
        tables: &dyn TaskTableProvider,
    ) -> Result<()> {
        let mut tx = db.begin().await?;

        let table = tables.tasks_table_full_name();
        let sql = format!("DELETE FROM {table} WHERE id = $1");
        sqlx::query(&sql).bind(self.id).execute(&mut tx).await?;

        let notify_fn = tables.tasks_notify_done_fn_full_name();
        let sql = format!("SELECT {notify_fn}($1)");
        sqlx::query(&sql).bind(self.id).execute(&mut tx).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Takes `tasks` and listens for notifications until all tasks are done.
    /// Inbetween notifications, at `poll_interval`, will manually query for
    /// updated tasks. Will ensure that those tasks are updated when this method
    /// returns.
    #[instrument(level = "trace", skip(pool, tables))]
    async fn wait_for_tasks_to_be_done(
        tasks: Vec<&mut Self>,
        pool: &Pool<Postgres>,
        tables: &dyn TaskTableProvider,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<()> {
        fn update<'a>(
            tasks_pending: Vec<&'a mut Task>,
            tasks_done: &mut Vec<&'a mut Task>,
            ids: std::collections::HashSet<Uuid>,
        ) -> Vec<&'a mut Task> {
            let (done, rest): (Vec<_>, Vec<_>) = tasks_pending
                .into_iter()
                .partition(|task| ids.contains(&task.id));
            tasks_done.extend(done);
            trace!("still waiting for {} tasks", rest.len());
            rest
        }

        let start_time = Instant::now();
        let mut tasks_pending = tasks.into_iter().collect::<Vec<_>>();
        let mut tasks_done = Vec::new();

        let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
        let queue_name = tables.tasks_queue_done_name();
        listener.listen(&queue_name).await?;

        let tasks_table = tables.tasks_table();
        let ready_sql = format!("SELECT id FROM {tasks_table} WHERE id = ANY($1) AND done = true");
        let existing_sql = format!("SELECT id FROM {tasks_table} WHERE id = ANY($1)");

        loop {
            trace!("waiting for task {} to be done", tasks_pending.len());

            let ready: Vec<(Uuid,)> = sqlx::query_as(&ready_sql)
                .bind(tasks_pending.iter().map(|ea| ea.id).collect::<Vec<_>>())
                .fetch_all(pool)
                .await?;

            let existing: Vec<(Uuid,)> = sqlx::query_as(&existing_sql)
                .bind(tasks_pending.iter().map(|ea| ea.id).collect::<Vec<_>>())
                .fetch_all(pool)
                .await?;
            let existing = existing.into_iter().map(|(id,)| id).collect::<HashSet<_>>();

            tasks_pending = update(
                tasks_pending,
                &mut tasks_done,
                HashSet::from_iter(ready.into_iter().map(|(id,)| id)),
            );

            if tasks_pending.is_empty() {
                break;
            }

            // in case one of the tasks we are waiting for was deleted
            for ea in &tasks_pending {
                if !existing.contains(&ea.id) {
                    return Err(Error::TaskDeleted { task: ea.id });
                }
            }

            let notification = if let Some(poll_interval) = poll_interval {
                tokio::select! {
                    _ = tokio::time::sleep(poll_interval) => {
                        continue;
                    },
                    notification = listener.recv() => notification,
                }
            } else {
                listener.recv().await
            };

            if let Ok(notification) = notification {
                if let Ok(id) = Uuid::parse_str(notification.payload()) {
                    tasks_pending =
                        update(tasks_pending, &mut tasks_done, HashSet::from_iter([id]));
                    if tasks_pending.is_empty() {
                        break;
                    }
                }
            }
        }

        for task in &mut tasks_done {
            task.update(pool, tables).await?;
        }

        debug!(
            "{} tasks done, wait time: {}ms",
            tasks_done.len(),
            (Instant::now() - start_time).as_millis()
        );

        Ok(())
    }

    #[instrument(level = "trace", skip(self, db,tables), fields(id=%self.id))]
    pub async fn wait_until_done(
        &mut self,
        db: &Pool<Postgres>,
        tables: &dyn TaskTableProvider,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<()> {
        Self::wait_for_tasks_to_be_done(vec![self], db, tables, poll_interval).await
    }

    pub async fn wait_until_done_and_delete(
        &mut self,
        pool: &Pool<Postgres>,
        tables: &dyn TaskTableProvider,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<()> {
        self.wait_until_done(pool, tables, poll_interval).await?;
        self.delete(pool, tables).await?;
        Ok(())
    }

    /// Takes request returns it deserialized.
    pub fn request<R: serde::de::DeserializeOwned>(&mut self) -> Result<R> {
        let request = match self.request.take() {
            None => {
                return Err(Error::TaskError {
                    task: self.id,
                    message: "Task has no request JSON data".to_string(),
                })
            }
            Some(request) => request,
        };
        Ok(
            serde_json::from_value(request).map_err(|err| Error::TaskError {
                task: self.id,
                message: format!("Error deserializing request JSON from task: {err}"),
            })?,
        )
    }

    /// Converts self into the result payload (or error).
    fn error(&mut self) -> Option<Error> {
        self.error.take().map(|error| Error::TaskError {
            task: self.id,
            message: match error.get("error").and_then(|msg| msg.as_str()) {
                Some(msg) => msg.to_string(),
                None => error.to_string(),
            },
        })
    }

    /// Takes the result returns it deserialized.
    fn result<R: serde::de::DeserializeOwned>(&mut self) -> Result<Option<R>> {
        match self.result.take() {
            None => Ok(None),
            Some(request) => Ok(Some(serde_json::from_value(request)?)),
        }
    }

    /// Takes the result returns it deserialized.
    #[allow(dead_code)]
    pub fn result_cloned<R: serde::de::DeserializeOwned>(&self) -> Result<Option<R>> {
        if let Some(result) = &self.result {
            Ok(Some(serde_json::from_value(result.clone())?))
        } else {
            Ok(None)
        }
    }

    /// Converts self into the result payload (or error).
    pub fn as_result<R: serde::de::DeserializeOwned>(&mut self) -> Result<Option<R>> {
        self.error().map(Err).unwrap_or_else(|| self.result())
    }
}
