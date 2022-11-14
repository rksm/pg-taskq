use chrono::prelude::*;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Acquire, PgExecutor, Pool, Postgres};
use std::collections::HashSet;
use std::time::Instant;
use uuid::Uuid;

use crate::setup::TaskTableProvider;
use crate::task_type::TaskType;

use super::Result;

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
    #[tracing::instrument(level = "trace", skip(db, tables, req))]
    pub async fn create_task<R: Serialize>(
        db: impl Acquire<'_, Database = Postgres>,
        tables: &dyn TaskTableProvider,
        task_type: impl TaskType,
        req: R,
        parent: Option<Uuid>,
    ) -> Result<Self> {
        let id = uuid::Uuid::new_v4();
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

        tracing::debug!("created task {task_type:?} {id}");
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

    #[tracing::instrument(level = "trace", skip(db, tables))]
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

    #[tracing::instrument(level = "trace", skip(db, tables))]
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

    #[tracing::instrument(level = "trace", skip(pool, tables))]
    pub async fn wait_for_tasks_to_be_done(
        tasks: Vec<&mut Self>,
        pool: &Pool<Postgres>,
        tables: &dyn TaskTableProvider,
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
            tracing::trace!("still waiting for {} tasks", rest.len());
            rest
        }

        let mut tasks_pending = tasks.into_iter().collect::<Vec<_>>();
        let mut tasks_done = Vec::new();
        let now = Instant::now();
        let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
        let queue_name = tables.tasks_queue_done_name();
        listener.listen(&queue_name).await?;

        let tasks_table = tables.tasks_table();
        let sql = format!("SELECT id FROM {tasks_table} WHERE id = ANY($1) AND done = true");

        loop {
            tracing::trace!("waiting for task {} to be done", tasks_pending.len());

            let ready: Vec<(Uuid,)> = sqlx::query_as(&sql)
                .bind(tasks_pending.iter().map(|ea| ea.id).collect::<Vec<_>>())
                .fetch_all(pool)
                .await?;
            tasks_pending = update(
                tasks_pending,
                &mut tasks_done,
                HashSet::from_iter(ready.into_iter().map(|(id,)| id)),
            );

            if tasks_pending.is_empty() {
                break;
            }

            let notification = tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    continue;
                },
                notification = listener.recv() => notification,
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

        tracing::debug!(
            "{} tasks done, wait time: {}ms",
            tasks_done.len(),
            (Instant::now() - now).as_millis()
        );

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, db,tables), fields(id=%self.id))]
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
            tracing::debug!("notifying about task done {}", self.id);
            let notify_fn = tables.tasks_notify_done_fn_full_name();
            let sql = format!("SELECT {notify_fn}($1)");
            sqlx::query(&sql).bind(self.id).execute(&mut *tx).await?;
        } else {
            tracing::debug!("notifying about task ready again {}", self.id);
            let notify_fn = tables.tasks_notify_done_fn_full_name();
            let sql = format!("SELECT {notify_fn}($1)");
            sqlx::query(&sql).bind(self.id).execute(&mut *tx).await?;
        }

        tx.commit().await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, db,tables), fields(id=%self.id))]
    pub async fn update(
        &mut self,
        db: impl PgExecutor<'_>,
        tables: &dyn TaskTableProvider,
    ) -> Result<()> {
        tracing::info!("UPDATING {}", self.id);
        let table = tables.tasks_table_full_name();
        let sql = format!("SELECT * FROM {table} WHERE id = $1");
        let me: Self = sqlx::query_as(&sql).bind(self.id).fetch_one(db).await?;

        let _ = std::mem::replace(self, me);

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, db,tables), fields(id=%self.id))]
    pub async fn delete(
        &self,
        db: impl PgExecutor<'_>,
        tables: &dyn TaskTableProvider,
    ) -> Result<()> {
        let table = tables.tasks_table_full_name();
        let sql = format!("DELETE FROM {table} WHERE id = $1");
        sqlx::query(&sql).bind(self.id).execute(db).await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, db,tables), fields(id=%self.id))]
    pub async fn wait_until_done(
        &mut self,
        db: &Pool<Postgres>,
        tables: &dyn TaskTableProvider,
    ) -> Result<()> {
        Self::wait_for_tasks_to_be_done(vec![self], db, tables).await
    }

    pub async fn wait_until_done_and_delete(
        &mut self,
        pool: &Pool<Postgres>,
        tables: &dyn TaskTableProvider,
    ) -> Result<()> {
        self.wait_until_done(pool, tables).await?;
        self.delete(pool, tables).await?;
        // self.as_result()
        Ok(())
    }

    // pub fn request<R: DeserializeOwned>(&mut self) -> Result<R> {
    //     let request = match self.request.take() {
    //         None => return Err(Error::UpdateError("No request JSON".to_string())),
    //         Some(request) => request,
    //     };
    //     Ok(serde_json::from_value(request)?)
    // }

    // pub fn as_result(&self) -> Result<()> {
    //     if let Some(error) = &self.error {
    //         return Err(Error::TaskError {
    //             task: self.id,
    //             message: match error.get("error").and_then(|msg| msg.as_str()) {
    //                 Some(msg) => msg.to_string(),
    //                 None => error.to_string(),
    //             },
    //         });
    //     }

    //     Ok(())
    // }
}
