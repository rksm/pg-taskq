use chrono::prelude::*;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Pool, Postgres};
use std::collections::HashSet;
use std::time::Instant;
use uuid::Uuid;

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
    #[tracing::instrument(level = "trace", skip(db, req))]
    pub async fn create_task<R: Serialize>(
        task_type: impl TaskType,
        req: R,
        db: &Pool<Postgres>,
        parent: Option<Uuid>,
    ) -> Result<Self> {
        let id = uuid::Uuid::new_v4();
        let req = Some(serde_json::to_value(req)?);
        let mut tx = db.begin().await?;
        let task: Self = sqlx::query_as(
            "
INSERT INTO tasks (id, task_type, request, parent, in_progress, done)
VALUES ($1, $2, $3, $4, false, false)
RETURNING *
",
        )
        .bind(id)
        .bind(task_type.to_string())
        .bind(req)
        .bind(parent)
        .fetch_one(&mut tx)
        .await?;

        sqlx::query("SELECT tasks_notify($1)")
            .bind(task.id)
            .execute(&mut tx)
            .await?;

        tx.commit().await?;

        tracing::debug!("created task {task_type:?} {id}");
        Ok(task)
    }

    pub async fn load(db: &Pool<Postgres>, id: Uuid) -> Result<Option<Self>> {
        Ok(sqlx::query_as("SELECT * FROM tasks WHERE id = $1")
            .bind(id)
            .fetch_optional(db)
            .await?)
    }

    #[tracing::instrument(level = "trace", skip(db))]
    pub async fn load_any_waiting(
        db: &Pool<Postgres>,
        // allowed_types: &[&'static str],
        allowed_types: &[impl TaskType],
    ) -> Result<Option<Self>> {
        let allowed_types = allowed_types
            .iter()
            .map(|ea| ea.to_string())
            .collect::<Vec<_>>();
        let task: Option<Self> = sqlx::query_as(
            "
UPDATE tasks
SET updated_at = NOW(),
    in_progress = true
WHERE id = (SELECT id
            FROM tasks
            WHERE task_type = ANY($1) AND id IN (SELECT id FROM tasks_ready)
            LIMIT 1
            FOR UPDATE SKIP LOCKED)
RETURNING *;
",
        )
        .bind(allowed_types)
        .fetch_optional(db)
        .await?;

        Ok(task)
    }

    #[tracing::instrument(level = "trace", skip(db))]
    pub async fn load_waiting(
        id: Uuid,
        db: &Pool<Postgres>,
        // allowed_types: &[&'static str],
        allowed_types: &[impl TaskType],
    ) -> Result<Option<Self>> {
        let allowed_types = allowed_types
            .iter()
            .map(|ea| ea.to_string())
            .collect::<Vec<_>>();
        let task: Option<Self> = sqlx::query_as(
            "
UPDATE tasks
SET updated_at = NOW(),
    in_progress = true
WHERE id = (SELECT id
            FROM tasks
            WHERE id = $1 AND task_type = ANY($2) AND id IN (SELECT id FROM tasks_ready)
            LIMIT 1
            FOR UPDATE SKIP LOCKED)
RETURNING *;
",
        )
        .bind(sqlx::types::Uuid::from_u128(id.as_u128()))
        .bind(allowed_types)
        .fetch_optional(db)
        .await?;

        Ok(task)
    }

    #[tracing::instrument(level = "trace", skip(pool))]
    pub async fn wait_for_tasks_to_be_done(
        tasks: Vec<&mut Self>,
        pool: &Pool<Postgres>,
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
        listener.listen("tasks_queue_done").await?;

        loop {
            tracing::trace!("waiting for task {} to be done", tasks_pending.len());

            let ready: Vec<(Uuid,)> =
                sqlx::query_as("SELECT id FROM tasks WHERE id = ANY($1) AND done = true")
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
            task.update(pool).await?;
        }

        tracing::debug!(
            "{} tasks done, wait time: {}ms",
            tasks_done.len(),
            (Instant::now() - now).as_millis()
        );

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, pool), fields(id=%self.id))]
    pub async fn save(&self, pool: &Pool<Postgres>) -> Result<()> {
        sqlx::query(
            "
UPDATE tasks
SET updated_at = NOW(),
    request = $2,
    result = $3,
    error = $4,
    in_progress = $5,
    done = $6
WHERE id = $1",
        )
        .bind(self.id)
        .bind(&self.request)
        .bind(&self.result)
        .bind(&self.error)
        .bind(self.in_progress)
        .bind(self.done)
        .execute(pool)
        .await?;

        if self.done {
            tracing::debug!("notifying about task done {}", self.id);
            sqlx::query("SELECT tasks_notify_done($1)")
                .bind(self.id)
                .execute(pool)
                .await?;
        } else {
            tracing::debug!("notifying about task ready again {}", self.id);
            sqlx::query("SELECT tasks_notify($1)")
                .bind(self.id)
                .execute(pool)
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, pool), fields(id=%self.id))]
    pub async fn update(&mut self, pool: &Pool<Postgres>) -> Result<()> {
        tracing::info!("UPDATING {}", self.id);
        let me: Self = sqlx::query_as("SELECT * FROM tasks WHERE id = $1")
            .bind(self.id)
            .fetch_one(pool)
            .await?;

        let _ = std::mem::replace(self, me);

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, pool), fields(id=%self.id))]
    pub async fn delete(&self, pool: &Pool<Postgres>) -> Result<()> {
        sqlx::query(
            "
DELETE FROM tasks
WHERE id = $1",
        )
        .bind(self.id)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, pool), fields(id=%self.id))]
    pub async fn wait_until_done(&mut self, pool: &Pool<Postgres>) -> Result<()> {
        Self::wait_for_tasks_to_be_done(vec![self], pool).await
    }

    pub async fn wait_until_done_and_delete(&mut self, pool: &Pool<Postgres>) -> Result<()> {
        self.wait_until_done(pool).await?;
        self.delete(pool).await?;
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
