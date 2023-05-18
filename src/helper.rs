use chrono::prelude::*;
use sqlx::Pool;
use sqlx::Postgres;

use crate::error::Result;
use crate::TaskTableProvider;

/// Sets the `in_progress` flag to `false` for any tasks that have been in progress for longer than
/// `max_allowed_last_update`.
pub async fn fixup_stale_tasks(
    pool: &Pool<Postgres>,
    tables: &impl TaskTableProvider,
    max_allowed_last_update: std::time::Duration,
) -> Result<()> {
    let max_age = Utc::now()
        - chrono::Duration::from_std(max_allowed_last_update)
            .expect("max_allowed_last_update is too large");
    let table = tables.tasks_table_full_name();
    let sql = format!(
        "
UPDATE {table}
SET updated_at = NOW(),
    in_progress = false
WHERE updated_at < $1::timestamp AND in_progress = true
RETURNING *;
"
    );

    let result: Vec<crate::Task> = sqlx::query_as(&sql).bind(max_age).fetch_all(pool).await?;

    if !result.is_empty() {
        tracing::info!("fixup_stale_tasks found {} stale tasks", result.len());
        for task in result {
            tracing::info!("  id={} created_at={}", task.id, task.created_at);
        }
    }

    Ok(())
}
