use sqlx::{Pool, Postgres, Result, Transaction};

/// Used to make the postgres table/view/function names pluggable. You'll
/// typically want to use its implementor [`TaskTables`] that can be
/// instantiated with a customizable schema and name prefix.
pub trait TaskTableProvider: Send + Sync + 'static {
    fn schema_name(&self) -> &str;
    fn tasks_table(&self) -> &str;
    fn tasks_ready_view(&self) -> &str;
    fn tasks_notify_fn(&self) -> &str;
    fn tasks_notify_done_fn(&self) -> &str;

    fn tasks_queue_name(&self) -> &str;

    fn tasks_queue_done_name(&self) -> String {
        format!("{}_done", self.tasks_queue_name())
    }

    fn tasks_table_full_name(&self) -> String {
        format!("{}.{}", self.schema_name(), self.tasks_table())
    }
    fn tasks_ready_view_full_name(&self) -> String {
        format!("{}.{}", self.schema_name(), self.tasks_ready_view())
    }
    fn tasks_notify_fn_full_name(&self) -> String {
        format!("{}.{}", self.schema_name(), self.tasks_notify_fn())
    }
    fn tasks_notify_done_fn_full_name(&self) -> String {
        format!("{}.{}", self.schema_name(), self.tasks_notify_done_fn())
    }
}

impl TaskTableProvider for TaskTables {
    fn schema_name(&self) -> &str {
        &self.schema
    }

    fn tasks_table(&self) -> &str {
        &self.tasks_table.name
    }

    fn tasks_ready_view(&self) -> &str {
        &self.tasks_ready.name
    }

    fn tasks_notify_fn(&self) -> &str {
        &self.tasks_notify.name
    }

    fn tasks_notify_done_fn(&self) -> &str {
        &self.tasks_notify_done.name
    }

    fn tasks_queue_name(&self) -> &str {
        &self.tasks_queue_name
    }
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

/// Helper used by [`TaskTables`] but create/delete/test existance of postgres
/// entities.
#[derive(Debug, Clone)]
enum EntityType {
    Function,
    View,
    Table,
}

#[derive(Debug, Clone)]
pub struct TaskTableEntity {
    schema: String,
    name: String,
    definition: String,
    entity_type: EntityType,
}

impl TaskTableEntity {
    async fn exists(&self, tx: &mut Transaction<'_, Postgres>) -> Result<bool> {
        let Self {
            schema,
            name,
            entity_type,
            ..
        } = self;

        let query = match entity_type {
            EntityType::Function => {
                "
SELECT count(*)
FROM information_schema.routines
WHERE routine_schema = $1 AND routine_name = $2
"
            }
            EntityType::View | EntityType::Table => {
                "
SELECT count(*)
FROM information_schema.tables
WHERE table_schema = $1 AND table_name = $2
"
            }
        };

        let n = sqlx::query_scalar::<_, i64>(query)
            .bind(schema)
            .bind(name)
            .fetch_one(tx)
            .await?;

        let exists = n > 0;

        debug!("{schema}.{name} exists={exists}");

        Ok(exists)
    }

    async fn create(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        if self.exists(tx).await? {
            return Ok(());
        }
        sqlx::query(&self.definition).execute(tx).await?;
        debug!("{}.{} created", self.schema, self.name);
        Ok(())
    }

    async fn drop(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        if !self.exists(tx).await? {
            return Ok(());
        }

        let Self {
            schema,
            name,
            entity_type,
            ..
        } = self;

        let query = match entity_type {
            EntityType::Function => {
                format!("DROP FUNCTION {schema}.{name} CASCADE")
            }
            EntityType::View => {
                format!("DROP VIEW {schema}.{name} CASCADE")
            }
            EntityType::Table => format!("DROP TABLE {schema}.{name} CASCADE"),
        };

        sqlx::query(&query).execute(tx).await?;

        debug!("{}.{} dropped", self.schema, self.name);

        Ok(())
    }
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

/// Use this to make [`TaskTables`].
#[derive(Default)]
pub struct TaskTableBuilder {
    schema_name: Option<String>,
    base_name: Option<String>,
}

impl TaskTableBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// The postgres schema name to install the task queue table into.
    #[must_use]
    pub fn schema_name(mut self, schema_name: impl ToString) -> Self {
        self.schema_name = Some(schema_name.to_string());
        self
    }

    /// Prefix name used for the postgres entities. Useful if you want to install
    /// multiple independent task queues into the same schema.
    #[must_use]
    pub fn base_name(mut self, base_name: impl ToString) -> Self {
        self.base_name = Some(base_name.to_string());
        self
    }

    pub fn build(self) -> TaskTables {
        let schema = self.schema_name.unwrap_or_else(|| "public".to_string());
        let base_name = self.base_name.unwrap_or_else(|| "tasks".to_string());

        let fullname = |name: Option<&str>| -> String {
            if let Some(name) = name {
                format!("{}_{}", base_name, name)
            } else {
                base_name.to_string()
            }
        };

        let tasks_table_name = fullname(None);
        let tasks_notify_name = fullname(Some("notify"));
        let tasks_notify_done_name = fullname(Some("notify_done"));
        let tasks_ready_name = fullname(Some("ready"));
        let tasks_queue_name: String = fullname(Some("queue"));

        let tasks_table_def = format!(
            "
CREATE TABLE {schema}.{tasks_table_name} (
    id UUID PRIMARY KEY,
    parent UUID REFERENCES {schema}.{tasks_table_name}(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    task_type TEXT NOT NULL,
    request JSONB DEFAULT NULL,
    result JSONB DEFAULT NULL,
    error JSONB DEFAULT NULL,
    in_progress BOOLEAN DEFAULT FALSE,
    done BOOLEAN DEFAULT FALSE
);
"
        );

        let tasks_notify_fn_def = format!(
            "
CREATE FUNCTION {schema}.{tasks_notify_name}(task_id uuid)
RETURNS VOID AS $$
BEGIN
PERFORM pg_notify('{tasks_queue_name}', task_id::text);
END;
$$ LANGUAGE plpgsql;
"
        );

        // TODO get rid of this? unused currently...
        let tasks_notify_done_fn_def = format!(
            "
CREATE FUNCTION {schema}.{tasks_notify_done_name}(task_id uuid)
RETURNS VOID AS $$
BEGIN
PERFORM pg_notify('{tasks_queue_name}_done', task_id::text);
END;
$$ LANGUAGE plpgsql;
"
        );

        let tasks_ready_view_def = format!(
            "
CREATE VIEW {schema}.{tasks_ready_name} AS (
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
    FROM {schema}.{tasks_table_name} dep_tasks
    JOIN {schema}.{tasks_table_name} parent ON parent.id = dep_tasks.parent
    GROUP BY parent.id
  ),
  -- any tasks that is not a tasks_with_children
  leaf_tasks AS (
    SELECT *
    FROM {schema}.{tasks_table_name} t
    WHERE t.id NOT IN (SELECT id FROM tasks_with_children)
  )
  SELECT tasks.*
  FROM (SELECT * FROM leaf_tasks UNION SELECT * FROM tasks_with_children) AS tasks
  WHERE in_progress = false AND done = false AND error IS NULL);
"
        );

        TaskTables {
            tasks_table: TaskTableEntity {
                schema: schema.clone(),
                name: tasks_table_name,
                definition: tasks_table_def,
                entity_type: EntityType::Table,
            },
            tasks_notify: TaskTableEntity {
                schema: schema.clone(),
                name: tasks_notify_name,
                definition: tasks_notify_fn_def,
                entity_type: EntityType::Function,
            },
            tasks_notify_done: TaskTableEntity {
                schema: schema.clone(),
                name: tasks_notify_done_name,
                definition: tasks_notify_done_fn_def,
                entity_type: EntityType::Function,
            },
            tasks_ready: TaskTableEntity {
                schema: schema.clone(),
                name: tasks_ready_name,
                definition: tasks_ready_view_def,
                entity_type: EntityType::View,
            },
            schema,
            tasks_queue_name,
        }
    }
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

/// [`TaskTables`] is used for creating the necessary tables/views/functions in
/// a postgres database. It implements [`TaskTableProvider`] and can be passed
/// to [`crate::Task`] and [`crate::Worker`] that use it for finding out what
/// postgres entities to use.
///
/// Use [`TaskTableBuilder`] to create it or use `TaskTables::default()` if you
/// are OK with default set of postgres entities.
///
/// That default will create:
/// - table `public.tasks`
/// - view `public.tasks_ready`
/// - function `public.notify`
/// - function `public.notify_done`
#[derive(Debug, Clone)]
pub struct TaskTables {
    schema: String,
    tasks_queue_name: String,
    tasks_table: TaskTableEntity,
    tasks_notify: TaskTableEntity,
    tasks_notify_done: TaskTableEntity,
    tasks_ready: TaskTableEntity,
}

impl Default for TaskTables {
    fn default() -> Self {
        TaskTableBuilder::new().build()
    }
}

impl TaskTables {
    /// Check if the necessary postgres entities have been installed already.
    pub async fn exists(&self, pool: &Pool<Postgres>) -> Result<bool> {
        let mut tx = pool.begin().await?;

        if !self.tasks_table.exists(&mut tx).await? {
            return Ok(false);
        };
        if !self.tasks_notify.exists(&mut tx).await? {
            return Ok(false);
        };
        if !self.tasks_notify_done.exists(&mut tx).await? {
            return Ok(false);
        };
        if !self.tasks_ready.exists(&mut tx).await? {
            return Ok(false);
        };

        Ok(true)
    }

    /// Create the necessary postgres entities for the task queue in the
    /// connected postgres database. This function is idempotent and it will not
    /// error if the entities already exist.
    pub async fn create(&self, pool: &Pool<Postgres>) -> Result<()> {
        let mut tx = pool.begin().await?;
        self.tasks_table.create(&mut tx).await?;
        self.tasks_notify.create(&mut tx).await?;
        self.tasks_notify_done.create(&mut tx).await?;
        self.tasks_ready.create(&mut tx).await?;
        tx.commit().await?;

        Ok(())
    }

    /// Delete all postgres entities used by the task queue. This function is
    /// idempotent and will not error if the entities don't exist.
    pub async fn drop(self, pool: &Pool<Postgres>) -> Result<()> {
        let mut tx = pool.begin().await?;
        self.tasks_ready.drop(&mut tx).await?;
        self.tasks_notify.drop(&mut tx).await?;
        self.tasks_notify_done.drop(&mut tx).await?;
        self.tasks_table.drop(&mut tx).await?;
        tx.commit().await?;

        debug!("cleanup for task setup done");

        Ok(())
    }
}
