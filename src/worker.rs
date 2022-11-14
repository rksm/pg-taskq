use futures::Future;
use sqlx::{Pool, Postgres};
use std::{
    pin::Pin,
    sync::atomic::AtomicUsize,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::broadcast::Receiver;
use uuid::Uuid;

use crate::{setup::TaskTableProvider, task::Task, task_type::TaskType};
use crate::{Error, Result};

static COUNTER: AtomicUsize = AtomicUsize::new(1);

enum LoopAction {
    Restart,
    DoNothing,
    Break,
    Error(Error),
}

pub type TaskFunctionResult = Pin<Box<dyn Future<Output = std::result::Result<(), Error>> + Send>>;

pub struct Worker {
    pool: Pool<Postgres>,
    stop: Receiver<()>,
    name: String,
    tables: Box<dyn TaskTableProvider>,
}

impl Worker {
    pub async fn start<F>(
        pool: Pool<Postgres>,
        tables: Box<dyn TaskTableProvider>,
        stop: Receiver<()>,
        supported_tasks: Vec<impl TaskType>,
        process: F,
    ) -> Result<()>
    where
        F: FnMut(Task) -> TaskFunctionResult,
    {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = format!("Worker.{n}");
        let mut worker = Self {
            pool,
            tables,
            name,
            stop,
        };
        worker.run(supported_tasks, process).await
    }

    pub async fn run<F>(
        &mut self,
        supported_tasks: Vec<impl TaskType>,
        mut process: F,
    ) -> Result<()>
    where
        F: FnMut(Task) -> TaskFunctionResult,
    {
        let name = self.name.clone();

        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool).await?;
        listener.listen(self.tables.tasks_queue_name()).await?;

        let mut last_status = UNIX_EPOCH;

        tracing::info!("[{name}] starting");

        loop {
            // Get tasks that are ready that we haven't received a notification for
            if last_status.elapsed().unwrap_or_default() > Duration::from_secs(60) {
                last_status = SystemTime::now();
                tracing::info!("[{name}] looking for tasks of type {supported_tasks:?}");
            }

            tokio::select! {
                task = Task::load_any_waiting(&self.pool, &*self.tables, &supported_tasks) =>
                        match self.deal_with_task_result(task, &mut process).await {
                            LoopAction::Restart => continue,
                            LoopAction::DoNothing => {}
                            LoopAction::Break => break,
                            LoopAction::Error(err) => return Err(err),
                        },
                _ = self.stop.recv() => {
                    tracing::debug!("[{name}] Received STOP signal");
                    break;
                },
            };

            // wait for tasks becoming ready
            tracing::trace!("[{name}] waiting for notifications...");

            // let sleep_time =
            //     (self.duration_until_rate_limit_refresh().await?).min(Duration::from_secs(30));
            let sleep_time = Duration::from_secs(1);
            let notification = tokio::select! {
                notification = listener.recv() => notification,
                _ = self.stop.recv() => {
                    tracing::debug!("[{name}] Received STOP signal");
                    break;
                },
                _ = tokio::time::sleep(sleep_time) => {
                    continue;
                },
            };

            let notification = match notification {
                Err(sqlx::Error::PoolClosed) => {
                    tracing::warn!("[{name}] pool closed");
                    break;
                }
                Err(err) => {
                    tracing::error!("[{name}] Error receiving notification {err}");
                    return Err(err.into());
                }
                Ok(notification) => notification,
            };

            let id = match Uuid::parse_str(notification.payload()) {
                Err(err) => {
                    tracing::error!("[{name}] tasks_queue notification {notification:?} but were no able to parse task id: {err}");
                    return Ok(());
                }
                Ok(id) => id,
            };

            let task = Task::load_waiting(id, &self.pool, &*self.tables, &supported_tasks).await;
            match self.deal_with_task_result(task, &mut process).await {
                LoopAction::Restart => continue,
                LoopAction::DoNothing => {}
                LoopAction::Break => break,
                LoopAction::Error(err) => return Err(err),
            }
        }

        tracing::info!("[{name}] stopping Worker");
        // self.env.close().now_or_never();

        Ok(())
    }

    async fn deal_with_task_result<F>(
        &mut self,
        task: Result<Option<Task>>,
        process: &mut F,
    ) -> LoopAction
    where
        F: FnMut(Task) -> TaskFunctionResult,
    {
        let name = &self.name;
        match task {
            Ok(Some(task)) => {
                let id = task.id;
                tracing::trace!("[{name}] task with id {id:?} can be processed");
                if let Err(err) = process(task).await {
                    tracing::error!("[{name}] Error processing task {id}: {err}");
                }
                LoopAction::Restart
            }
            Ok(None) => LoopAction::DoNothing,
            Err(Error::Db(sqlx::error::Error::PoolClosed)) => {
                tracing::warn!("[{name}] pool closed");
                LoopAction::Break
            }
            Err(err) => {
                tracing::error!("[{name}] unexpected error dealing with task: {err}");
                LoopAction::Error(err)
            }
        }
    }

    // async fn request_types_worker_can_handle(&mut self) -> Result<Vec<&'static str>> {
    //     self.env
    //         .update_rate_limit_if_older_than(Duration::from_secs(90))
    //         .await?;
    //     let rate_limit = self.env.rate_limit();

    //     let mut types = Vec::new();
    //     if UserLookupRequest::api_methods()
    //         .iter()
    //         .all(|m| rate_limit.can_call(m))
    //     {
    //         types.push(UserLookupRequest::type_name());
    //     }
    //     if FollowerIdsRequest::api_methods()
    //         .iter()
    //         .all(|m| rate_limit.can_call(m))
    //     {
    //         types.push(FollowerIdsRequest::type_name());
    //     }
    //     if FriendIdsRequest::api_methods()
    //         .iter()
    //         .all(|m| rate_limit.can_call(m))
    //     {
    //         types.push(FriendIdsRequest::type_name());
    //     }
    //     if FullUserUpdateRequest::api_methods()
    //         .iter()
    //         .all(|m| rate_limit.can_call(m))
    //     {
    //         types.push(FullUserUpdateRequest::type_name());
    //     }
    //     if UserLookupInitRequest::api_methods()
    //         .iter()
    //         .all(|m| rate_limit.can_call(m))
    //     {
    //         types.push(UserLookupInitRequest::type_name());
    //     }
    //     if UserLookupBatchRequest::api_methods()
    //         .iter()
    //         .all(|m| rate_limit.can_call(m))
    //     {
    //         types.push(UserLookupBatchRequest::type_name());
    //     }
    //     Ok(types)
    // }
}
