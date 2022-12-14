use thiserror::Error;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("sql error {0}")]
    Db(#[from] sqlx::Error),

    #[error("json error {0}")]
    JSONError(#[from] serde_json::Error),

    #[error("task {task} error: {message}")]
    TaskError { task: Uuid, message: String },
}
