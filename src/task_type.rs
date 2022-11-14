pub trait TaskType: ToString + std::fmt::Debug + Send + Sync {}

#[derive(Clone, Debug)]
pub struct TaskTypeString(pub String);

impl From<String> for TaskTypeString {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl From<&str> for TaskTypeString {
    fn from(name: &str) -> Self {
        Self(name.to_string())
    }
}

impl std::fmt::Display for TaskTypeString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TaskType for TaskTypeString {}
