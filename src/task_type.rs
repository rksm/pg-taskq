/// A [`TaskType`] is used to group kinds of tasks. This is useful if you have
/// specialized workers, where one worker only processes on or a few types of
/// tasks. You might also think of it as the "topic".
/// In the simplest case just use a `String` or `&str`. But you can also use a
/// custom enum that implements this trait.
pub trait TaskType: ToString + std::fmt::Debug + Send + Sync {}

impl TaskType for String {}

impl TaskType for &str {}
