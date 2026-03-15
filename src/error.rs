#[derive(Debug, thiserror::Error)]
pub enum StrunkError {
    #[error("{0}")]
    Database(#[from] sqlx::Error),

    #[error("{0}")]
    Serialisation(#[from] serde_json::Error),

    #[error("{0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, StrunkError>;
