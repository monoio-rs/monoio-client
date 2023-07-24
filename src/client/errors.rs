use std::fmt;

use super::hooks::HookError;

/// Possible errors returned by the [`Manager::reuse()`] method.
///
/// [`Manager::reuse()`]: super::Manager::reuse
#[derive(Debug)]
pub enum ReuseError<E> {
    /// Recycling failed for some other reason.
    Message(String),

    /// Recycling failed for some other reason.
    StaticMessage(&'static str),

    /// Error caused by the backend.
    Backend(E),
}

impl<E> From<E> for ReuseError<E> {
    fn from(e: E) -> Self {
        Self::Backend(e)
    }
}

impl<E: fmt::Display> fmt::Display for ReuseError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(msg) => {
                write!(f, "Error occurred while recycling an connection: {}", msg)
            }
            Self::StaticMessage(msg) => {
                write!(f, "Error occurred while recycling an connection: {}", msg)
            }
            Self::Backend(e) => write!(f, "Error occurred while recycling an connection: {}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for ReuseError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Message(_) => None,
            Self::StaticMessage(_) => None,
            Self::Backend(e) => Some(e),
        }
    }
}

/// Possible steps causing the timeout in an error returned by [`Client::get()`]
/// method.
///
/// [`Client::get()`]: super::Client::get
#[derive(Clone, Copy, Debug)]
pub enum TimeoutType {
    /// Timeout happened while waiting for a slot to become available.
    Wait,

    /// Timeout happened while creating a new connection.
    Create,

    /// Timeout happened while recycling an connection.
    Reuse,
}

/// Possible errors returned by [`Client::get()`] method.
///
/// [`Client::get()`]: super::Client::get
#[derive(Debug)]
pub enum ClientError<E> {
    /// Timeout happened.
    Timeout(TimeoutType),

    /// Backend reported an error.
    Backend(E),

    /// [`Client`] has been closed.
    ///
    /// [`Client`]: super::Client
    Closed,

    /// A `post_create` hook reported an error.
    PostCreateHook(HookError<E>),
}

impl<E> From<E> for ClientError<E> {
    fn from(e: E) -> Self {
        Self::Backend(e)
    }
}

impl<E: fmt::Display> fmt::Display for ClientError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout(tt) => match tt {
                TimeoutType::Wait => write!(
                    f,
                    "Timeout occurred while waiting for a slot to become available"
                ),
                TimeoutType::Create => {
                    write!(f, "Timeout occurred while creating a new connection")
                }
                TimeoutType::Reuse => write!(f, "Timeout occurred while recycling an connection"),
            },
            Self::Backend(e) => write!(f, "Error occurred while creating a new connection: {}", e),
            Self::Closed => write!(f, "Client has been closed"),
            Self::PostCreateHook(e) => writeln!(f, "`post_create` hook failed: {}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for ClientError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Timeout(_) | Self::Closed => None,
            Self::Backend(e) => Some(e),
            Self::PostCreateHook(e) => Some(e),
        }
    }
}
