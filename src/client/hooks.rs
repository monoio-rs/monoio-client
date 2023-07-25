//! Hooks allowing to run code when creating and/or recycling connections.

use std::{fmt, future::Future, pin::Pin};

use super::{Connector, Metrics, PooledConnectionInner};

/// The result returned by hooks
pub type HookResult<E> = Result<(), HookError<E>>;

/// The boxed future that should be returned by async hooks
pub type HookFuture<'a, E> = Pin<Box<dyn Future<Output = HookResult<E>> + 'a>>;

/// Function signature for sync callbacks
type SyncFn<M> =
    dyn Fn(&mut <M as Connector>::Connection, &Metrics) -> HookResult<<M as Connector>::Error>;

/// Function siganture for async callbacks
type AsyncFn<M> = dyn for<'a> Fn(
    &'a mut <M as Connector>::Connection,
    &'a Metrics,
) -> HookFuture<'a, <M as Connector>::Error>;

/// Wrapper for hook functions
pub enum Hook<M: Connector> {
    /// Use a plain function (non-async) as a hook
    Fn(Box<SyncFn<M>>),
    /// Use an async function as a hook
    AsyncFn(Box<AsyncFn<M>>),
}

impl<M: Connector> Hook<M> {
    /// Create Hook from sync function
    pub fn sync_fn(
        f: impl Fn(&mut M::Connection, &Metrics) -> HookResult<M::Error> + 'static,
    ) -> Self {
        Self::Fn(Box::new(f))
    }
    /// Create Hook from async function
    pub fn async_fn(
        f: impl for<'a> Fn(&'a mut M::Connection, &'a Metrics) -> HookFuture<'a, M::Error> + 'static,
    ) -> Self {
        Self::AsyncFn(Box::new(f))
    }
}

impl<M: Connector> fmt::Debug for Hook<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fn(_) => f
                .debug_tuple("Fn")
                //.field(arg0)
                .finish(),
            Self::AsyncFn(_) => f
                .debug_tuple("AsyncFn")
                //.field(arg0)
                .finish(),
        }
    }
}

/// Error which is returned by `pre_create`, `pre_reuse` and
/// `post_reuse` hooks.
#[derive(Debug)]
pub enum HookError<E> {
    /// Hook failed for some other reason.
    Message(String),

    /// Hook failed for some other reason.
    StaticMessage(&'static str),

    /// Error caused by the backend.
    Backend(E),
}

impl<E: fmt::Display> fmt::Display for HookError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(msg) => write!(f, "{}", msg),
            Self::StaticMessage(msg) => write!(f, "{}", msg),
            Self::Backend(e) => write!(f, "{}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for HookError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Message(_) => None,
            Self::StaticMessage(_) => None,
            Self::Backend(e) => Some(e),
        }
    }
}

pub(crate) struct HookVec<M: Connector> {
    vec: Vec<Hook<M>>,
}

// Implemented manually to avoid unnecessary trait bound on `M` type parameter.
impl<M: Connector> fmt::Debug for HookVec<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HookVec")
            //.field("fns", &self.fns)
            .finish_non_exhaustive()
    }
}

// Implemented manually to avoid unnecessary trait bound on `M` type parameter.
impl<M: Connector> Default for HookVec<M> {
    fn default() -> Self {
        Self { vec: Vec::new() }
    }
}

impl<M: Connector> HookVec<M> {
    pub(crate) async fn apply(
        &self,
        inner: &mut PooledConnectionInner<M>,
    ) -> Result<(), HookError<M::Error>> {
        for hook in &self.vec {
            match hook {
                Hook::Fn(f) => f(&mut inner.conn, &inner.metrics)?,
                Hook::AsyncFn(f) => f(&mut inner.conn, &inner.metrics).await?,
            };
        }
        Ok(())
    }
    pub(crate) fn push(&mut self, hook: Hook<M>) {
        self.vec.push(hook);
    }
}

/// Collection of all the hooks that can be configured for a [`Pool`].
///
/// [`Pool`]: super::Pool
pub(crate) struct Hooks<M: Connector> {
    pub(crate) post_connect: HookVec<M>,
    pub(crate) pre_reuse: HookVec<M>,
    pub(crate) post_reuse: HookVec<M>,
}

// Implemented manually to avoid unnecessary trait bound on `M` type parameter.
impl<M: Connector> fmt::Debug for Hooks<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hooks")
            .field("post_create", &self.post_connect)
            .field("pre_reuse", &self.post_reuse)
            .field("post_reuse", &self.post_reuse)
            .finish()
    }
}

// Implemented manually to avoid unnecessary trait bound on `M` type parameter.
impl<M: Connector> Default for Hooks<M> {
    fn default() -> Self {
        Self {
            pre_reuse: HookVec::default(),
            post_connect: HookVec::default(),
            post_reuse: HookVec::default(),
        }
    }
}
