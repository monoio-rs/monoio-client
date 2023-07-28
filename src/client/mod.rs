mod builder;
mod config;
mod errors;
mod hooks;
mod metrics;

use std::{
    cell::UnsafeCell,
    collections::{HashMap, VecDeque},
    fmt::{self, Debug},
    future::Future,
    hash::Hash,
    ops::{Deref, DerefMut},
    rc::Rc,
    rc::Weak,
    time::{Duration, Instant},
};

pub use crate::Status;
use local_sync::semaphore::{Semaphore, TryAcquireError};

pub use self::{
    builder::ClientBuilder,
    config::{ClientConfig, Timeouts},
    errors::{ClientError, ReuseError, TimeoutType},
    hooks::{Hook, HookError, HookFuture, HookResult},
    metrics::Metrics,
};
// / Result Connection of the [`Connector::reuse()`] method.
pub type ReuseResult<E> = Result<(), ReuseError<E>>;

/// Should be implemented for Connector::Connection if it supports multiplexing,
/// like HTTP/2. The client will retain one connection in the Pool
/// at all times and use the fork_connection method to create handles
/// to the underlying connection. Use the purge funtion on [`PooledConnection`]
/// when you want to remove this resource from the pool
pub trait Multiplex {
    /// Should return True
    fn is_multiplexed(&self) -> bool {
        false
    }
    /// Creates a new connection to the underlying
    /// multiplexed connection
    fn fork_connection(&self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}

pub trait Connector {
    /// The type used as a unique key to identify different connections.
    type Key: Hash + Eq + PartialEq + Clone + Debug;

    /// The type representing a connection to the resource (e.g., http connection, database connection).
    type Connection: Multiplex + Debug;

    /// The type representing an error that can occur during connection handling.
    type Error;

    type ConnectionFuture<'a>: Future<Output = Result<Self::Connection, Self::Error>>
    where
        Self: 'a;

    type ReuseFuture<'a>: Future<Output = ReuseResult<Self::Error>>
    where
        Self: 'a;

    /// Establishes a new connection based on the provided key.
    fn connect(&self, key: Self::Key) -> Self::ConnectionFuture<'_>;

    /// Determines whether a connection can be reused or if it needs to be replaced.
    fn reuse<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
        metrics: &Metrics,
    ) -> Self::ReuseFuture<'_>;

    /// Detaches a connection, allowing the connector to handle disconnection procedures.
    /// This method can be used for cleanup or resource management.
    fn detach(&self, _conn: &mut Self::Connection) {}
}

/// Wrapper around the actual pooled connection which implements [`Deref`],
/// [`DerefMut`] and [`Drop`] traits.
///
/// Use this connection just as if it was of Connection `T` and upon leaving a scope the
/// [`Drop::drop()`] will take care of returning it to the pool.
#[must_use]
pub struct PooledConnection<M: Connector> {
    /// The actual connection
    inner: Option<PooledConnectionInner<M>>,

    /// Client to return the pooled connection to.
    pool: Weak<ClientInner<M>>,
}

impl<M> fmt::Debug for PooledConnection<M>
where
    M: fmt::Debug + Connector,
    M::Connection: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledConnection")
            .field("inner", &self.inner)
            .finish()
    }
}

struct CandidateConnection<'a, M: Connector> {
    inner: Option<PooledConnectionInner<M>>,
    pool: &'a ClientInner<M>,
}

impl<'a, M: Connector> CandidateConnection<'a, M> {
    fn ready(mut self) -> PooledConnectionInner<M> {
        self.inner.take().unwrap()
    }
    fn inner(&mut self) -> &mut PooledConnectionInner<M> {
        return self.inner.as_mut().unwrap();
    }
}

impl<'a, M: Connector> Drop for CandidateConnection<'a, M> {
    fn drop(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            unsafe { (*self.pool.slots.get()).size -= 1 };
            self.pool.connector.detach(&mut inner.conn);
        }
    }
}

#[derive(Debug)]
pub(crate) struct PooledConnectionInner<M: Connector> {
    /// Actual pooled connection.
    conn: M::Connection,

    key: M::Key,

    /// PooledConnection metrics.
    metrics: Metrics,
}

pub enum PurgeError {
    NonMultiplexedConn,
    LookupError,
    PoolUpgradeError,
    InnerConnectionMissing,
}

impl<M: Connector> PooledConnection<M> {
    /// Takes this [`PooledConnection`] from its [`Client`] permanently. This reduces the
    /// size of the [`Client`].
    #[must_use]
    pub fn take(mut this: Self) -> M::Connection {
        let mut inner = this.inner.take().unwrap().conn;
        if let Some(pool) = PooledConnection::pool(&this) {
            pool.inner.detach_connection(&mut inner)
        }
        inner
    }

    /// Purge the multiplexed connection from the pool.
    /// No more forked_connections can be created from the underlying
    /// Multiplexed connection
    pub fn purge(&mut self) -> Result<(), PurgeError> {
        if !self.is_multiplexed() {
            return Err(PurgeError::NonMultiplexedConn);
        }

        let inner = match self.inner.take() {
            Some(i) => i,
            None => {
                return Err(PurgeError::InnerConnectionMissing);
            }
        };

        let key = inner.key;

        match PooledConnection::pool(self) {
            Some(pool) => {
                let slots = unsafe { pool.slots() };
                match slots.get_entry(&key) {
                    Some(mut purge_conn) => {
                        pool.inner.detach_connection(&mut purge_conn.conn);
                        Ok(())
                    }
                    None => Err(PurgeError::LookupError),
                }
            }
            None => Err(PurgeError::PoolUpgradeError),
        }
    }

    /// Get connection statistics
    pub fn metrics(this: &Self) -> &Metrics {
        &this.inner.as_ref().unwrap().metrics
    }

    /// Returns the [`Client`] this [`PooledConnection`] belongs to.
    ///
    /// Since [`PooledConnection`]s only hold a [`Weak`] reference to the [`Client`] they
    /// come from, this can fail and return [`None`] instead.
    pub fn pool(this: &Self) -> Option<Client<M>> {
        this.pool.upgrade().map(|inner| Client { inner })
    }
}

impl<M: Connector> Drop for PooledConnection<M> {
    fn drop(&mut self) {
        // Drop the handle, multiplexed connection already is
        // in the pool.
        if self.is_multiplexed() {
            return;
        }

        if let Some(inner) = self.inner.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.return_connection(inner)
            }
        }
    }
}

impl<M: Connector> Deref for PooledConnection<M> {
    type Target = M::Connection;
    fn deref(&self) -> &M::Connection {
        &self.inner.as_ref().unwrap().conn
    }
}

impl<M: Connector> DerefMut for PooledConnection<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner.as_mut().unwrap().conn
    }
}

impl<M: Connector> AsRef<M::Connection> for PooledConnection<M> {
    fn as_ref(&self) -> &M::Connection {
        self
    }
}

impl<M: Connector> AsMut<M::Connection> for PooledConnection<M> {
    fn as_mut(&mut self) -> &mut M::Connection {
        self
    }
}

/// Generic connection and connection pool.
///
/// This struct can be cloned and transferred across thread boundaries and uses
/// reference counting for its internal state.
pub struct Client<M: Connector> {
    inner: Rc<ClientInner<M>>,
}

// Implemented manually to avoid unnecessary trait bound on `W` Connection parameter.
impl<M> Debug for Client<M>
where
    M: Debug + Connector,
    M::Connection: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<M: Connector> Clone for Client<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<M: Connector> Client<M> {
    #[allow(clippy::mut_from_ref)]
    unsafe fn slots(&self) -> &mut Slots<M> {
        &mut *self.inner.slots.get()
    }

    /// Instantiates a builder for a new [`Client`].
    ///
    /// This is the only way to create a [`Client`] instance.
    pub fn builder(connector: M) -> ClientBuilder<M> {
        ClientBuilder::new(connector)
    }

    pub(crate) fn from_builder(builder: ClientBuilder<M>) -> Self {
        Self {
            inner: Rc::new(ClientInner {
                connector: builder.connector,
                slots: UnsafeCell::new(Slots {
                    map: HashMap::with_capacity(builder.config.max_size),
                    size: 0,
                    max_size: builder.config.max_size,
                }),
                semaphore: Semaphore::new(builder.config.max_size),
                config: builder.config,
                hooks: builder.hooks,
            }),
        }
    }

    /// Retrieves an [`PooledConnection`] from this [`Client`] or waits for one to
    /// become available.
    ///
    /// # Errors
    ///
    /// See [`ClientError`] for details.
    pub async fn get(&self, key: M::Key) -> Result<PooledConnection<M>, ClientError<M::Error>> {
        self.timeout_get(key, &self.timeouts()).await
    }

    /// Retrieves an [`PooledConnection`] from this [`Client`] using a different `timeout`
    /// than the configured one.
    ///
    /// # Errors
    ///
    /// See [`ClientError`] for details.
    pub async fn timeout_get(
        &self,
        key: M::Key,
        timeouts: &Timeouts,
    ) -> Result<PooledConnection<M>, ClientError<M::Error>> {
        let non_blocking = match timeouts.wait {
            Some(t) => t.as_nanos() == 0,
            None => false,
        };

        let permit = if non_blocking {
            self.inner.semaphore.try_acquire().map_err(|e| match e {
                TryAcquireError::Closed => ClientError::Closed,
                TryAcquireError::NoPermits => ClientError::Timeout(TimeoutType::Wait),
            })?
        } else {
            apply_timeout(TimeoutType::Wait, timeouts.wait, async {
                self.inner
                    .semaphore
                    .acquire()
                    .await
                    .map_err(|_| ClientError::Closed)
            })
            .await?
        };

        let inner_conn = loop {
            let key_owned = key.clone();
            let slots = unsafe { self.slots() };
            let inner_conn = slots.get_entry(&key_owned);

            let inner_conn = if let Some(mut inner_conn) = inner_conn {
                self.handle_multiplex_connections(&mut inner_conn);
                self.try_reuse(timeouts, inner_conn).await?
            } else {
                self.try_connect(key_owned, timeouts).await?
            };
            if let Some(inner_conn) = inner_conn {
                break inner_conn;
            }
        };

        permit.forget();

        Ok(PooledConnection {
            inner: Some(inner_conn),
            pool: Rc::downgrade(&self.inner),
        })
    }

    fn handle_multiplex_connections(&self, inner: &mut PooledConnectionInner<M>) {
        if inner.conn.is_multiplexed() {
            if let Some(handle) = inner.conn.fork_connection() {
                let slots = unsafe { self.slots() };
                slots.add_entry(PooledConnectionInner {
                    key: inner.key.clone(),
                    conn: handle,
                    metrics: inner.metrics,
                })
            }
        }
    }

    #[inline]
    async fn try_reuse(
        &self,
        timeouts: &Timeouts,
        inner_conn: PooledConnectionInner<M>,
    ) -> Result<Option<PooledConnectionInner<M>>, ClientError<M::Error>> {
        let mut candidate_conn = CandidateConnection {
            inner: Some(inner_conn),
            pool: &self.inner,
        };
        let inner = candidate_conn.inner();

        // Apply pre_reuse hooks
        if let Err(_e) = self.inner.hooks.pre_reuse.apply(inner).await {
            // TODO log pre_reuse error
            return Ok(None);
        }

        if apply_timeout(
            TimeoutType::Reuse,
            timeouts.reuse,
            self.inner.connector.reuse(&mut inner.conn, &inner.metrics),
        )
        .await
        .is_err()
        {
            return Ok(None);
        }

        // Apply post_reuse hooks
        if let Err(_e) = self.inner.hooks.post_reuse.apply(inner).await {
            // TODO log post_reuse error
            return Ok(None);
        }

        inner.metrics.reuse_count += 1;
        inner.metrics.reused = Some(Instant::now());

        Ok(Some(candidate_conn.ready()))
    }

    #[inline]
    async fn try_connect(
        &self,
        key: M::Key,
        timeouts: &Timeouts,
    ) -> Result<Option<PooledConnectionInner<M>>, ClientError<M::Error>> {
        let mut candidate_conn = CandidateConnection {
            inner: Some(PooledConnectionInner {
                key: key.clone(),
                conn: apply_timeout(
                    TimeoutType::Connect,
                    timeouts.connect,
                    self.inner.connector.connect(key),
                )
                .await?,
                metrics: Metrics::default(),
            }),
            pool: &self.inner,
        };

        unsafe {
            self.slots().size += 1;
        };

        // Apply post_connect hooks
        if let Err(e) = self
            .inner
            .hooks
            .post_connect
            .apply(candidate_conn.inner())
            .await
        {
            return Err(ClientError::PostCreateHook(e));
        }

        Ok(Some(candidate_conn.ready()))
    }

    /// Get current timeout configuration
    pub fn timeouts(&self) -> Timeouts {
        self.inner.config.timeouts
    }

    /// Closes this [`Client`].
    ///
    /// All current and future tasks waiting for [`PooledConnection`]s will return
    /// [`ClientError::Closed`] immediately.
    ///
    /// This operation resizes the pool to 0.
    pub fn close(&self) {
        // self.resize(0);
        self.inner.semaphore.close();
    }

    /// Indicates whether this [`Client`] has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.semaphore.is_closed()
    }

    /// Retrieves [`Status`] of this [`Client`].
    #[must_use]
    pub fn status(&self) -> Status {
        let slots = unsafe { self.slots() };

        Status {
            max_size: slots.max_size,
            size: slots.size,
        }
    }

    /// Returns [`Connector`] of this [`Client`].
    #[must_use]
    pub fn connector(&self) -> &M {
        &self.inner.connector
    }
}

struct ClientInner<M: Connector> {
    connector: M,
    slots: UnsafeCell<Slots<M>>,
    /// Number of available [`PooledConnection`]s in the [`Client`]. If there are no
    /// [`PooledConnection`]s in the [`Client`] this number can become negative and store
    /// the number of [`Future`]s waiting for an [`PooledConnection`].
    semaphore: Semaphore,
    config: ClientConfig,
    hooks: hooks::Hooks<M>,
}

#[derive(Debug)]
struct Slots<M: Connector> {
    map: HashMap<M::Key, VecDeque<PooledConnectionInner<M>>>,
    size: usize,
    max_size: usize,
}

impl<M: Connector> Slots<M> {
    fn add_entry(&mut self, conn: PooledConnectionInner<M>) {
        let key = conn.key.clone();
        let entry = self.map.entry(key).or_default();
        (*entry).push_back(conn);
    }

    fn get_entry(&mut self, key: &M::Key) -> Option<PooledConnectionInner<M>> {
        match self.map.get_mut(key) {
            Some(v) => v.pop_front(),
            None => None,
        }
    }
}

// Implemented manually to avoid unnecessary trait bound on the struct.
impl<M> fmt::Debug for ClientInner<M>
where
    M: fmt::Debug + Connector,
    M::Connection: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientInner")
            .field("Connector", &self.connector)
            .field("slots", &self.slots)
            .field("semaphore", &self.semaphore)
            .field("config", &self.config)
            .field("hooks", &self.hooks)
            .finish()
    }
}

impl<M: Connector> ClientInner<M> {
    fn return_connection(&self, mut inner: PooledConnectionInner<M>) {
        let slots = unsafe { &mut *self.slots.get() };
        if slots.size <= slots.max_size {
            slots.add_entry(inner);
            self.semaphore.add_permits(1);
        } else {
            slots.size -= 1;
            self.connector.detach(&mut inner.conn);
        }
    }
    fn detach_connection(&self, conn: &mut M::Connection) {
        let slots = unsafe { &mut *self.slots.get() };
        let add_permits = slots.size <= slots.max_size;
        slots.size -= 1;
        if add_permits {
            self.semaphore.add_permits(1);
        }
        self.connector.detach(conn);
    }
}

async fn apply_timeout<O, E>(
    timeout_type: TimeoutType,
    duration: Option<Duration>,
    future: impl Future<Output = Result<O, impl Into<ClientError<E>>>>,
) -> Result<O, ClientError<E>> {
    match duration {
        None => future.await.map_err(Into::into),
        Some(duration) => match monoio::time::timeout(duration, future).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err(ClientError::Timeout(timeout_type)),
        },
    }
}
