use std::{fmt, time::Duration};

use super::{hooks::Hooks, Client, ClientConfig, Connector, Hook, Timeouts};

/// Builder for [`Client`]s.
///
/// Instances of this are created by calling the [`Client::builder()`] method.
#[must_use = "builder does nothing itself, use `.build()` to build it"]
pub struct ClientBuilder<M>
where
    M: Connector,
{
    pub(crate) connector: M,
    pub(crate) config: ClientConfig,
    pub(crate) hooks: Hooks<M>,
}

// Implemented manually to avoid unnecessary trait bound on `W` type parameter.
impl<M> fmt::Debug for ClientBuilder<M>
where
    M: fmt::Debug + Connector,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("Connector", &self.connector)
            .field("config", &self.config)
            .field("hooks", &self.hooks)
            .finish()
    }
}

impl<M> ClientBuilder<M>
where
    M: Connector,
{
    pub(crate) fn new(connector: M) -> Self {
        Self {
            connector,
            config: ClientConfig::default(),
            hooks: Hooks::default(),
        }
    }

    /// Builds the [`Client`].
    ///
    /// # Errors
    ///
    /// See [`BuildError`] for details.
    pub fn build(self) -> Client<M> {
        Client::from_builder(self)
    }

    /// Sets a [`ClientConfig`] to build the [`Client`] with.
    pub fn config(mut self, value: ClientConfig) -> Self {
        self.config = value;
        self
    }

    /// Sets the [`ClientConfig::max_size`].
    pub fn max_size(mut self, value: usize) -> Self {
        self.config.max_size = value;
        self
    }

    /// Sets the [`ClientConfig::timeouts`].
    pub fn timeouts(mut self, value: Timeouts) -> Self {
        self.config.timeouts = value;
        self
    }

    /// Sets the [`Timeouts::wait`] value of the [`ClientConfig::timeouts`].
    pub fn wait_timeout(mut self, value: Option<Duration>) -> Self {
        self.config.timeouts.wait = value;
        self
    }

    /// Sets the [`Timeouts::connect`] value of the [`ClientConfig::timeouts`].
    pub fn connect_timeout(mut self, value: Option<Duration>) -> Self {
        self.config.timeouts.connect = value;
        self
    }

    /// Sets the [`Timeouts::reuse`] value of the [`ClientConfig::timeouts`].
    pub fn reuse_timeout(mut self, value: Option<Duration>) -> Self {
        self.config.timeouts.reuse = value;
        self
    }

    /// Attaches a `post_create` hook.
    ///
    /// The given `hook` will be called each time right after a new [`PooledConnection`]
    /// has been created.
    pub fn post_create(mut self, hook: impl Into<Hook<M>>) -> Self {
        self.hooks.post_connect.push(hook.into());
        self
    }

    /// Attaches a `pre_reuse` hook.
    ///
    /// The given `hook` will be called each time right before an [`PooledConnection`] will
    /// be reused.
    pub fn pre_reuse(mut self, hook: impl Into<Hook<M>>) -> Self {
        self.hooks.pre_reuse.push(hook.into());
        self
    }

    /// Attaches a `post_reuse` hook.
    ///
    /// The given `hook` will be called each time right after an [`PooledConnection`] has
    /// been reused.
    pub fn post_reuse(mut self, hook: impl Into<Hook<M>>) -> Self {
        self.hooks.post_reuse.push(hook.into());
        self
    }
}
