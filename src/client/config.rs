use std::time::Duration;

/// [`Client`] configuration.
///
/// [`Client`]: super::Client
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct ClientConfig {
    /// Maximum size of the [`Client`].
    ///
    /// Default: `cpu_count * 4`
    ///
    /// [`Client`]: super::Client
    pub max_size: usize,

    /// Timeouts of the [`Client`].
    ///
    /// Default: No timeouts
    ///
    /// [`Client`]: super::Client
    #[cfg_attr(feature = "serde", serde(default))]
    pub timeouts: Timeouts,
}

impl ClientConfig {
    /// Creates a new [`ClientConfig`] without any timeouts and with the provided
    /// `max_size`.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            timeouts: Timeouts::default(),
        }
    }
}

impl Default for ClientConfig {
    /// Creates a new [`ClientConfig`] with the `max_size` being set to
    /// `cpu_count * 4` ignoring any logical CPUs (Hyper-Threading).
    fn default() -> Self {
        Self::new(num_cpus::get_physical() * 4)
    }
}

/// Timeouts when getting [`connection`]s from a [`Client`].
///
/// [`connection`]: super::connection
/// [`Client`]: super::Client
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct Timeouts {
    /// Timeout when waiting for a slot to become available.
    pub wait: Option<Duration>,

    /// Timeout when creating a new connection.
    pub create: Option<Duration>,

    /// Timeout when recycling an connection.
    pub reuse: Option<Duration>,
}

impl Timeouts {
    /// Create an empty [`Timeouts`] config (no timeouts set).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new [`Timeouts`] config with only the `wait` timeout being
    /// set.
    #[must_use]
    pub fn wait_millis(wait: u64) -> Self {
        Self {
            create: None,
            wait: Some(Duration::from_millis(wait)),
            reuse: None,
        }
    }
}

// Implemented manually to provide a custom documentation.
impl Default for Timeouts {
    /// Creates an empty [`Timeouts`] config (no timeouts set).
    fn default() -> Self {
        Self {
            create: None,
            wait: None,
            reuse: None,
        }
    }
}
