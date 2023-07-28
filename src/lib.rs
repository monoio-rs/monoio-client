#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

pub mod client;

pub mod tcp_client;

/// The current pool status.
#[derive(Clone, Copy, Debug)]
pub struct Status {
    /// The maximum size of the pool.
    pub max_size: usize,

    /// The current size of the pool.
    pub size: usize,
}
