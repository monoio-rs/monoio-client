use std::{
    future::Future,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    time::Duration,
};

use monoio::{buf::IoBuf, io::AsyncWriteRentExt};

use crate::client::{
    Client, ClientBuilder, ClientError, Connector, Metrics, Multiplex,
    ReuseError, ReuseResult,
};
use std::net::ToSocketAddrs;

use monoio::net::TcpStream;

use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum TcpClientError {
    #[error("IO error {0}")]
    IOError(#[from] std::io::Error),
    #[error("ClientError error {0}")]
    ClientError(#[from] ClientError<std::io::Error>),
}

#[derive(Clone, Debug)]
struct TcpClientInner {
    config: TcpClientBuilder,
}

pub struct TcpClient {
    inner: Client<TcpClientInner>,
}

impl TcpClient {
    /// Sends a buffer to  
    pub async fn send_buf<K: ToSocketAddrs, T: IoBuf>(
        &self,
        key: K,
        buf: T,
    ) -> Result<(), TcpClientError> {
        let addr = key.to_socket_addrs()?.next().unwrap();
        let mut conn = self.inner.get(addr).await?;
        let (result, _) = conn.write_all(buf).await;
        if let Err(e) = result {
            conn.set_io_error(&e);
            Err(e.into())

        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug)]
pub struct TcpClientBuilder {
    tcp_fast_open: bool,
    no_delay: bool,
    tcp_keepalive: Option<(Option<Duration>, Option<Duration>, Option<u32>)>,
    max_conns: usize,
}

impl TcpClientBuilder {
    /// Builder with default options for connection
    pub fn new() -> Self {
        Self {
            tcp_fast_open: false,
            no_delay: false,
            tcp_keepalive: None,
            max_conns: 1024,
        }
    }

    /// Set TCP fast open option
    pub fn tcp_fast_open(mut self, fast_open: bool) -> Self {
        self.tcp_fast_open = fast_open;
        self
    }

    /// Set TCP no_delay option
    pub fn no_delay(mut self, no_delay: bool) -> Self {
        self.no_delay = no_delay;
        self
    }

    /// Set TCP keepalive options
    pub fn tcp_keepalive(
        mut self,
        time: Option<Duration>,
        interval: Option<Duration>,
        retries: Option<u32>,
    ) -> Self {
        self.tcp_keepalive = Some((time, interval, retries));
        self
    }

    /// Set maximum number of TCP connections.
    /// Defaults to 1024
    pub fn max_conns(mut self, conns: usize) -> Self {
        self.max_conns = conns;
        self
    }

    pub fn build(self) -> TcpClient {
        let max_conns = self.max_conns;
        let inner = TcpClientInner { config: self };
        let inner = ClientBuilder::new(inner).max_size(max_conns).build();

        TcpClient { inner }
    }
}

impl Default for TcpClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}


#[derive(Debug)]
pub struct TcpConn {
    inner: TcpStream,
    io_error: Option<std::io::Error>,
}

impl TcpConn {
    fn set_io_error(&mut self, e: &std::io::Error) {
        let cloned_error = std::io::Error::new(e.kind(), "IO error");
        self.io_error = Some(cloned_error)
    }
}

impl Multiplex for TcpConn {}

impl Deref for TcpConn {
    type Target = TcpStream;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for TcpConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Connector for TcpClientInner {
    type Connection = TcpConn;
    type Key = SocketAddr;
    type Error = std::io::Error;

    type ConnectionFuture<'a> = impl Future<Output = Result<Self::Connection, Self::Error>> + 'a;
    type ReuseFuture<'a> = impl Future<Output = ReuseResult<Self::Error>> + 'a;

    fn connect(&self, key: Self::Key) -> Self::ConnectionFuture<'_> {
        async move {
            let stream = TcpStream::connect(key).await?;
            if let Some(c) = self.config.tcp_keepalive {
                stream.set_tcp_keepalive(c.0, c.1, c.2)?;
            }
            if self.config.no_delay {
                stream.set_nodelay(true)?;
            }
            Ok(TcpConn {
                inner: stream,
                io_error: None,
            })
        }
    }

    fn reuse<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
        _metrics: &Metrics,
    ) -> Self::ReuseFuture<'_> {
        async move {
            match conn.io_error.take() {
                Some(e) => Err(ReuseError::Backend(e)),
                None => Ok(()),
            }
        }
    }
}
