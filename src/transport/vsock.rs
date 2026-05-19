//! VSOCK transport adapter — Linux only.
//!
//! Wraps [`tokio_vsock::VsockStream`] to implement `embedded-io-async`'s
//! `Read + Write` traits, enabling use with [`FramedTransport`].
//!
//! VSOCK (`AF_VSOCK`) is a Linux kernel socket family for efficient VM-to-host
//! and VM-to-VM communication without a network stack.  A VSOCK address is a
//! `(CID, port)` pair; the host's CID is always 2 (`host`).
//!
//! # URL scheme
//!
//! `vsock://CID:port` — for example `vsock://2:5555` to connect to the host,
//! or `vsock://any:5555` to listen for connections from any peer.
//!
//! | CID string | Numeric value | Meaning |
//! |---|---|---|
//! | `any` | 4294967295 | Wildcard — accept connections from any peer |
//! | `host` | 2 | The hypervisor host |
//! | `local` | 1 | Loopback within the same virtual machine |
//! | _decimal_ | literal | Any explicit numeric CID |
//!
//! # Kernel support
//!
//! VSOCK requires the `vhost_vsock` kernel module on the host and the
//! `vsock` module in the guest.  Loopback (`vsock://local:port`) additionally
//! requires `vsock_loopback`.  Load with `modprobe vsock_loopback` for
//! in-guest testing without a hypervisor.
//!
//! [`FramedTransport`]: crate::transport::FramedTransport

use std::io;

use embedded_io_async::{ErrorType, Read, Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VsockAddr, VsockListener, VsockStream};

/// Parse a `CID:port` string (vsock:// scheme already stripped) into a
/// [`VsockAddr`].
///
/// Accepts `"any"`, `"host"`, `"local"`, or any decimal `u32` as the CID.
pub(crate) fn parse_vsock_addr(s: &str) -> io::Result<VsockAddr> {
    let (cid_str, port_str) = s.rsplit_once(':').ok_or_else(|| {
        io::Error::other(format!("invalid vsock address (expected CID:port): {s}"))
    })?;
    let cid: u32 = match cid_str {
        "any" => u32::MAX,
        "host" => 2,
        "local" => 1,
        n => n
            .parse()
            .map_err(|_| io::Error::other(format!("invalid vsock CID: {n}")))?,
    };
    let port: u32 = port_str
        .parse()
        .map_err(|_| io::Error::other(format!("invalid vsock port: {port_str}")))?;
    Ok(VsockAddr::new(cid, port))
}

/// Bind a [`VsockListener`] at the address encoded in `addr` (scheme stripped).
pub(crate) fn bind_vsock_listener(addr: &str) -> io::Result<VsockListener> {
    VsockListener::bind(parse_vsock_addr(addr)?)
}

/// Connect a [`VsockStream`] to the address encoded in `addr` (scheme stripped).
pub(crate) async fn connect_vsock_stream(addr: &str) -> io::Result<VsockStream> {
    VsockStream::connect(parse_vsock_addr(addr)?).await
}

/// Wraps a [`tokio_vsock::VsockStream`] as an `embedded-io-async` stream.
pub(crate) struct TokioVsockStream(pub VsockStream);

impl ErrorType for TokioVsockStream {
    type Error = io::Error;
}

impl Read for TokioVsockStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        AsyncReadExt::read(&mut self.0, buf).await
    }
}

impl Write for TokioVsockStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        AsyncWriteExt::write(&mut self.0, buf).await
    }

    async fn flush(&mut self) -> Result<(), io::Error> {
        AsyncWriteExt::flush(&mut self.0).await
    }
}
