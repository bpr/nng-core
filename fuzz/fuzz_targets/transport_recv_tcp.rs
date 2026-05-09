#![no_main]

use std::sync::OnceLock;

use embedded_io_async::{ErrorType, Read, Write};
use libfuzzer_sys::fuzz_target;
use nng_core::{
    codec::ProtocolId,
    transport::{FrameFormat, FramedTransport},
};

// PUSH0 handshake — the valid peer identity expected by a PULL0 local socket.
const PUSH0_HANDSHAKE: [u8; 8] = [0x00, b'S', b'P', 0x00, 0x00, 0x50, 0x00, 0x00];

// In-memory read-only stream: yields a fixed handshake prefix followed by
// arbitrary fuzz bytes, then signals EOF.  All writes are silently discarded
// (our outgoing handshake and any sends during the test).
struct FuzzStream {
    data: Vec<u8>,
    pos: usize,
}

impl FuzzStream {
    fn new(prefix: &[u8], payload: &[u8]) -> Self {
        let mut data = Vec::with_capacity(prefix.len() + payload.len());
        data.extend_from_slice(prefix);
        data.extend_from_slice(payload);
        Self { data, pos: 0 }
    }
}

impl ErrorType for FuzzStream {
    type Error = std::io::Error;
}

impl Read for FuzzStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let avail = &self.data[self.pos..];
        if avail.is_empty() {
            return Ok(0); // EOF → transport returns Closed
        }
        let n = buf.len().min(avail.len());
        buf[..n].copy_from_slice(&avail[..n]);
        self.pos += n;
        Ok(n)
    }
}

impl Write for FuzzStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        Ok(buf.len()) // discard
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

// Reuse the runtime across iterations to avoid per-call overhead.
static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fuzz_target!(|data: &[u8]| {
    let rt = RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    });
    rt.block_on(async {
        // Prepend the PUSH0 handshake so connect() succeeds; fuzz bytes follow
        // as the message stream.  FramedTransport::recv must never panic on
        // any byte sequence — it should only return Ok(msg) or Err(...).
        let stream = FuzzStream::new(&PUSH0_HANDSHAKE, data);
        let Ok(mut transport) =
            FramedTransport::connect(stream, ProtocolId::PULL0, FrameFormat::Tcp).await
        else {
            return;
        };
        let _ = transport.recv().await;
    });
});
