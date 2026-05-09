#![no_main]

use libfuzzer_sys::fuzz_target;
use nng_core::codec::decode_handshake;

// Feed arbitrary 8-byte windows to decode_handshake.  The function must
// never panic regardless of input — it should only return Ok or Err.
fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }
    let buf: [u8; 8] = data[..8].try_into().unwrap();
    let _ = decode_handshake(&buf);
});
