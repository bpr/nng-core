#![no_main]

use libfuzzer_sys::fuzz_target;
use nng_core::codec::decode_frame;

// Feed arbitrary byte slices to decode_frame.  The function must never
// panic — oversized length fields, truncated payloads, and empty input
// must all produce Ok or Err without any memory unsafety.
fuzz_target!(|data: &[u8]| {
    let _ = decode_frame(data);
});
