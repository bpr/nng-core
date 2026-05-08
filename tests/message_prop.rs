use nng_core::{Message, MessageBuf, ZeroCopyMessage};
use proptest::prelude::*;

// ZeroCopyMessage<256>: HEADER_CAP = 64, body_capacity = 192.
// All strategies for ZCM tests stay within those bounds.

// ── Message (heap-backed) ─────────────────────────────────────────────────────

proptest! {
    #[test]
    fn message_push_back_appends(
        prefix in prop::collection::vec(any::<u8>(), 0..128),
        data   in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        let mut msg = Message::new();
        msg.push_back(&prefix);
        let before = msg.body().to_vec();
        msg.push_back(&data);
        let mut expected = before;
        expected.extend_from_slice(&data);
        prop_assert_eq!(msg.body(), expected.as_slice());
    }

    #[test]
    fn message_header_push_back_appends(
        prefix in prop::collection::vec(any::<u8>(), 0..128),
        data   in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        let mut msg = Message::new();
        msg.header_push_back(&prefix);
        let before = msg.header().to_vec();
        msg.header_push_back(&data);
        let mut expected = before;
        expected.extend_from_slice(&data);
        prop_assert_eq!(msg.header(), expected.as_slice());
    }

    #[test]
    fn message_trim_front_removes_prefix(
        (data, n) in prop::collection::vec(any::<u8>(), 0..256)
            .prop_flat_map(|v| {
                let len = v.len();
                (Just(v), 0..=len)
            })
    ) {
        let mut msg = Message::new();
        msg.push_back(&data);
        let full = msg.body().to_vec();
        msg.trim_front(n);
        prop_assert_eq!(msg.body(), &full[n..]);
    }

    #[test]
    fn message_sequential_trims_compose(
        data in prop::collection::vec(any::<u8>(), 4..256),
    ) {
        let a = data.len() / 4;
        let b = data.len() / 4;

        let mut msg1 = Message::new();
        msg1.push_back(&data);
        msg1.trim_front(a);
        msg1.trim_front(b);

        let mut msg2 = Message::new();
        msg2.push_back(&data);
        msg2.trim_front(a + b);

        prop_assert_eq!(msg1.body(), msg2.body());
    }

    #[test]
    fn message_push_body_does_not_affect_header(
        hdr  in prop::collection::vec(any::<u8>(), 0..64),
        body in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        let mut msg = Message::new();
        msg.header_push_back(&hdr);
        let hdr_snapshot = msg.header().to_vec();
        msg.push_back(&body);
        prop_assert_eq!(msg.header(), hdr_snapshot.as_slice());
    }

    #[test]
    fn message_push_header_does_not_affect_body(
        body in prop::collection::vec(any::<u8>(), 0..128),
        hdr  in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut msg = Message::new();
        msg.push_back(&body);
        let body_snapshot = msg.body().to_vec();
        msg.header_push_back(&hdr);
        prop_assert_eq!(msg.body(), body_snapshot.as_slice());
    }

    #[test]
    fn message_sequential_pushes_concat(
        parts in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..64),
            1..8,
        )
    ) {
        let mut msg = Message::new();
        let mut expected = Vec::new();
        for part in &parts {
            msg.push_back(part);
            expected.extend_from_slice(part);
        }
        prop_assert_eq!(msg.body(), expected.as_slice());
    }
}

// ── ZeroCopyMessage (stack-allocated) ─────────────────────────────────────────

proptest! {
    #[test]
    fn zcm_push_back_appends(
        prefix in prop::collection::vec(any::<u8>(), 0..96),
        data   in prop::collection::vec(any::<u8>(), 0..96),
    ) {
        let mut msg = ZeroCopyMessage::<256>::new();
        msg.push_back(&prefix);
        let before = msg.body().to_vec();
        msg.push_back(&data);
        let mut expected = before;
        expected.extend_from_slice(&data);
        prop_assert_eq!(msg.body(), expected.as_slice());
    }

    #[test]
    fn zcm_header_push_back_appends(
        prefix in prop::collection::vec(any::<u8>(), 0..32),
        data   in prop::collection::vec(any::<u8>(), 0..32),
    ) {
        let mut msg = ZeroCopyMessage::<256>::new();
        msg.header_push_back(&prefix);
        let before = msg.header().to_vec();
        msg.header_push_back(&data);
        let mut expected = before;
        expected.extend_from_slice(&data);
        prop_assert_eq!(msg.header(), expected.as_slice());
    }

    #[test]
    fn zcm_trim_front_removes_prefix(
        (data, n) in prop::collection::vec(any::<u8>(), 0..192)
            .prop_flat_map(|v| {
                let len = v.len();
                (Just(v), 0..=len)
            })
    ) {
        let mut msg = ZeroCopyMessage::<256>::new();
        msg.push_back(&data);
        let full = msg.body().to_vec();
        msg.trim_front(n);
        prop_assert_eq!(msg.body(), &full[n..]);
    }

    #[test]
    fn zcm_sequential_trims_compose(
        data in prop::collection::vec(any::<u8>(), 4..192),
    ) {
        let a = data.len() / 4;
        let b = data.len() / 4;

        let mut msg1 = ZeroCopyMessage::<256>::new();
        msg1.push_back(&data);
        msg1.trim_front(a);
        msg1.trim_front(b);

        let mut msg2 = ZeroCopyMessage::<256>::new();
        msg2.push_back(&data);
        msg2.trim_front(a + b);

        prop_assert_eq!(msg1.body(), msg2.body());
    }

    #[test]
    fn zcm_push_body_does_not_affect_header(
        hdr  in prop::collection::vec(any::<u8>(), 0..32),
        body in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        let mut msg = ZeroCopyMessage::<256>::new();
        msg.header_push_back(&hdr);
        let hdr_snapshot = msg.header().to_vec();
        msg.push_back(&body);
        prop_assert_eq!(msg.header(), hdr_snapshot.as_slice());
    }

    #[test]
    fn zcm_push_header_does_not_affect_body(
        body in prop::collection::vec(any::<u8>(), 0..128),
        hdr  in prop::collection::vec(any::<u8>(), 0..32),
    ) {
        let mut msg = ZeroCopyMessage::<256>::new();
        msg.push_back(&body);
        let body_snapshot = msg.body().to_vec();
        msg.header_push_back(&hdr);
        prop_assert_eq!(msg.body(), body_snapshot.as_slice());
    }

    /// body_remaining() tracks tailroom: it decreases by exactly push_back's data length
    /// and is unaffected by trim_front (which only advances b_start, not b_end).
    #[test]
    fn zcm_body_remaining_tracks_tailroom(
        (data, n) in prop::collection::vec(any::<u8>(), 0..192)
            .prop_flat_map(|v| {
                let len = v.len();
                (Just(v), 0..=len)
            })
    ) {
        let mut msg = ZeroCopyMessage::<256>::new();
        let initial = msg.body_remaining();

        msg.push_back(&data);
        prop_assert_eq!(msg.body_remaining(), initial - data.len());

        // trim_front advances b_start, not b_end — tailroom is unchanged
        let after_push = msg.body_remaining();
        msg.trim_front(n);
        prop_assert_eq!(msg.body_remaining(), after_push);
    }

    /// header() and body() slices never overlap in the backing [u8; N] array.
    #[test]
    fn zcm_header_body_non_overlapping(
        body in prop::collection::vec(any::<u8>(), 1..128),
        hdr  in prop::collection::vec(any::<u8>(), 1..32),
    ) {
        let mut msg = ZeroCopyMessage::<256>::new();
        msg.push_back(&body);
        msg.header_push_back(&hdr);

        let h = msg.header();
        let b = msg.body();
        let h_start = h.as_ptr() as usize;
        let h_end   = h_start + h.len();
        let b_start = b.as_ptr() as usize;
        let b_end   = b_start + b.len();

        prop_assert!(
            h_end <= b_start || b_end <= h_start,
            "header [{h_start}..{h_end}) overlaps body [{b_start}..{b_end})"
        );
    }
}
