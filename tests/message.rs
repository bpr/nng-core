use nng_pure::Message;

#[test]
fn new_is_empty() {
    let m = Message::new();
    assert!(m.is_empty());
    assert_eq!(m.header(), b"");
    assert_eq!(m.body(), b"");
}

#[test]
fn body_push_back() {
    let mut m = Message::new();
    m.push_back(b"hello");
    m.push_back(b" world");
    assert_eq!(m.body(), b"hello world");
}

#[test]
fn body_push_front() {
    let mut m = Message::new();
    m.push_back(b"world");
    m.push_front(b"hello ");
    assert_eq!(m.body(), b"hello world");
}

#[test]
fn body_trim_front() {
    let mut m = Message::new();
    m.push_back(b"hello world");
    m.trim_front(6);
    assert_eq!(m.body(), b"world");
}

#[test]
fn body_trim_back() {
    let mut m = Message::new();
    m.push_back(b"hello world");
    m.trim_back(6);
    assert_eq!(m.body(), b"hello");
}

#[test]
fn header_push_back() {
    let mut m = Message::new();
    m.header_push_back(&[1, 2, 3, 4]);
    assert_eq!(m.header(), &[1, 2, 3, 4]);
}

#[test]
fn header_push_front() {
    let mut m = Message::new();
    m.header_push_back(&[3, 4]);
    m.header_push_front(&[1, 2]);
    assert_eq!(m.header(), &[1, 2, 3, 4]);
}

#[test]
fn header_trim_front() {
    let mut m = Message::new();
    m.header_push_back(&[1, 2, 3, 4]);
    m.header_trim_front(2);
    assert_eq!(m.header(), &[3, 4]);
}

#[test]
fn header_trim_back() {
    let mut m = Message::new();
    m.header_push_back(&[1, 2, 3, 4]);
    m.header_trim_back(2);
    assert_eq!(m.header(), &[1, 2]);
}

#[test]
fn header_and_body_independent() {
    let mut m = Message::new();
    m.header_push_back(&[0xDE, 0xAD]);
    m.push_back(b"payload");
    assert_eq!(m.header(), &[0xDE, 0xAD]);
    assert_eq!(m.body(), b"payload");
    assert_eq!(m.len(), 9);
}

#[test]
fn clone_is_deep() {
    let mut orig = Message::new();
    orig.header_push_back(&[1]);
    orig.push_back(b"data");

    let mut copy = orig.clone();
    copy.push_back(b" extra");
    // original unchanged
    assert_eq!(orig.body(), b"data");
    assert_eq!(copy.body(), b"data extra");
}

#[test]
fn into_and_from_parts() {
    let mut m = Message::new();
    m.header_push_back(&[0xAB]);
    m.push_back(b"body");
    let (h, b) = m.into_parts();
    let m2 = Message::from_parts(h, b);
    assert_eq!(m2.header(), &[0xAB]);
    assert_eq!(m2.body(), b"body");
}

#[test]
fn fmt_write_appends_to_body() {
    use core::fmt::Write;
    let mut m = Message::new();
    write!(m, "hello {}", 42).unwrap();
    assert_eq!(m.body(), b"hello 42");
}

#[test]
fn trim_front_beyond_len_is_safe() {
    let mut m = Message::new();
    m.push_back(b"hi");
    m.trim_back(100); // saturating
    assert_eq!(m.body(), b"");
}
