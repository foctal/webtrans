#![no_main]

use libfuzzer_sys::fuzz_target;
use webtrans_proto::{Capsule, Frame};

mod common;

fuzz_target!(|data: &[u8]| {
    let data = common::corpus_bytes(data);
    let mut input = data.as_slice();
    let _ = Frame::read(&mut input);
    let mut input = data.as_slice();
    if let Ok(capsule) = Capsule::decode(&mut input) {
        let mut encoded = Vec::new();
        capsule.encode(&mut encoded).expect("decoded capsule must encode");
        assert_eq!(Capsule::decode(&mut encoded.as_slice()).unwrap(), capsule);
    }
});
