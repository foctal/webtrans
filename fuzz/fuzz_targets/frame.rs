#![no_main]

use libfuzzer_sys::fuzz_target;
use webtrans_proto::Frame;

mod common;

fuzz_target!(|data: &[u8]| {
    let data = common::corpus_bytes(data);
    let mut input = data.as_slice();
    let _ = Frame::read(&mut input);
});
