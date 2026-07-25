#![no_main]

use futures::executor::block_on;
use libfuzzer_sys::fuzz_target;
use webtrans_proto::Capsule;

mod common;

fuzz_target!(|data: &[u8]| {
    let data = common::corpus_bytes(data);

    let mut input = data.as_slice();
    let _ = Capsule::decode(&mut input);

    let mut input = data.as_slice();
    let _ = block_on(Capsule::read(&mut input));
});
