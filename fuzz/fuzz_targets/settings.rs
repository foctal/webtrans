#![no_main]

use futures::executor::block_on;
use libfuzzer_sys::fuzz_target;
use webtrans_proto::Settings;

mod common;

fuzz_target!(|data: &[u8]| {
    let data = common::corpus_bytes(data);

    let mut input = data.as_slice();
    let _ = Settings::decode(&mut input);

    let mut input = data.as_slice();
    let _ = block_on(Settings::read(&mut input));
});
