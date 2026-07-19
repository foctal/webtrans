#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;

fuzz_target!(|data: &[u8]| {
    let data = common::corpus_bytes(data);

    let mut qpack = data.as_slice();
    webtrans_proto::fuzzing::decode_qpack(&mut qpack);

    let mut string = data.as_slice();
    webtrans_proto::fuzzing::decode_qpack_string(&mut string);
});
