#![no_main]

use futures::executor::block_on;
use libfuzzer_sys::fuzz_target;
use webtrans_proto::{ConnectRequest, ConnectResponse};

mod common;

fuzz_target!(|data: &[u8]| {
    let data = common::corpus_bytes(data);

    let mut request = data.as_slice();
    let _ = ConnectRequest::decode(&mut request);
    let mut response = data.as_slice();
    let _ = ConnectResponse::decode(&mut response);

    let mut request = data.as_slice();
    let _ = block_on(ConnectRequest::read(&mut request));
    let mut response = data.as_slice();
    let _ = block_on(ConnectResponse::read(&mut response));
});
