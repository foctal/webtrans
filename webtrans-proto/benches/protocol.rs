use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use url::Url;
use webtrans_proto::{Capsule, ConnectRequest, ConnectResponse, Settings};

fn bench_connect_request(c: &mut Criterion) {
    let url = Url::parse("https://example.com:4433/wt/chat?room=alpha&v=1").unwrap();
    let request = ConnectRequest { url };

    c.bench_function("connect/request_encode_decode", |b| {
        b.iter(|| {
            let mut encoded = Vec::new();
            black_box(&request).encode(&mut encoded);

            let mut src = encoded.as_slice();
            let decoded = ConnectRequest::decode(&mut src).unwrap();
            black_box(decoded);
        });
    });
}

fn bench_connect_response(c: &mut Criterion) {
    let response = ConnectResponse {
        status: http::StatusCode::OK,
    };

    c.bench_function("connect/response_encode_decode", |b| {
        b.iter(|| {
            let mut encoded = Vec::new();
            black_box(&response).encode(&mut encoded);

            let mut src = encoded.as_slice();
            let decoded = ConnectResponse::decode(&mut src).unwrap();
            black_box(decoded);
        });
    });
}

fn bench_settings(c: &mut Criterion) {
    let mut settings = Settings::default();
    settings.enable_webtransport(32);

    c.bench_function("settings/encode_decode", |b| {
        b.iter(|| {
            let mut encoded = Vec::new();
            black_box(&settings).encode(&mut encoded);

            let mut src = encoded.as_slice();
            let decoded = Settings::decode(&mut src).unwrap();
            black_box(decoded);
        });
    });
}

fn bench_capsule(c: &mut Criterion) {
    let capsule = Capsule::CloseWebTransportSession {
        code: 420,
        reason: "benchmark reason".to_string(),
    };

    c.bench_function("capsule/encode_decode", |b| {
        b.iter(|| {
            let mut encoded = Vec::new();
            black_box(&capsule).encode(&mut encoded).unwrap();

            let mut src = encoded.as_slice();
            let decoded = Capsule::decode(&mut src).unwrap();
            black_box(decoded);
        });
    });
}

criterion_group!(
    benches,
    bench_connect_request,
    bench_connect_response,
    bench_settings,
    bench_capsule
);
criterion_main!(benches);
