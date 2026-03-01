use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use webtrans_proto::VarInt;

fn bench_varint_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("varint/encode");
    let values = [63_u64, 16_383, 1_073_741_823, (1_u64 << 62) - 1];

    for value in values {
        group.bench_with_input(format!("value={value}"), &value, |b, &value| {
            let varint = VarInt::from_u64(value).unwrap();
            b.iter(|| {
                let mut out = [0_u8; 8];
                let mut dst: &mut [u8] = &mut out;
                black_box(varint).encode(&mut dst);
                black_box(out);
            });
        });
    }

    group.finish();
}

fn bench_varint_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("varint/decode");
    let values = [63_u64, 16_383, 1_073_741_823, (1_u64 << 62) - 1];

    for value in values {
        let varint = VarInt::from_u64(value).unwrap();
        let mut encoded = [0_u8; 8];
        let mut dst: &mut [u8] = &mut encoded;
        varint.encode(&mut dst);
        let size = 8 - dst.len();

        group.bench_with_input(format!("value={value}"), &size, |b, &size| {
            b.iter(|| {
                let mut src = &encoded[..size];
                let decoded = VarInt::decode(&mut src).unwrap();
                black_box(decoded);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_varint_encode, bench_varint_decode);
criterion_main!(benches);
