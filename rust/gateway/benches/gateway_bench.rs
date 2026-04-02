use criterion::{criterion_group, criterion_main, Criterion};

fn bench_gateway_placeholder(c: &mut Criterion) {
    c.bench_function("gateway_placeholder", |b| {
        b.iter(|| {
            let x = 1u64;
            let y = 2u64;
            x + y
        })
    });
}

criterion_group!(benches, bench_gateway_placeholder);
criterion_main!(benches);
