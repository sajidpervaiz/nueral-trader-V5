use criterion::{criterion_group, criterion_main, Criterion};

fn bench_risk_placeholder(c: &mut Criterion) {
    c.bench_function("risk_placeholder", |b| {
        b.iter(|| {
            let exposure = 1000.0f64;
            let limit = 2000.0f64;
            exposure < limit
        })
    });
}

criterion_group!(benches, bench_risk_placeholder);
criterion_main!(benches);
