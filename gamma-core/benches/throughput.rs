//! Throughput benchmarks for the Gamma event bus.
//!
//! Run:
//!   cargo bench -p gamma-core                          (std::thread::scope backend)
//!   cargo bench -p gamma-core --features parallel       (rayon thread-pool backend)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};

use gamma_core::Event;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

#[repr(C)]
struct EmptyEvent;

impl Event for EmptyEvent {
    fn stable_type_id() -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in stringify!(EmptyEvent).as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= std::mem::size_of::<Self>() as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        hash ^= std::mem::align_of::<Self>() as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        hash
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct LargeEvent {
    buffer: [u8; 1024],
    checksum: u64,
}

impl Event for LargeEvent {
    fn stable_type_id() -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in stringify!(LargeEvent).as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= std::mem::size_of::<Self>() as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        hash ^= std::mem::align_of::<Self>() as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        hash
    }
}

fn empty_event() -> EmptyEvent { EmptyEvent }
fn large_event() -> LargeEvent {
    LargeEvent { buffer: [0xAB; 1024], checksum: 0xDEAD_BEEF }
}

/// Simulate `n` iterations of busy-work that the compiler cannot elide.
fn busy(n: u64, seed: u64) -> u64 {
    let mut v = seed;
    for _ in 0..n {
        v = black_box(v).wrapping_mul(0x100000001b3);
    }
    v
}

// ---------------------------------------------------------------------------
// Baseline: raw throughput for empty work
// ---------------------------------------------------------------------------

fn bench_eventbus_publish_empty(c: &mut Criterion) {
    let mut bus = gamma_core::EventBus::new();
    bus.subscribe(|_: &EmptyEvent| black_box(()));
    c.bench_function("EventBus::publish / EmptyEvent / 1 sub", |b| {
        b.iter_batched(empty_event, |e| bus.publish(black_box(e)), BatchSize::SmallInput)
    });
}

fn bench_eventbus_publish_large(c: &mut Criterion) {
    let mut bus = gamma_core::EventBus::new();
    bus.subscribe(|e: &LargeEvent| { black_box(e.checksum); });
    c.bench_function("EventBus::publish / 1KB event / 1 sub", |b| {
        b.iter_batched(large_event, |e| bus.publish(black_box(e)), BatchSize::SmallInput)
    });
}

fn bench_eventbus_many_subs(c: &mut Criterion) {
    let mut bus = gamma_core::EventBus::new();
    for _ in 0..50 {
        bus.subscribe(|_: &EmptyEvent| black_box(()));
    }
    c.bench_function("EventBus::publish / EmptyEvent / 50 subs", |b| {
        b.iter_batched(empty_event, |e| bus.publish(black_box(e)), BatchSize::SmallInput)
    });
}

fn bench_eventbus_subscribe(c: &mut Criterion) {
    c.bench_function("EventBus::subscribe / 1 closure", |b| {
        b.iter(|| {
            let mut bus = gamma_core::EventBus::new();
            bus.subscribe(|_: &EmptyEvent| black_box(()));
        });
    });
}

// ---------------------------------------------------------------------------
// SyncEventBus single-threaded overhead
// ---------------------------------------------------------------------------

fn bench_sync_bus_publish(c: &mut Criterion) {
    let bus = gamma_core::SyncEventBus::new();
    bus.subscribe(|_: &EmptyEvent| black_box(()));
    c.bench_function("SyncEventBus::publish / EmptyEvent / 1 sub", |b| {
        b.iter_batched(empty_event, |e| bus.publish(black_box(e)), BatchSize::SmallInput)
    });
}

// ---------------------------------------------------------------------------
// SyncEventBus cross-thread (true concurrency — N callers, one publish each)
// ---------------------------------------------------------------------------

fn bench_sync_bus_concurrent_publish(c: &mut Criterion) {
    let bus = Arc::new(gamma_core::SyncEventBus::new());
    bus.subscribe(|_: &EmptyEvent| black_box(()));

    let mut group = c.benchmark_group("SyncEventBus (cross-thread publish)");
    for threads in [1, 2, 4, 8] {
        group.bench_function(format!("publish {}T", threads), |b| {
            let bus = Arc::clone(&bus);
            b.iter(|| {
                std::thread::scope(|s| {
                    for _ in 0..threads {
                        s.spawn(|| { bus.publish(EmptyEvent); });
                    }
                });
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Sequential vs parallel dispatch for handlers with real work
// ---------------------------------------------------------------------------

/// Helper: register `n` subscribers that each do `work_iters` of busy-work.
fn register_workers(
    bus: &gamma_core::SyncEventBus,
    n: usize,
    work_iters: u64,
) -> Arc<AtomicU64> {
    let total = Arc::new(AtomicU64::new(0));
    for i in 0..n {
        let total = Arc::clone(&total);
        bus.subscribe(move |_: &EmptyEvent| {
            let r = busy(work_iters, i as u64);
            total.fetch_add(r, Ordering::Relaxed);
        });
    }
    total
}

fn bench_parallel_vs_seq(c: &mut Criterion) {
    let mut group = c.benchmark_group("Parallel vs Sequential");

    // Fixed at 8 subscribers, vary the per-handler work
    for (work_label, work_iters) in [("~0.1µs", 100), ("~1µs", 1_000), ("~10µs", 10_000)] {
        // Sequential
        let bus_seq = gamma_core::SyncEventBus::new();
        let _t = register_workers(&bus_seq, 8, work_iters);
        group.bench_function(format!("seq {} x8", work_label), |b| {
            b.iter_batched(empty_event, |e| bus_seq.publish(black_box(e)), BatchSize::SmallInput)
        });

        // Parallel
        let bus_par = gamma_core::SyncEventBus::new();
        let _t = register_workers(&bus_par, 8, work_iters);
        group.bench_function(format!("parallel {} x8", work_label), |b| {
            b.iter_batched(empty_event, |e| bus_par.parallel_publish(black_box(e)), BatchSize::SmallInput)
        });
    }

    // Sweep subscriber count with fixed ~1µs work per handler
    for n_subs in [2, 4, 8, 16] {
        let bus_seq = gamma_core::SyncEventBus::new();
        let _t = register_workers(&bus_seq, n_subs, 1_000);
        group.bench_function(format!("seq ~1µs x{} subs", n_subs), |b| {
            b.iter_batched(empty_event, |e| bus_seq.publish(black_box(e)), BatchSize::SmallInput)
        });

        let bus_par = gamma_core::SyncEventBus::new();
        let _t = register_workers(&bus_par, n_subs, 1_000);
        group.bench_function(format!("parallel ~1µs x{} subs", n_subs), |b| {
            b.iter_batched(empty_event, |e| bus_par.parallel_publish(black_box(e)), BatchSize::SmallInput)
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Overhead of parallel dispatch with no-op handlers
// ---------------------------------------------------------------------------

fn bench_parallel_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("Parallel dispatch overhead (no-op handlers)");

    for n_subs in [2, 4, 8] {
        let bus = gamma_core::SyncEventBus::new();
        for _ in 0..n_subs {
            bus.subscribe(|_: &EmptyEvent| black_box(()));
        }
        group.bench_function(format!("parallel {} subs", n_subs), |b| {
            b.iter_batched(empty_event, |e| bus.parallel_publish(black_box(e)), BatchSize::SmallInput)
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    // EventBus baselines
    bench_eventbus_publish_empty,
    bench_eventbus_publish_large,
    bench_eventbus_many_subs,
    bench_eventbus_subscribe,
    // SyncEventBus baselines
    bench_sync_bus_publish,
    bench_sync_bus_concurrent_publish,
    // Parallel vs sequential comparison
    bench_parallel_vs_seq,
    bench_parallel_overhead,
);

criterion_main!(benches);
