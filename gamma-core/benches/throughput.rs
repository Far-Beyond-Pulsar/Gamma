//! Throughput benchmarks for the Gamma event bus.
//!
//! Run:
//!   cargo bench -p gamma-core                          (std::thread::scope backend)
//!   cargo bench -p gamma-core --features parallel       (rayon thread-pool backend)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

use gamma_core::{Channel, DynEvent, DynValue, Event, EventBus, SubscribeOptions, SyncEventBus};
use gamma_derive::pulsar_event;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

#[pulsar_event]
struct EmptyEvent;

#[pulsar_event]
#[derive(Clone, Copy)]
struct LargeEvent {
    buffer: [u8; 1024],
    checksum: u64,
}

#[pulsar_event(dynamic)]
#[derive(Clone, Copy)]
struct Hit {
    target: u64,
    damage: f64,
}

fn large_event() -> LargeEvent {
    LargeEvent {
        buffer: [0xAB; 1024],
        checksum: 0xDEAD_BEEF,
    }
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
// EventBus: immediate publish
// ---------------------------------------------------------------------------

fn bench_publish_n_subs(c: &mut Criterion) {
    let mut group = c.benchmark_group("EventBus::publish (EmptyEvent)");
    for n in [0usize, 1, 10, 50, 200] {
        let bus = EventBus::new();
        for _ in 0..n {
            bus.subscribe(|_: &EmptyEvent| black_box(())).detach();
        }
        group.bench_with_input(BenchmarkId::new("subs", n), &n, |b, _| {
            b.iter(|| bus.publish(black_box(EmptyEvent)))
        });
    }
    group.finish();
}

fn bench_publish_large(c: &mut Criterion) {
    let bus = EventBus::new();
    bus.subscribe(|e: &LargeEvent| {
        black_box(e.checksum);
    })
    .detach();
    c.bench_function("EventBus::publish / 1KB event / 1 sub", |b| {
        b.iter_batched(
            large_event,
            |e| bus.publish(black_box(e)),
            BatchSize::SmallInput,
        )
    });
}

fn bench_publish_entity_channel(c: &mut Criterion) {
    // 1000 entities with one subscriber each; publish to one of them.
    let bus = EventBus::new();
    for e in 0..1000u64 {
        bus.subscribe_with(SubscribeOptions::channel(Channel::Entity(e)), |h: &Hit| {
            black_box(h.damage);
        })
        .detach();
    }
    c.bench_function(
        "EventBus::publish_to / Entity channel / 1 of 1000 entities",
        |b| {
            b.iter(|| {
                bus.publish_to(
                    Channel::Entity(black_box(500)),
                    Hit {
                        target: 500,
                        damage: 1.0,
                    },
                )
            })
        },
    );
}

fn bench_subscribe(c: &mut Criterion) {
    c.bench_function("EventBus::subscribe + unsubscribe", |b| {
        let bus = EventBus::new();
        b.iter(|| bus.subscribe(|_: &EmptyEvent| black_box(())).unsubscribe());
    });
}

// ---------------------------------------------------------------------------
// Deferred delivery
// ---------------------------------------------------------------------------

fn bench_deferred_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("EventBus deferred");
    for batch in [1usize, 100, 1000] {
        let bus = EventBus::new();
        for _ in 0..4 {
            bus.subscribe(|h: &Hit| {
                black_box(h.target);
            })
            .detach();
        }
        group.bench_with_input(
            BenchmarkId::new("publish_deferred+flush / 4 subs / events", batch),
            &batch,
            |b, &batch| {
                b.iter(|| {
                    for i in 0..batch {
                        bus.publish_deferred(Hit {
                            target: i as u64,
                            damage: 1.0,
                        });
                    }
                    black_box(bus.flush())
                })
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Dynamic events
// ---------------------------------------------------------------------------

fn bench_dynamic(c: &mut Criterion) {
    let mut group = c.benchmark_group("Dynamic events");

    // Typed publish, dynamic subscriber (to_dyn once per publish).
    let bus = EventBus::new();
    bus.register_event::<Hit>().unwrap();
    bus.subscribe_dyn(
        Hit::stable_type_id(),
        SubscribeOptions::default(),
        |d: &DynEvent| {
            black_box(d.fields.len());
        },
    )
    .detach();
    group.bench_function("typed publish -> dyn subscriber", |b| {
        b.iter(|| {
            bus.publish(Hit {
                target: 1,
                damage: 2.0,
            })
        })
    });

    // Dynamic publish, typed subscriber (from_dyn per subscriber).
    let bus = EventBus::new();
    bus.register_event::<Hit>().unwrap();
    bus.subscribe(|h: &Hit| {
        black_box(h.target);
    })
    .detach();
    let ev = DynEvent::new(
        Hit::stable_type_id(),
        vec![DynValue::U64(1), DynValue::F64(2.0)],
    );
    group.bench_function("dyn publish -> typed subscriber", |b| {
        b.iter(|| bus.publish_dyn(Channel::Global, black_box(&ev)).unwrap())
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// SyncEventBus
// ---------------------------------------------------------------------------

fn bench_sync_bus_publish(c: &mut Criterion) {
    let bus = SyncEventBus::new();
    bus.subscribe(|_: &EmptyEvent| black_box(())).detach();
    c.bench_function("SyncEventBus::publish / EmptyEvent / 1 sub", |b| {
        b.iter(|| bus.publish(black_box(EmptyEvent)))
    });
}

fn bench_sync_bus_concurrent_publish(c: &mut Criterion) {
    let bus = Arc::new(SyncEventBus::new());
    bus.subscribe(|_: &EmptyEvent| black_box(())).detach();

    let mut group = c.benchmark_group("SyncEventBus (cross-thread publish)");
    for threads in [1, 2, 4, 8] {
        group.bench_function(format!("publish {}T", threads), |b| {
            let bus = Arc::clone(&bus);
            b.iter(|| {
                std::thread::scope(|s| {
                    for _ in 0..threads {
                        s.spawn(|| bus.publish(EmptyEvent));
                    }
                });
            });
        });
    }
    group.finish();
}

/// Concurrent `publish_deferred` from N threads onto one queue. Only the
/// enqueue phase is timed (thread start-up included); the flush that
/// drains the queue after each iteration is not.
fn bench_sync_bus_concurrent_deferred(c: &mut Criterion) {
    const PER_THREAD: usize = 1000;
    let bus = SyncEventBus::new();
    bus.subscribe(|h: &Hit| {
        black_box(h.target);
    })
    .detach();

    let mut group =
        c.benchmark_group("SyncEventBus::publish_deferred (cross-thread, 1000 events/thread)");
    for threads in [1usize, 2, 4, 8] {
        group.throughput(criterion::Throughput::Elements(
            (threads * PER_THREAD) as u64,
        ));
        group.bench_function(format!("{}T enqueue", threads), |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    std::thread::scope(|s| {
                        for t in 0..threads {
                            let bus = &bus;
                            s.spawn(move || {
                                for i in 0..PER_THREAD {
                                    bus.publish_deferred(Hit {
                                        target: (t * PER_THREAD + i) as u64,
                                        damage: 1.0,
                                    });
                                }
                            });
                        }
                    });
                    total += start.elapsed();
                    black_box(bus.flush());
                }
                total
            });
        });
    }
    group.finish();
}

/// `flush` alone: N events are queued in the (untimed) setup.
fn bench_flush_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush of N queued events (1 sub)");
    for n in [100usize, 1000, 10_000] {
        let bus = EventBus::new();
        bus.subscribe(|h: &Hit| {
            black_box(h.target);
        })
        .detach();
        group.throughput(criterion::Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("EventBus", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    for i in 0..n {
                        bus.publish_deferred(Hit {
                            target: i as u64,
                            damage: 1.0,
                        });
                    }
                },
                |()| black_box(bus.flush()),
                BatchSize::PerIteration,
            )
        });

        let sbus = SyncEventBus::new();
        sbus.subscribe(|h: &Hit| {
            black_box(h.target);
        })
        .detach();
        group.bench_with_input(BenchmarkId::new("SyncEventBus", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    for i in 0..n {
                        sbus.publish_deferred(Hit {
                            target: i as u64,
                            damage: 1.0,
                        });
                    }
                },
                |()| black_box(sbus.flush()),
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

/// Register `n` subscribers that each do `work_iters` of busy-work.
fn register_workers(bus: &SyncEventBus, n: usize, work_iters: u64) -> Arc<AtomicU64> {
    let total = Arc::new(AtomicU64::new(0));
    for i in 0..n {
        let total = Arc::clone(&total);
        bus.subscribe(move |_: &EmptyEvent| {
            let r = busy(work_iters, i as u64);
            total.fetch_add(r, Ordering::Relaxed);
        })
        .detach();
    }
    total
}

fn bench_parallel_vs_seq(c: &mut Criterion) {
    let mut group = c.benchmark_group("Parallel vs Sequential");
    for (work_label, work_iters) in [("~0.1µs", 100), ("~1µs", 1_000), ("~10µs", 10_000)] {
        let bus_seq = SyncEventBus::new();
        let _t = register_workers(&bus_seq, 8, work_iters);
        group.bench_function(format!("seq {} x8", work_label), |b| {
            b.iter(|| bus_seq.publish(black_box(EmptyEvent)))
        });

        let bus_par = SyncEventBus::new();
        let _t = register_workers(&bus_par, 8, work_iters);
        group.bench_function(format!("parallel {} x8", work_label), |b| {
            b.iter(|| bus_par.parallel_publish(black_box(EmptyEvent)))
        });
    }
    group.finish();
}

fn bench_parallel_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("Parallel dispatch overhead (no-op handlers)");
    for n_subs in [2, 4, 8] {
        let bus = SyncEventBus::new();
        for _ in 0..n_subs {
            bus.subscribe(|_: &EmptyEvent| black_box(())).detach();
        }
        group.bench_function(format!("parallel {} subs", n_subs), |b| {
            b.iter(|| bus.parallel_publish(black_box(EmptyEvent)))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_publish_n_subs,
    bench_publish_large,
    bench_publish_entity_channel,
    bench_subscribe,
    bench_deferred_flush,
    bench_dynamic,
    bench_sync_bus_publish,
    bench_sync_bus_concurrent_publish,
    bench_sync_bus_concurrent_deferred,
    bench_flush_only,
    bench_parallel_vs_seq,
    bench_parallel_overhead,
);

criterion_main!(benches);
