//! End-to-end deferred queue throughput at frame-sized event counts.
//!
//! Run the same target and Criterion settings on the single-queue parent and
//! the sharded branch to compare producer + flush latency.

use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use gamma_core::{Channel, SubscribeOptions, SyncEventBus};
use gamma_derive::pulsar_event;

#[pulsar_event]
struct QueueEvent(u64);

fn bench_pipelined_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("deferred producer + flush pipeline");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for total_events in [10_000usize, 100_000] {
        group.throughput(Throughput::Elements(total_events as u64));
        for producers in [1usize, 4, 8] {
            let bus = SyncEventBus::new();
            for producer in 0..producers {
                bus.subscribe_with(
                    SubscribeOptions::channel(Channel::Entity(producer as u64)),
                    move |event: &QueueEvent| {
                        black_box(event.0);
                    },
                )
                .detach();
            }
            let per_producer = total_events / producers;
            group.bench_function(
                format!("{total_events} events / {producers} producers"),
                |b| {
                    b.iter_custom(|iterations| {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let start = Instant::now();
                            std::thread::scope(|scope| {
                                for producer in 0..producers {
                                    let bus = &bus;
                                    scope.spawn(move || {
                                        for event in 0..per_producer {
                                            bus.publish_deferred_to(
                                                Channel::Entity(producer as u64),
                                                QueueEvent(
                                                    (producer * per_producer + event) as u64,
                                                ),
                                            );
                                        }
                                    });
                                }
                            });
                            black_box(bus.flush());
                            elapsed += start.elapsed();
                        }
                        elapsed
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_pipelined_queue);
criterion_main!(benches);
