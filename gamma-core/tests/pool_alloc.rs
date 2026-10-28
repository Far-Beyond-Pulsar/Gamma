//! Payload pool behaviour: allocated counts around the publish/flush round
//! trip. Allocation counting is global, so every test here holds one lock
//! for its whole run; other files' tests have their own binaries.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use gamma_core::{Channel, DynEvent, Event, SyncEventBus};
use gamma_derive::pulsar_event;

#[pulsar_event]
struct Ping(u32);

#[pulsar_event(dynamic)]
struct Hit {
    target: u64,
    damage: f64,
    title: String,
    flags: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Counting allocator
// ---------------------------------------------------------------------------

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static DEALLOCATED: AtomicUsize = AtomicUsize::new(0);
static ARMED: AtomicBool = AtomicBool::new(false);

/// Global allocation counter, armed per test (tests within this binary
/// share one counter, and each takes the process-wide lock).
#[global_allocator]
static COUNTED: Counted = Counted;
struct Counted;

unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCATED.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forwards to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            DEALLOCATED.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: paired with the alloc above.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCATED.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forwards to the system allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Serialises every allocation-counting test in this file: one sees the
/// counter exclusively (tests have their own binaries).
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn reset_counts() {
    ALLOCATED.store(0, Ordering::Relaxed);
    DEALLOCATED.store(0, Ordering::Relaxed);
}

fn allocated() -> usize {
    ALLOCATED.load(Ordering::Relaxed)
}

fn armed(on: bool) {
    ARMED.store(on, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Typed payload pool
// ---------------------------------------------------------------------------

/// Publishing and flushing the same small event is allocation-free in
/// steady state: the queue deque grew during warm-up, the payload buffer is
/// pooled, and the merge buffer keeps its capacity.
#[test]
fn pooled_typed_round_trip_is_zero_alloc() {
    let _g = TEST_LOCK.lock().unwrap();
    let bus = SyncEventBus::new();
    bus.set_pool_caps(64, 256);
    let hit = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hitc = hit.clone();
    let _s = bus.subscribe(move |_: &Ping| {
        hitc.fetch_add(1, Ordering::Relaxed);
    });
    const N: usize = 10_000;
    // Cold pass: one buffer alloc, deque growth, merge buffer growth.
    for i in 0..N as u32 {
        bus.publish_deferred(Ping(i));
    }
    let report = bus.flush();
    assert_eq!(report.delivered, N);

    // Warm pass. Publish is structural: each queued event holds a live
    // payload buffer until delivery, so a pool can only cover the flush
    // phase (buffers recycle delivery by delivery, not at queue time).
    for i in 0..N as u32 {
        bus.publish_deferred(Ping(i));
    }
    armed(true);
    reset_counts();
    bus.flush();
    assert_eq!(hit.load(Ordering::Relaxed), 2 * N);
    assert_eq!(
        allocated(),
        0,
        "steady-state flush delivered with pooled buffers"
    );
    armed(false);
}

// ---------------------------------------------------------------------------
// Dynamic conversion pool
// ---------------------------------------------------------------------------

/// A dynamic subscriber sees the pooled dynamic form: publishing many
/// events first, then flushing, delivers all of them from pooled buffers.
/// The only allocation inside the warm flush is one shared `Arc` handle
/// per event (the single conversion's owner), so conversions, strings,
/// byte vectors, fields vectors, and payload buffers all recycle.
#[test]
fn pooled_dyn_conversion_is_zero_alloc_in_steady_flush() {
    let _g = TEST_LOCK.lock().unwrap();
    let bus = SyncEventBus::new();
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hitc = hits.clone();
    let _s = bus.subscribe_dyn(
        Hit::stable_type_id(),
        gamma_core::SubscribeOptions::channel(Channel::Global),
        move |_: &DynEvent| {
            hitc.fetch_add(1, Ordering::Relaxed);
        },
    );
    const N: usize = 10_000;
    let publish_burst = |bus: &SyncEventBus| {
        for i in 0..N as u64 {
            bus.publish_deferred_to(
                Channel::Global,
                Hit {
                    target: i,
                    damage: 1.5,
                    title: String::from("steady-state-title"),
                    flags: Vec::from(&b"flags"[..]),
                },
            );
        }
    };
    // Cold run: pool primed (fields vec, strings, bytes, payload buffers).
    publish_burst(&bus);
    let report = bus.flush();
    assert_eq!(report.delivered, N);
    assert_eq!(hits.load(Ordering::Relaxed), N);

    // Publish burst is not measured (the caller builds every event).
    publish_burst(&bus);
    armed(true);
    reset_counts();
    bus.flush();
    assert_eq!(
        allocated(),
        N,
        "steady-state flush with dynamic conversions allocated"
    );
    assert_eq!(hits.load(Ordering::Relaxed), 2 * N);
    armed(false);
}
