<p align="center">
    <img width="300" src="https://raw.githubusercontent.com/Far-Beyond-Pulsar/Gamma/refs/heads/main/assets/Gamma.png">
</p>

# Pulsar Gamma

A **plugin-safe, data-driven event system** for Rust engines with native
plugins and scripts.

- **Plugin-safe dispatch.** Events are routed by a stable 64-bit id, never
  by `TypeId`/`Any`. Plugins reach the host bus through a `#[repr(C)]`
  `extern "C"` table, and a test proves delivery both ways with a separately
  compiled `cdylib`.
- **Subscription handles.** `subscribe` returns a handle that unsubscribes on
  drop. Unsubscribing (even yourself) inside a handler is safe.
- **Immediate or deferred.** `publish` runs handlers now; `publish_deferred`
  queues until `flush()`, which runs at points you choose.
- **Dynamic events.** Runtime descriptors and `DynEvent` payloads for
  scripts, bridged both ways with Rust types declared
  `#[pulsar_event(dynamic)]`.
- **Channels.** `Global`, `Entity(id)` or `Class(id)`.
- **Deterministic order.** Priority, then subscription order; flushes deliver
  in publish order.

> **0.2 is a breaking release.** See [CHANGELOG.md](CHANGELOG.md) for the
> migration notes.

## Crates

```text
gamma-core   ──► Event trait, EventBus, SyncEventBus, dynamic events, ffi
gamma-derive ──► #[pulsar_event] / #[derive(Event)]
gamma        ──► umbrella crate re-exporting both (features: parallel, serde)
```

```toml
[dependencies]
gamma = { git = "https://github.com/Far-Beyond-Pulsar/gamma" }
```

With only the umbrella crate as a dependency, write
`#[pulsar_event(crate = gamma)]` so the generated code finds the runtime.

## Usage

```rust
use gamma_core::{Channel, EventBus, SubscribeOptions};
use gamma_derive::pulsar_event;

#[pulsar_event]                       // adds #[repr(C)] + implements Event
struct PlayerJumped {
    height: f32,
    player: u64,
}

let bus = EventBus::new();

// Keep the handle: dropping it unsubscribes. `.detach()` keeps it forever.
let sub = bus.subscribe(|e: &PlayerJumped| println!("jumped {}", e.height));

// Per-entity channel with a priority (higher runs first).
let only_7 = bus.subscribe_with(
    SubscribeOptions::channel(Channel::Entity(7)).priority(10),
    |e: &PlayerJumped| println!("entity 7 jumped {}", e.height),
);

bus.publish(PlayerJumped { height: 5.0, player: 1 });              // now, Global
bus.publish_deferred_to(Channel::Entity(7), PlayerJumped { height: 2.0, player: 7 });
let report = bus.flush();                                          // deliver the queue
assert_eq!(report.delivered, 1);

sub.unsubscribe();
drop(only_7);
```

### Deferred delivery

`publish_deferred` queues; `flush()` delivers the queue oldest-first.
Handlers that queue more events during a flush get them delivered in the same
flush, one *round* per generation, up to `max_flush_rounds` (default 16,
`set_max_flush_rounds` / `flush_with_limit`). The `FlushReport` says how many
events were delivered, how many rounds ran, whether the limit was hit and how
many remain. A `flush` called while another flush of the same bus is running
(from a handler or another thread) does nothing and reports
`already_flushing`. `flush` swaps the queue out under the lock and delivers
after releasing it, so publishers never wait for handlers.

Immediate `publish` inside a handler still runs inline (re-entrant); prefer
`publish_deferred` from handlers, especially while holding engine locks.

### Dynamic events (scripts)

```rust
use gamma_core::{Channel, DynEvent, DynValue, Event, EventBus, EventDescriptor, FieldType, SubscribeOptions};
use gamma_derive::pulsar_event;

#[pulsar_event(dynamic, name = "physics.Hit")]
struct Hit {
    target: u64,
    damage: f64,
}

let bus = EventBus::new();
bus.register_event::<Hit>().unwrap();               // Rust type -> descriptor

// A script-declared event.
let opened = EventDescriptor::dynamic("Door.Opened", [("door", FieldType::U64)]);
bus.register_descriptor(opened.clone()).unwrap();

// A dynamic subscriber receives the Rust-typed event…
let _s1 = bus.subscribe_dyn(Hit::stable_type_id(), SubscribeOptions::default(), |e: &DynEvent| {
    println!("hit fields: {:?}", e.fields);
});
bus.publish(Hit { target: 3, damage: 10.0 });

// …and a typed subscriber receives a matching dynamic event.
let _s2 = bus.subscribe(|h: &Hit| println!("typed hit on {}", h.target));
let id = bus.descriptor_by_name("physics.Hit").unwrap().id;
bus.publish_dyn(Channel::Global, &DynEvent::new(id, vec![DynValue::U64(4), DynValue::F64(1.0)])).unwrap();
```

`DynValue` is `Bool`, `I64`, `F64`, `U64` (entity handles, ids), `Str` or
`Bytes`. Rust fields map through the `DynField` trait (implemented for
`bool`, all integers with range checks, `f32`/`f64`, `String`, `Vec<u8>`;
implement it for your own newtypes). `publish_dyn` checks the event against
its registered descriptor. For a dynamic Rust type the field schema is part
of the stable id. The `serde` feature derives `Serialize`/`Deserialize` for
the dynamic types and `Channel`.

### Channels

A subscriber listens on one channel and a publish targets one channel. There
is **no fan-out**: an event sent to `Entity(7)` reaches only `Entity(7)`
subscribers, not `Global` or `Class` ones. Publish twice if both audiences
need it. `clear_channel(Channel::Entity(e))` drops every subscription of a
destroyed entity.

### Threads

`EventBus` is single-threaded (`!Send`, handlers need not be `Send`) and uses
no atomics on the publish path. `SyncEventBus` has the same API with
`Send + Sync` handlers. `parallel_publish` (immediate only, priorities
ignored) runs handlers on rayon with the `parallel` feature and on scoped
threads without it.

## Plugin safety

These claims are tested by `gamma-core/tests/cross_library.rs`. It builds
`tests/fixtures/plugin` as a `cdylib` in its own cargo invocation (release
profile, separate target dir, so it has its own copy of gamma-core), loads it
with `libloading`, and runs the scenario against both `EventBus` and
`SyncEventBus`.

| Claim | How | Tested by |
|---|---|---|
| A plugin's event reaches host subscribers and vice versa, even though the two `Ping` types have different `TypeId`s | Dispatch by `stable_type_id()`; the handler checks the id, size and alignment, then casts. No `Any`. | Steps 1–2 (and an assertion that the `TypeId`s differ) |
| Channels hold across the boundary | `RawChannel` in every call | Step 3 |
| Typed ↔ dynamic bridging across the boundary | Dynamic data crosses in Gamma's versioned byte encoding; a plugin's `to_dyn` writes into a host-owned sink | Steps 4–6 |
| Invalid dynamic events from a plugin are refused | The host validates against the registry | Step 5 |
| No allocator is shared | A handler is a `RawHandler` whose `drop` function comes from the plugin; deferred plugin events are dropped with the plugin's drop glue; bytes are always copied | Steps 7–8 and `plugin_handlers_dropped_in_plugin_when_bus_is_dropped` |
| Bad pointers or layouts from a plugin are rejected | Null/misaligned data, bad alignment and bad channels return an error status | `raw_publish_rejects_bad_layouts` |

Host side:

```rust,ignore
let bus = SyncEventBus::new();
let init: Symbol<unsafe extern "C" fn(RawBus) -> u32> = lib.get(b"plugin_init")?;
init(bus.export_raw());                       // hands the plugin a strong reference
```

Plugin side:

```rust,ignore
#[unsafe(no_mangle)]
pub unsafe extern "C" fn plugin_init(raw: RawBus) -> u32 {
    let bus = unsafe { ForeignBus::from_raw(raw) }.unwrap();   // checks ABI_VERSION
    let sub = bus.subscribe(|p: &Ping| { /* … */ });            // Send + Sync handlers
    bus.publish(Ping { value: 1 });
    // keep `bus` and `sub` somewhere; drop both before unloading
    0
}
```

What you are still responsible for:

- **Unloading.** Drop every `ForeignSubscription` and `ForeignBus`, and
  flush or drop the host bus's queued events from that plugin, before
  unloading the plugin: the bus calls back into its code.
- **The id is a contract.** Two event types with the same name and layout
  (and schema, for dynamic ones) are treated as the same event. Use
  `name = "..."` to namespace. Prefer FFI-safe fields (numbers, `bool`,
  arrays, `#[repr(C)]` structs) for typed events that cross between
  libraries built by different compilers. `String`/`Vec` fields are only
  layout-compatible with the same compiler; the dynamic path has no such
  limit.
- **Threads.** A bus exported from `EventBus` is not thread-safe
  (`ForeignBus::is_thread_safe()` is false): use it only on the host's bus
  thread.
- **Panics** that reach an `extern "C"` function abort the process.

## Performance

`cargo bench -p gamma-core --features parallel` on a 4-vCPU Intel Xeon
2.1 GHz cloud VM (short runs: `--warm-up-time 0.3 --measurement-time 1`).
Numbers are indicative; compare runs on the same machine.

### Immediate publish

| Scenario | 0.2 |
|---|---|
| `EventBus::publish`, 0 subscribers | 3.4 ns |
| `EventBus::publish`, 1 subscriber | 18 ns |
| `EventBus::publish`, 10 / 50 / 200 subscribers | 33 / 125 / 391 ns |
| `EventBus::publish`, 1 KB event, 1 subscriber | 101 ns |
| `publish_to(Entity(500))`, 1000 entities with 1 subscriber each | 13 ns |
| `subscribe` + `unsubscribe` | 133 ns |
| typed publish → dynamic subscriber (one `to_dyn`) | 55 ns |
| dynamic publish → typed subscriber (validate + `from_dyn`) | 19 ns |
| `SyncEventBus::publish`, 1 subscriber | 39 ns |
| `SyncEventBus::publish` from 1 / 2 / 4 / 8 threads (thread spawn included) | 48 / 75 / 124 / 226 µs |
| `parallel_publish` overhead, 2 / 4 / 8 no-op subscribers (rayon) | 29 / 48 / 52 µs |
| 8 subscribers × ~10 µs work: sequential vs `parallel_publish` | 89 µs vs 35 µs |

### Deferred delivery

| Scenario | EventBus | SyncEventBus |
|---|---|---|
| `flush` of 100 queued events, 1 subscriber | 2.9 µs (35 M events/s) | 5.7 µs (17 M/s) |
| `flush` of 1 000 | 35 µs (28 M/s) | 58 µs (17 M/s) |
| `flush` of 10 000 | 337 µs (30 M/s) | 559 µs (18 M/s) |
| `publish_deferred` + `flush`, 1 000 events, 4 subscribers | 81 µs | |

Concurrent `SyncEventBus::publish_deferred` (1 000 events per thread, enqueue
only, thread spawn included):

| Threads | Time | Throughput |
|---|---|---|
| 1 | 115 µs | 8.7 M events/s |
| 2 | 368 µs | 5.4 M events/s |
| 4 | 864 µs | 4.6 M events/s |
| 8 | 1.86 ms | 4.3 M events/s |

The deferred queue is one `Mutex<VecDeque>`, so throughput drops under
contention. It is held only for the push and for the swap in `flush`.

### Compared with 0.1

The same tight loop (`publish` of an empty event, release build, same
machine), with instruction counts from callgrind:

| | 0.1 | 0.2 |
|---|---|---|
| `EventBus`, 0 subscribers | 0.6 ns / 5 instr | 3.5 ns / 53 instr |
| `EventBus`, 1 subscriber | 4.4 ns / 56 instr | 19 ns / ~190 instr |
| `EventBus`, 50 subscribers | 172 ns | 142 ns |
| `SyncEventBus`, 1 subscriber | 21 ns / 90 instr | 39 ns / ~193 instr |

The fixed per-publish cost grew from the channel-aware lookup, the
re-entrancy-safe snapshot of the subscriber list, and the lazily converted
dynamic view. The per-subscriber cost is lower than 0.1's `Any` downcast,
so 0.2 is faster from about 50 subscribers up.

## Case study: global and per-entity events

Unreal-style engines have global events (`Tick`, `BeginPlay`) and
per-instance events (`OnActorHit`). With 0.1 you needed one `EventBus` per
actor for the latter. With channels, one bus serves both:

```rust
use gamma_core::{Channel, EventBus, SubscribeOptions, Subscription};
use gamma_derive::pulsar_event;

#[pulsar_event]
struct OnDied { killer: u64 }

struct Monster { entity: u64, subs: Vec<Subscription> }

let world = EventBus::new();
let mut goblin = Monster { entity: 42, subs: Vec::new() };

// React when *this* goblin dies…
goblin.subs.push(world.subscribe_with(
    SubscribeOptions::channel(Channel::Entity(goblin.entity)),
    |e: &OnDied| println!("goblin killed by {}", e.killer),
));
// …and log every death globally.
let _log = world.subscribe(|e: &OnDied| println!("something died ({})", e.killer));

world.publish_to(Channel::Entity(42), OnDied { killer: 7 });   // only the goblin's handler
world.publish(OnDied { killer: 7 });                           // only the global logger

// Despawn: dropping the handles (or clear_channel) removes the handlers.
drop(goblin);
```

A bus per actor still works (`EventBus` is cheap) if you prefer ownership
over routing.

## Why not `TypeId`?

`TypeId::of::<T>()` differs between separately compiled libraries (the test
suite checks this for its plugin), so an `Any`-based bus silently drops
plugin events. Gamma's id is a deterministic hash of the name, size and
alignment (plus field names and types for dynamic events), identical in
every library that declares the same event.

## License

Gamma is distributed under the [MIT License](LICENSE).
