# Changelog

## 0.2.0

Gamma v2: plugin-safe dispatch, subscription handles, deferred delivery,
dynamic events, channels and deterministic ordering (Pulsar-Native#923).

### Breaking changes and migration

| 0.1 | 0.2 |
|---|---|
| `bus.subscribe(f);` returns `()` | Returns a `Subscription` (`SyncSubscription` for `SyncEventBus`) that **unsubscribes when dropped**. Keep it (`let sub = …`, store it) or call `.detach()` for a permanent handler. `let _ = bus.subscribe(…)` drops it at once. The type is `#[must_use]`, so the old statement form now warns. |
| `subscribe(&mut self)` | All `EventBus` methods take `&self`; handlers may subscribe, unsubscribe and publish on the bus they run on. Remove `mut` from bus bindings. |
| `EventHandler` trait (public) | Removed; handlers are type-erased internally. |
| Shared global allocator required for plugins | Not required. Plugins use `ffi::ForeignBus` over an exported `ffi::RawBus`; do not share a Rust `EventBus` across a library boundary. |
| `Event` has only `stable_type_id()` | Also `REFLECTED`, `descriptor()`, `to_dyn()`, `from_dyn()` with defaults. Manual `impl Event` blocks keep compiling. |
| `EventBus` is `!Send` because of its handlers; `SyncEventBus` is `Send + Sync` | Unchanged. Both are now `Clone` (clones share the bus). |
| `parallel_publish` | Unchanged API; immediate only, and ignores priorities. |

Stable ids of existing `#[pulsar_event]` types are **unchanged**. Types
declared with the new `#[pulsar_event(dynamic)]` or `name = "…"` get new ids,
since the schema or the name is hashed.

When depending on the umbrella `gamma` crate only, use
`#[pulsar_event(crate = gamma)]`: the generated code refers to
`::gamma_core` by default.

### Added

- Dispatch by stable id plus a size and alignment check; the
  `Any::downcast_ref` path is gone.
- `ffi` module: `RawBus` / `RawHandler` / `RawEventRef` (`#[repr(C)]`,
  `extern "C"`, `ABI_VERSION = 1`), `EventBus::export_raw` /
  `SyncEventBus::export_raw` on the host, and `ForeignBus` /
  `ForeignSubscription` on the plugin side. A handler is dropped by the
  library that created it.
- `Subscription` / `SyncSubscription`: `unsubscribe()`, `detach()`,
  `is_active()`, `token()`. Unsubscribing inside a handler (including
  your own subscription) is deferred until the running dispatch ends.
- Deferred delivery: `publish_deferred[_to]`, `publish_dyn_deferred`,
  `flush()`, `flush_with_limit(n)`, `set_max_flush_rounds(n)` (default
  `DEFAULT_MAX_FLUSH_ROUNDS = 16`), `queued_len()`, and `FlushReport`.
- Dynamic events: `EventDescriptor`, `DynEvent`, `DynValue`, `FieldType`,
  `DynField`, a registry (`register_descriptor`, `register_event::<T>()`,
  `descriptor`, `descriptor_by_name`, `descriptors`), `subscribe_dyn`,
  `publish_dyn`, and a versioned wire encoding (`DynEvent::encode/decode`).
- `#[pulsar_event(dynamic, name = "...", crate = path)]` and
  `#[derive(Event)] #[event(...)]`.
- Channels: `Channel::{Global, Entity(u64), Class(u64)}`, `subscribe_with`,
  `publish_to`, `clear_channel`, `subscriber_count`. No fan-out between
  channels.
- Ordering: `SubscribeOptions::priority` (higher first, ties in
  subscription order).
- `serde` feature for the dynamic types and `Channel`; the umbrella crate
  forwards `parallel` and `serde`.
- `stable_id` module (the `const fn` hashing behind the derive).
- Tests: a cross-library test that builds and loads a `cdylib`, handle,
  deferred, dynamic, channel, ordering and thread-safety tests, and the
  README examples as doctests. The benches add N-subscriber, entity-channel,
  deferred flush, concurrent `publish_deferred` and dynamic-bridge cases.
