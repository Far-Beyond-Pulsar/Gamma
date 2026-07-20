# Pulsar Gamma

A **DLL‑safe, data‑driven event system** for Rust plugin architectures.

Gamma lets the host and dynamically‑loaded plugins exchange events safely
even when they are compiled with different compiler versions, optimisation
levels, or feature flags.

## Architecture

```
gamma-core   ──► Event trait, EventBus, EventHandler
gamma-derive ──► #[pulsar_event] — single‑attribute event definition
gamma          ─► Umbrella crate that re‑exports both
```

## Usage

```toml
[dependencies]
gamma-core = "0.1"
gamma-derive = "0.1"
```

Or use the umbrella crate:

```toml
[dependencies]
gamma = { git = "https://github.com/your-org/gamma" }
```

### Example

```rust
use gamma_core::EventBus;
use gamma_derive::pulsar_event;

#[pulsar_event]                        // ← adds #[repr(C)] + implements Event
struct PlayerJumped {
    height: f32,
    timestamp: u64,
}

let mut bus = EventBus::new();

// Subscribe
bus.subscribe(|e: &PlayerJumped| {
    println!("Jumped {} at {}", e.height, e.timestamp);
});

// Publish
bus.publish(PlayerJumped { height: 5.0, timestamp: 12345 });
```

The same pattern works across a DLL boundary — the stable type ID
(`Event::stable_type_id()`) ensures that a plugin's event matches the
host's subscriber.

## DLL‑safety checklist

| Requirement | What does it? |
|---|---|
| [`#[pulsar_event]`](https://docs.rs/gamma-derive/latest/gamma_derive/attr.pulsar_event.html) | Applies `#[repr(C)]` **and** generates a deterministic `stable_type_id()`. No manual attributes needed. |
| Shared global allocator | `Box<dyn EventHandler>` created in one compilation unit may be dropped in another. Link the same allocator globally (e.g., `mimalloc` or `jemalloc`) to avoid allocator mismatches. |
| Versioning (optional) | If you plan to evolve event structs at runtime, add a `VERSION` constant to your event type and include it in `stable_type_id()`. |

## Crate structure

| Crate | Purpose |
|---|---|
| `gamma-core` | `Event` trait, `EventBus`, internal `EventHandler` — no dependencies beyond `std`. |
| `gamma-derive` | `#[pulsar_event]` attribute macro and `#[derive(Event)]` derive macro. |
| `gamma` (root) | Umbrella crate re‑exporting both. Use `use gamma::prelude::*;` for convenience. |

## Why not just use `TypeId`?

`TypeId::of::<T>()` is **not** stable across compilation units. Two separate
`rustc` invocations may assign different `TypeId` values to the same type.
Gamma replaces this with a deterministic hash that is:

- Computed from the type's **name**, **size**, and **alignment**
- Identical for the same struct compiled in different crates
- Different for structs with the same name but different field layouts

## License

Gamma is distributed under the [MIT License](LICENSE).
