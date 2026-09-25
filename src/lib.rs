//! # gamma — a plugin-safe, data-driven event system
//!
//! Gamma is an event bus for engines with native plugins and scripts. This
//! umbrella crate re-exports [`gamma_core`] (runtime) and [`gamma_derive`]
//! (`#[pulsar_event]`).
//!
//! | Concern | Solution |
//! |---|---|
//! | Stable type identity | [`Event::stable_type_id()`] is a deterministic FNV-1a hash of the name, size and alignment (plus the field schema for dynamic events). No `TypeId`, no `Any` downcast. |
//! | Layout compatibility | `#[pulsar_event]` applies `#[repr(C)]`; handlers check size and alignment before casting. |
//! | Plugin boundary | [`ffi::RawBus`], a `#[repr(C)]` table of `extern "C"` functions, wrapped by [`ffi::ForeignBus`] on the plugin side. |
//! | Allocators | Handlers and deferred events are dropped by the library that created them; no shared allocator needed. |
//! | Scripts | [`EventDescriptor`] + [`DynEvent`]; `#[pulsar_event(dynamic)]` bridges Rust types both ways. |
//! | Targeting | [`Channel::Global`], [`Channel::Entity`], [`Channel::Class`]. |
//! | Ordering | Priority, then subscription order; deferred events flush in publish order. |
//!
//! ## Quick start
//!
//! ```rust
//! use gamma::prelude::*;
//!
//! #[pulsar_event(crate = gamma)]
//! struct PlayerJumped {
//!     height: f32,
//!     timestamp: u64,
//! }
//!
//! let bus = EventBus::new();
//! let _sub = bus.subscribe(|e: &PlayerJumped| {
//!     println!("Jumped {} at {}", e.height, e.timestamp);
//! });
//! bus.publish(PlayerJumped { height: 5.0, timestamp: 12345 });
//! ```
//!
//! When you depend only on `gamma` (not `gamma-core`), pass
//! `crate = gamma` to `#[pulsar_event]` so the generated code finds the
//! runtime.

pub use gamma_core::*;
pub use gamma_derive::{Event, pulsar_event};

/// Convenience re-exports for the most common types.
///
/// ```rust
/// use gamma::prelude::*;
/// ```
pub mod prelude {
    pub use crate::{
        Channel, DynEvent, DynValue, Event, EventBus, EventDescriptor, FieldType, FlushReport,
        SubscribeOptions, Subscription, SyncEventBus, SyncSubscription, pulsar_event,
    };
}

/// Compiles and runs the README's examples as doctests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
