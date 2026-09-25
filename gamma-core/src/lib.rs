//! Core runtime for the **Gamma** event system.
//!
//! Gamma is an event bus for engines with native plugins and scripts:
//!
//! * **Stable ids, no `TypeId`.** Events are routed by
//!   [`Event::stable_type_id`], a hash of the type name, size and alignment
//!   (plus the field schema for dynamic events). Handlers receive typed
//!   events through a layout-checked pointer cast, never through
//!   `Any::downcast_ref`, so an event published by a separately compiled
//!   plugin reaches host subscribers (and vice versa). See [`ffi`].
//! * **Subscription handles.** `subscribe` returns a [`Subscription`] that
//!   unsubscribes on drop (or [`detach`](Subscription::detach) it). It is safe
//!   to unsubscribe, subscribe or publish from inside a handler.
//! * **Immediate and deferred delivery.** [`EventBus::publish`] runs handlers
//!   now; [`EventBus::publish_deferred`] queues the event until
//!   [`EventBus::flush`].
//! * **Dynamic events.** [`EventDescriptor`] and [`DynEvent`] describe events
//!   at runtime (for scripts). Rust events declared with
//!   `#[pulsar_event(dynamic)]` convert both ways, so typed and dynamic
//!   subscribers see the same events.
//! * **Channels.** Every subscription and publish targets a [`Channel`]:
//!   `Global`, `Entity(id)` or `Class(id)`.
//! * **Deterministic order.** Higher [priority](SubscribeOptions::priority)
//!   first, then subscription order; a flush delivers in publish order.
//!
//! # Example
//!
//! ```rust
//! # use gamma_derive::pulsar_event;
//! use gamma_core::{Channel, EventBus, SubscribeOptions};
//!
//! #[pulsar_event]
//! struct PlayerJumped {
//!     height: f32,
//!     timestamp: u64,
//! }
//!
//! let bus = EventBus::new();
//!
//! // Keep the handle alive as long as you want the handler.
//! let sub = bus.subscribe(|e: &PlayerJumped| {
//!     println!("Jumped {} at {}", e.height, e.timestamp);
//! });
//!
//! bus.publish(PlayerJumped { height: 5.0, timestamp: 12345 });
//!
//! // Entity-targeted, deferred until the next flush.
//! let _only_42 = bus.subscribe_with(SubscribeOptions::channel(Channel::Entity(42)), |_: &PlayerJumped| {});
//! bus.publish_deferred_to(Channel::Entity(42), PlayerJumped { height: 1.0, timestamp: 1 });
//! let report = bus.flush();
//! assert_eq!(report.delivered, 1);
//!
//! sub.unsubscribe();
//! ```
//!
//! # Plugin safety
//!
//! What is guaranteed, and tested by `tests/cross_library.rs` against a
//! separately compiled `cdylib`:
//!
//! 1. A plugin reaches a host bus only through [`ffi::RawBus`], a `#[repr(C)]`
//!    table of `extern "C"` functions. No Rust-layout type (`Vec`, `String`,
//!    trait objects, `Arc`) crosses the boundary.
//! 2. Typed events cross as a pointer plus `(id, size, align)`. The receiver
//!    checks all three before casting. Event types must be `#[repr(C)]`
//!    (added by `#[pulsar_event]`) and should contain only FFI-safe fields
//!    if they are shared between libraries built by different compilers.
//! 3. Dynamic events cross in Gamma's own versioned byte encoding.
//! 4. A handler is always created **and dropped** by the library that
//!    subscribed it: the subscription carries a drop function from that
//!    library. Deferred events published by a plugin are dropped with the
//!    plugin's drop function. No shared global allocator is required.
//!
//! What remains the caller's responsibility: a plugin must drop all its
//! subscriptions, its [`ffi::ForeignBus`] handles and flush (or drop) any
//! events it queued before the plugin library is unloaded; and the stable id
//! is a contract, two types with the same name and layout are assumed to be
//! the same event.

#![warn(missing_docs)]

mod bus;
mod channel;
mod core;
mod dynamic;
pub mod ffi;
pub mod stable_id;

pub use bus::{
    EventBus, SubscribeOptions, Subscription, SyncEventBus, SyncSubscription, WeakEventBus,
    WeakSyncEventBus,
};
pub use channel::Channel;
pub use core::FlushReport;
pub use dynamic::{
    DecodeError, DynEvent, DynEventError, DynField, DynValue, EventDescriptor, FieldType,
    RegistryError, dynamic_event_id,
};

/// The default limit on flush rounds (see [`EventBus::flush`]).
pub const DEFAULT_MAX_FLUSH_ROUNDS: u32 = 16;

// ---------------------------------------------------------------------------
// Event trait
// ---------------------------------------------------------------------------

/// An event type.
///
/// Implement it with `#[pulsar_event]` (adds `#[repr(C)]`) or
/// `#[pulsar_event(dynamic)]` (also implements the dynamic conversions).
///
/// # Stable id
///
/// [`stable_type_id`](Event::stable_type_id) must be deterministic across
/// compilations and must differ for different events. The derive computes
/// it with [`stable_id::type_id`] (name, size, alignment) or, for dynamic
/// events, [`stable_id::type_id_with_fields`] (also the field names and
/// types). Bus handlers compare size and alignment again before casting, so
/// an id collision between types of different layout is dropped rather than
/// misread.
pub trait Event: 'static {
    /// Deterministic 64-bit identifier of this event type.
    fn stable_type_id() -> u64;

    /// `true` when [`descriptor`](Event::descriptor),
    /// [`to_dyn`](Event::to_dyn) and [`from_dyn`](Event::from_dyn) are
    /// implemented (`#[pulsar_event(dynamic)]`).
    const REFLECTED: bool = false;

    /// Runtime description of this event, for dynamic subscribers and
    /// scripts. `None` unless the type is reflected.
    fn descriptor() -> Option<EventDescriptor> {
        None
    }

    /// Convert to a dynamic event. `None` unless the type is reflected.
    fn to_dyn(&self) -> Option<DynEvent> {
        None
    }

    /// Build from a dynamic event with this type's id and fields. `None` on
    /// a mismatch or if the type is not reflected.
    fn from_dyn(event: &DynEvent) -> Option<Self>
    where
        Self: Sized,
    {
        let _ = event;
        None
    }
}

#[doc(hidden)]
pub mod __private {
    pub use crate::stable_id::{type_id, type_id_with_fields};
}
