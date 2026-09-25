//! [`EventBus`], [`SyncEventBus`] and their subscription handles.

use std::sync::{Arc, Weak};

use crate::core::{
    Core, DynHandler, ErasedHandler, EventView, OwnedPayload, OwnedTyped, Queued, ToDyn,
    TypedHandler, entry,
};
use crate::ffi::{self, RawBus};
use crate::{Channel, DynEvent, DynEventError, Event, EventDescriptor, FlushReport, RegistryError};

/// Channel and priority for a subscription.
///
/// ```rust
/// use gamma_core::{Channel, SubscribeOptions};
/// let opts = SubscribeOptions::channel(Channel::Entity(7)).priority(10);
/// assert_eq!(opts.channel, Channel::Entity(7));
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubscribeOptions {
    /// The channel to listen on (default [`Channel::Global`]).
    pub channel: Channel,
    /// Higher runs first; equal priorities run in subscription order
    /// (default 0).
    pub priority: i32,
}

impl SubscribeOptions {
    /// Options for `channel` with priority 0.
    pub fn channel(channel: Channel) -> Self {
        Self {
            channel,
            priority: 0,
        }
    }

    /// Set the priority.
    pub fn priority(self, priority: i32) -> Self {
        Self { priority, ..self }
    }
}

type LocalCore = Core<dyn ErasedHandler>;
type SyncCore = Core<dyn ErasedHandler + Send + Sync>;

// The two buses share every method except for the `Send + Sync` bounds on
// handlers and events; this macro writes the common part once.
macro_rules! common_api {
    ($Sub:ident, $($handler_bounds:tt)*) => {
        /// Subscribe to `T` on the global channel with priority 0.
        ///
        /// The handler stays registered until the returned handle is dropped,
        /// unsubscribed or detached.
        pub fn subscribe<T: Event, F>(&self, handler: F) -> $Sub
        where
            F: Fn(&T) + $($handler_bounds)*,
        {
            self.subscribe_with(SubscribeOptions::default(), handler)
        }

        /// Subscribe to `T` with a channel and priority.
        ///
        /// If `T` is `#[pulsar_event(dynamic)]`, the handler also receives
        /// [`DynEvent`]s with `T`'s id (converted with `T::from_dyn`).
        pub fn subscribe_with<T: Event, F>(&self, opts: SubscribeOptions, handler: F) -> $Sub
        where
            F: Fn(&T) + $($handler_bounds)*,
        {
            let token = self.core.next_token();
            self.core.insert(T::stable_type_id(), opts.channel, entry(token, opts.priority, TypedHandler::<T, F>::new(handler)));
            $Sub { core: Some(Arc::downgrade(&self.core)), token }
        }

        /// Subscribe to event `id` as a [`DynEvent`].
        ///
        /// Receives dynamic events with this id, and typed events of a
        /// reflected Rust type with this id (converted with `to_dyn`).
        pub fn subscribe_dyn<F>(&self, id: u64, opts: SubscribeOptions, handler: F) -> $Sub
        where
            F: Fn(&DynEvent) + $($handler_bounds)*,
        {
            let token = self.core.next_token();
            self.core.insert(id, opts.channel, entry(token, opts.priority, DynHandler(handler)));
            $Sub { core: Some(Arc::downgrade(&self.core)), token }
        }

        /// Deliver `event` on the global channel now.
        ///
        /// Handlers run before this returns, in priority order. A handler
        /// that publishes immediately runs the nested dispatch inline; use
        /// [`publish_deferred`](Self::publish_deferred) from handlers to
        /// avoid re-entrancy.
        pub fn publish<T: Event>(&self, event: T) {
            self.publish_to(Channel::Global, event)
        }

        /// Deliver `event` on `channel` now.
        pub fn publish_to<T: Event>(&self, channel: Channel, event: T) {
            let to_dyn = ToDyn::of::<T>();
            self.core.dispatch(T::stable_type_id(), channel, || EventView::typed(channel, &event, &to_dyn));
        }

        /// Deliver a dynamic event on `channel` now.
        ///
        /// Fails unless a descriptor for `event.id` is registered and the
        /// fields match it.
        pub fn publish_dyn(&self, channel: Channel, event: &DynEvent) -> Result<(), DynEventError> {
            self.core.check_dyn(event)?;
            self.core.dispatch(event.id, channel, || EventView::dynamic(channel, event));
            Ok(())
        }

        /// Queue a dynamic event for the next [`flush`](Self::flush).
        pub fn publish_dyn_deferred(&self, channel: Channel, event: DynEvent) -> Result<(), DynEventError> {
            self.core.check_dyn(&event)?;
            self.core.enqueue(Queued { channel, payload: OwnedPayload::Dyn(event) });
            Ok(())
        }

        /// Deliver every queued event, oldest first, using the configured
        /// round limit ([`set_max_flush_rounds`](Self::set_max_flush_rounds)).
        ///
        /// Events queued by handlers during the flush are delivered in the
        /// same flush, in a later round. After the round limit the rest stay
        /// queued and [`FlushReport::hit_round_limit`] is set. Subscribers
        /// are resolved at delivery time, not at publish time.
        pub fn flush(&self) -> FlushReport {
            self.core.flush(self.core.max_rounds())
        }

        /// [`flush`](Self::flush) with an explicit round limit (at least 1).
        pub fn flush_with_limit(&self, max_rounds: u32) -> FlushReport {
            self.core.flush(max_rounds.max(1))
        }

        /// Set the default flush round limit (at least 1; default
        /// [`DEFAULT_MAX_FLUSH_ROUNDS`](crate::DEFAULT_MAX_FLUSH_ROUNDS)).
        pub fn set_max_flush_rounds(&self, rounds: u32) {
            self.core.set_max_rounds(rounds)
        }

        /// The default flush round limit.
        pub fn max_flush_rounds(&self) -> u32 {
            self.core.max_rounds()
        }

        /// Number of events waiting for a flush.
        pub fn queued_len(&self) -> usize {
            self.core.queued_len()
        }

        /// Number of subscribers of event `id` on `channel`.
        pub fn subscriber_count(&self, id: u64, channel: Channel) -> usize {
            self.core.subscriber_count(id, channel)
        }

        /// Remove every subscription on `channel` (for example when an entity
        /// is destroyed). Their handles become inactive. Returns how many
        /// were removed.
        pub fn clear_channel(&self, channel: Channel) -> usize {
            self.core.clear_channel(channel)
        }

        /// Register a descriptor. Registering an identical descriptor again
        /// is a no-op; a different descriptor with the same id or name is an
        /// error.
        pub fn register_descriptor(&self, descriptor: EventDescriptor) -> Result<u64, RegistryError> {
            self.core.register(descriptor)
        }

        /// Register the descriptor of a `#[pulsar_event(dynamic)]` type.
        pub fn register_event<T: Event>(&self) -> Result<u64, RegistryError> {
            self.core.register(T::descriptor().ok_or(RegistryError::NotReflected)?)
        }

        /// The descriptor registered for `id`.
        pub fn descriptor(&self, id: u64) -> Option<Arc<EventDescriptor>> {
            self.core.descriptor(id)
        }

        /// The descriptor registered under `name`.
        pub fn descriptor_by_name(&self, name: &str) -> Option<Arc<EventDescriptor>> {
            self.core.descriptor_by_name(name)
        }

        /// All registered descriptors, sorted by name.
        pub fn descriptors(&self) -> Vec<Arc<EventDescriptor>> {
            self.core.descriptors()
        }

        /// Hand this bus to a plugin: a `#[repr(C)]` function table holding
        /// one strong reference. Wrap it on the plugin side with
        /// [`ForeignBus::from_raw`](crate::ffi::ForeignBus::from_raw), which
        /// releases the reference when dropped.
        pub fn export_raw(&self) -> RawBus {
            ffi::export(Arc::clone(&self.core))
        }
    };
}

macro_rules! subscription {
    ($(#[$m:meta])* $Sub:ident, $CoreTy:ty) => {
        $(#[$m])*
        #[must_use = "dropping a subscription unsubscribes it; call .detach() to keep the handler for the bus's lifetime"]
        pub struct $Sub {
            core: Option<Weak<$CoreTy>>,
            token: u64,
        }

        impl $Sub {
            /// Remove the handler now. Safe to call from inside a handler
            /// (including the handler itself): it is skipped for the rest of
            /// the running dispatch and dropped when that dispatch ends.
            pub fn unsubscribe(mut self) {
                self.remove();
            }

            /// Keep the handler registered until the bus is dropped (or its
            /// channel is cleared), and discard the handle.
            pub fn detach(mut self) {
                self.core = None;
            }

            /// `true` while the handler is registered.
            pub fn is_active(&self) -> bool {
                self.core.as_ref().and_then(Weak::upgrade).is_some_and(|c| c.is_subscribed(self.token))
            }

            /// The bus-unique subscription token.
            pub fn token(&self) -> u64 {
                self.token
            }

            fn remove(&mut self) {
                if let Some(core) = self.core.take().and_then(|w| w.upgrade()) {
                    core.remove(self.token);
                }
            }
        }

        impl Drop for $Sub {
            fn drop(&mut self) {
                self.remove();
            }
        }

        impl std::fmt::Debug for $Sub {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($Sub)).field("token", &self.token).finish()
            }
        }
    };
}

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

/// A single-threaded event bus.
///
/// All methods take `&self`; handlers may subscribe, unsubscribe and publish
/// on the bus they are called from. Clones share the same bus. Handlers need
/// not be `Send`, so neither the bus nor its handles can leave the thread:
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<gamma_core::EventBus>();
/// ```
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<gamma_core::Subscription>();
/// ```
#[derive(Clone)]
pub struct EventBus {
    core: Arc<LocalCore>,
}

subscription!(
    /// Handle for an [`EventBus`] subscription. Unsubscribes on drop.
    Subscription,
    LocalCore
);

impl EventBus {
    /// An empty bus.
    pub fn new() -> Self {
        // The core is behind an `Arc` (not `Rc`) only so that the FFI layer
        // can use one refcounting scheme for both buses; this bus stays
        // `!Send`/`!Sync` because its handlers are.
        #[allow(clippy::arc_with_non_send_sync)]
        let core = Arc::new(LocalCore::new());
        Self { core }
    }

    /// A weak handle, for handlers that publish on their own bus without
    /// keeping it alive.
    pub fn downgrade(&self) -> WeakEventBus {
        WeakEventBus(Arc::downgrade(&self.core))
    }

    common_api!(Subscription, 'static);

    /// Queue `event` on the global channel for the next [`flush`](Self::flush).
    pub fn publish_deferred<T: Event>(&self, event: T) {
        self.publish_deferred_to(Channel::Global, event)
    }

    /// Queue `event` on `channel` for the next [`flush`](Self::flush).
    pub fn publish_deferred_to<T: Event>(&self, channel: Channel, event: T) {
        self.core.enqueue(Queued {
            channel,
            payload: OwnedPayload::Typed {
                id: T::stable_type_id(),
                value: OwnedTyped::new(event),
            },
        });
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// A weak reference to an [`EventBus`].
#[derive(Clone)]
pub struct WeakEventBus(Weak<LocalCore>);

impl WeakEventBus {
    /// The bus, if it still exists.
    pub fn upgrade(&self) -> Option<EventBus> {
        self.0.upgrade().map(|core| EventBus { core })
    }
}

// ---------------------------------------------------------------------------
// SyncEventBus
// ---------------------------------------------------------------------------

/// A thread-safe event bus: the same API as [`EventBus`], with `Send + Sync`
/// handlers. Share it with an `Arc` or by cloning (clones share the bus).
///
/// Dispatch never holds a lock while a handler runs, so handlers may use the
/// bus freely. [`unsubscribe`](SyncSubscription::unsubscribe) called from one
/// thread while another thread is dispatching the same event can still see
/// that one in-flight call finish; the handler is dropped after it does.
///
/// ```
/// fn assert_send_sync<T: Send + Sync>() {}
/// assert_send_sync::<gamma_core::SyncEventBus>();
/// assert_send_sync::<gamma_core::SyncSubscription>();
/// assert_send_sync::<gamma_core::ffi::ForeignBus>();
/// ```
#[derive(Clone)]
pub struct SyncEventBus {
    core: Arc<SyncCore>,
}

subscription!(
    /// Handle for a [`SyncEventBus`] subscription. Unsubscribes on drop.
    SyncSubscription,
    SyncCore
);

impl SyncEventBus {
    /// An empty bus.
    pub fn new() -> Self {
        Self {
            core: Arc::new(SyncCore::new()),
        }
    }

    /// A weak handle.
    pub fn downgrade(&self) -> WeakSyncEventBus {
        WeakSyncEventBus(Arc::downgrade(&self.core))
    }

    common_api!(SyncSubscription, Send + Sync + 'static);

    /// Queue `event` on the global channel for the next [`flush`](Self::flush).
    pub fn publish_deferred<T: Event + Send>(&self, event: T) {
        self.publish_deferred_to(Channel::Global, event)
    }

    /// Queue `event` on `channel` for the next [`flush`](Self::flush).
    pub fn publish_deferred_to<T: Event + Send>(&self, channel: Channel, event: T) {
        self.core.enqueue(Queued {
            channel,
            payload: OwnedPayload::Typed {
                id: T::stable_type_id(),
                value: OwnedTyped::new(event),
            },
        });
    }

    /// Deliver `event` on the global channel now, running handlers in
    /// parallel (rayon's pool with the `parallel` feature, scoped threads
    /// otherwise). Priorities are ignored: there is no ordering between
    /// handlers. Immediate only; deferred events always flush sequentially.
    pub fn parallel_publish<T: Event + Sync>(&self, event: T) {
        self.parallel_publish_to(Channel::Global, event)
    }

    /// [`parallel_publish`](Self::parallel_publish) on `channel`.
    pub fn parallel_publish_to<T: Event + Sync>(&self, channel: Channel, event: T) {
        let to_dyn = ToDyn::of::<T>();
        self.core
            .dispatch_parallel(T::stable_type_id(), channel, || {
                EventView::typed(channel, &event, &to_dyn)
            });
    }
}

impl Default for SyncEventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// A weak reference to a [`SyncEventBus`].
#[derive(Clone)]
pub struct WeakSyncEventBus(Weak<SyncCore>);

impl WeakSyncEventBus {
    /// The bus, if it still exists.
    pub fn upgrade(&self) -> Option<SyncEventBus> {
        self.0.upgrade().map(|core| SyncEventBus { core })
    }
}
