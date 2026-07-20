//! Core runtime for the **Gamma** event system.
//!
//! Provides the foundational traits and types for a type-safe,
//! plugin-compatible event bus designed to work across FFI boundaries
//! (e.g., between a host and dynamically loaded libraries).
//!
//! For per‑instance events (each actor owns its own [`EventBus`]), see the
//! [case study](https://github.com/Far-Beyond-Pulsar/gamma#readme).
//!
//! # DLL / Plugin Safety
//!
//! Every event type **must**:
//!
//! 1. Use `#[pulsar_event]` (which adds `#[repr(C)]` **and** implements
//!    [`Event`] with a deterministic, layout‑sensitive type identifier).
//! 2. Share the same global allocator between host and plugin (see
//!    [mimalloc](https://crates.io/crates/mimalloc) or
//!    [jemalloc](https://crates.io/crates/jemalloc) linked globally).
//!
//! # Example
//!
//! ```rust
//! # use gamma_derive::pulsar_event;
//! use gamma_core::EventBus;
//!
//! #[pulsar_event]
//! struct PlayerJumped {
//!     height: f32,
//!     timestamp: u64,
//! }
//!
//! let mut bus = EventBus::new();
//!
//! bus.subscribe(|e: &PlayerJumped| {
//!     println!("Jumped {} at {}", e.height, e.timestamp);
//! });
//!
//! bus.publish(PlayerJumped { height: 5.0, timestamp: 12345 });
//! ```

use std::any::Any;
use std::sync::RwLock;

use rustc_hash::FxHashMap;

// ---------------------------------------------------------------------------
// Event trait
// ---------------------------------------------------------------------------

/// Trait for events that can travel across plugin boundaries.
///
/// Every implementor **must** also apply `#[repr(C)]` to guarantee a stable
/// memory layout across different compilation units.  The [`stable_type_id`]
/// method returns a deterministic hash that incorporates the type name,
/// its size, and its alignment so that structurally different types always
/// produce different IDs.
///
/// [`stable_type_id`]: Event::stable_type_id
///
/// # Usage
///
/// Prefer the [`#[pulsar_event]`](https://docs.rs/gamma-derive/latest/gamma_derive/attr.pulsar_event.html)
/// attribute macro — it applies `#[repr(C)]` automatically.
pub trait Event: 'static {
    /// Deterministic 64-bit identifier that uniquely distinguishes this event
    /// type from every other event type in the system.
    ///
    /// The value is computed from:
    /// - The fully-qualified type name
    /// - [`size_of`](std::mem::size_of) the type
    /// - [`align_of`](std::mem::align_of) the type
    ///
    /// Because size and alignment are baked into the hash, two types with the
    /// same name but different layouts (e.g., one compiled with `#[repr(C)]`
    /// and one without) will **not** collide.
    fn stable_type_id() -> u64;
}

// ---------------------------------------------------------------------------
// EventHandler
// ---------------------------------------------------------------------------

/// Internal trait erased inside [`EventBus`] for dynamic dispatch.
///
/// You should never need to implement this trait directly — use
/// [`EventBus::subscribe`] with a closure.
pub trait EventHandler {
    /// Dispatch an event if it matches the concrete type handled by this
    /// subscriber.
    fn handle(&self, event: &dyn Any);
}

struct CallbackWrapper<T, F> {
    callback: F,
    _marker: std::marker::PhantomData<T>,
}

impl<T: 'static, F: Fn(&T)> EventHandler for CallbackWrapper<T, F> {
    fn handle(&self, event: &dyn Any) {
        if let Some(e) = event.downcast_ref::<T>() {
            (self.callback)(e);
        }
    }
}

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

/// A type-safe, plugin-compatible event dispatcher.
///
/// Events are routed to subscribers by the stable 64-bit ID returned by
/// [`Event::stable_type_id`].  This design ensures that a plugin compiled
/// separately from the host can publish events that the host (or other
/// plugins) have subscribed to, provided all event types use `#[repr(C)]`
/// and both sides share a global allocator.
pub struct EventBus {
    subscribers: FxHashMap<u64, Vec<Box<dyn EventHandler>>>,
}

impl EventBus {
    /// Create an empty event bus.
    pub fn new() -> Self {
        Self {
            subscribers: FxHashMap::default(),
        }
    }

    /// Register a callback for events of type `T`.
    ///
    /// The callback will be invoked every time [`publish`] is called with
    /// an event of the same type.
    ///
    /// [`publish`]: EventBus::publish
    ///
    /// # Example
    ///
    /// ```rust
    /// # use gamma_derive::pulsar_event;
    /// use gamma_core::EventBus;
    ///
    /// #[pulsar_event]
    /// struct MyEvent(i32);
    ///
    /// let mut bus = EventBus::new();
    /// bus.subscribe(|e: &MyEvent| println!("got {}", e.0));
    /// ```
    pub fn subscribe<T: Event, F>(&mut self, callback: F)
    where
        F: Fn(&T) + 'static,
    {
        self.subscribers
            .entry(T::stable_type_id())
            .or_default()
            .push(Box::new(CallbackWrapper {
                callback,
                _marker: std::marker::PhantomData,
            }));
    }

    /// Publish an event to all registered subscribers.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use gamma_derive::pulsar_event;
    /// use gamma_core::EventBus;
    ///
    /// #[pulsar_event]
    /// struct MyEvent(i32);
    ///
    /// let bus = EventBus::new();
    /// bus.publish(MyEvent(42));
    /// ```
    pub fn publish<T: Event>(&self, event: T) {
        if let Some(listeners) = self.subscribers.get(&T::stable_type_id()) {
            for listener in listeners {
                listener.handle(&event);
            }
        }
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// SyncEventBus — thread-safe variant
// ---------------------------------------------------------------------------

/// A thread-safe event bus that can be shared across threads via [`Arc`].
///
/// `SyncEventBus` wraps all internal state in a [`RwLock`] so that both
/// [`subscribe`] and [`publish`] take only `&self`.  Subscribers must be
/// `Send + Sync`.
///
/// The [`parallel_publish`] method dispatches to all subscribers using
/// scoped threads, enabling parallel processing of expensive handlers.
///
/// [`subscribe`]: SyncEventBus::subscribe
/// [`publish`]: SyncEventBus::publish
/// [`parallel_publish`]: SyncEventBus::parallel_publish
/// [`Arc`]: std::sync::Arc
pub struct SyncEventBus {
    subscribers: RwLock<FxHashMap<u64, Vec<Box<dyn EventHandler + Send + Sync>>>>,
}

impl SyncEventBus {
    /// Create an empty, thread-safe event bus.
    pub fn new() -> Self {
        Self {
            subscribers: RwLock::new(FxHashMap::default()),
        }
    }

    /// Register a callback for events of type `T`.
    ///
    /// The closure must be `Send + Sync` so the bus can be shared across
    /// threads safely.
    pub fn subscribe<T: Event + Send + Sync, F>(&self, callback: F)
    where
        F: Fn(&T) + Send + Sync + 'static,
    {
        self.subscribers
            .write()
            .unwrap()
            .entry(T::stable_type_id())
            .or_default()
            .push(Box::new(CallbackWrapper {
                callback,
                _marker: std::marker::PhantomData,
            }));
    }

    /// Publish an event to all registered subscribers (sequential).
    ///
    /// Acquires a read lock and invokes each subscriber in order.
    pub fn publish<T: Event>(&self, event: T) {
        let id = T::stable_type_id();
        if let Some(listeners) = self.subscribers.read().unwrap().get(&id) {
            for listener in listeners {
                listener.handle(&event);
            }
        }
    }

    /// Publish an event and dispatch to subscribers **in parallel**.
    ///
    /// When the `parallel` feature is enabled (requires `rayon`), dispatch
    /// uses a global work‑stealing thread pool — threads stay warm between
    /// calls, making this viable for handlers as cheap as ~1 µs.
    /// Without the feature, falls back to `std::thread::scope`.
    ///
    /// The event type must be [`Sync`] because `&event` is shared across
    /// threads.
    ///
    /// # Panics
    ///
    /// Panics if any spawned thread panics.
    pub fn parallel_publish<T: Event + Sync>(&self, event: T) {
        let id = T::stable_type_id();
        if let Some(listeners) = self.subscribers.read().unwrap().get(&id) {
            #[cfg(feature = "parallel")]
            {
                use rayon::prelude::*;
                listeners.par_iter().for_each(|listener| {
                    listener.handle(&event);
                });
            }
            #[cfg(not(feature = "parallel"))]
            {
                std::thread::scope(|s| {
                    for listener in listeners {
                        s.spawn(|| listener.handle(&event));
                    }
                });
            }
        }
    }
}

impl Default for SyncEventBus {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Fixtures: event types exercising various repr(C) layouts
    // -----------------------------------------------------------------------

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
    struct PrimitiveEvent {
        a: u8,
        b: u16,
        c: u32,
        d: u64,
        e: i8,
        f: i16,
        g: i32,
        h: i64,
        i: f32,
        j: f64,
    }

    impl Event for PrimitiveEvent {
        fn stable_type_id() -> u64 {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in stringify!(PrimitiveEvent).as_bytes() {
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
    struct BoolCharEvent {
        flag: bool,
        ch: char,
    }

    impl Event for BoolCharEvent {
        fn stable_type_id() -> u64 {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in stringify!(BoolCharEvent).as_bytes() {
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
    struct ArrayEvent {
        values: [u64; 8],
    }

    impl Event for ArrayEvent {
        fn stable_type_id() -> u64 {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in stringify!(ArrayEvent).as_bytes() {
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
    struct NestedInner {
        x: f32,
        y: f32,
    }

    impl Event for NestedInner {
        fn stable_type_id() -> u64 {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in stringify!(NestedInner).as_bytes() {
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
    struct NestedEvent {
        id: u32,
        inner: NestedInner,
    }

    impl Event for NestedEvent {
        fn stable_type_id() -> u64 {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in stringify!(NestedEvent).as_bytes() {
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
    struct TupleEvent(u32, f64, bool);

    impl Event for TupleEvent {
        fn stable_type_id() -> u64 {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in stringify!(TupleEvent).as_bytes() {
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

    /// Helper: run `f` inside a scope so `Rc` lifetimes don't leak.
    fn cell<T: Copy + 'static>(
        val: T,
    ) -> (std::rc::Rc<std::cell::Cell<T>>, std::rc::Rc<std::cell::Cell<T>>) {
        let rc = std::rc::Rc::new(std::cell::Cell::new(val));
        (std::rc::Rc::clone(&rc), rc)
    }

    // -----------------------------------------------------------------------
    // Basic correctness
    // -----------------------------------------------------------------------

    #[test]
    fn subscribe_receives_published_event() {
        let mut bus = EventBus::new();
        let (sent, received) = cell(0u32);
        bus.subscribe(move |e: &PrimitiveEvent| sent.set(e.c));
        bus.publish(PrimitiveEvent {
            a: 1,
            b: 2,
            c: 42,
            d: 4,
            e: 5,
            f: 6,
            g: 7,
            h: 8,
            i: 9.0,
            j: 10.0,
        });
        assert_eq!(received.get(), 42);
    }

    #[test]
    fn publish_with_no_subscribers_is_noop() {
        let bus = EventBus::new();
        bus.publish(EmptyEvent);
        bus.publish(PrimitiveEvent {
            a: 0,
            b: 0,
            c: 0,
            d: 0,
            e: 0,
            f: 0,
            g: 0,
            h: 0,
            i: 0.0,
            j: 0.0,
        });
    }

    #[test]
    fn event_types_are_independent() {
        let mut bus = EventBus::new();
        let (flag_a, received_a) = cell(false);
        let (flag_b, received_b) = cell(false);

        bus.subscribe(move |_: &EmptyEvent| flag_a.set(true));
        bus.subscribe(move |_: &BoolCharEvent| flag_b.set(true));

        bus.publish(EmptyEvent);
        assert!(received_a.get());
        assert!(!received_b.get());

        bus.publish(BoolCharEvent {
            flag: true,
            ch: 'Z',
        });
        assert!(received_b.get());
    }

    #[test]
    fn multiple_subscribers_same_type_all_see_event() {
        let mut bus = EventBus::new();
        let (a_sent, a_recv) = cell(0i32);
        let (b_sent, b_recv) = cell(0i32);
        let (c_sent, c_recv) = cell(0i32);

        bus.subscribe(move |e: &TupleEvent| a_sent.set(e.0 as i32));
        bus.subscribe(move |e: &TupleEvent| b_sent.set((e.0 * 2) as i32));
        bus.subscribe(move |e: &TupleEvent| c_sent.set((e.0 * 3) as i32));

        bus.publish(TupleEvent(7, 3.14, true));

        assert_eq!(a_recv.get(), 7);
        assert_eq!(b_recv.get(), 14);
        assert_eq!(c_recv.get(), 21);
    }

    #[test]
    fn subscribers_invoked_in_registration_order() {
        let mut bus = EventBus::new();
        let log = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));

        for i in 0..5 {
            let log = std::rc::Rc::clone(&log);
            bus.subscribe(move |_: &EmptyEvent| log.borrow_mut().push(i));
        }

        bus.publish(EmptyEvent);

        let expected: Vec<i32> = (0..5).collect();
        assert_eq!(*log.borrow(), expected);
    }

    // -----------------------------------------------------------------------
    // Complex data types
    // -----------------------------------------------------------------------

    #[test]
    fn bool_and_char_fields() {
        let mut bus = EventBus::new();
        let (sent_f, recv_f) = cell(false);
        let (sent_c, recv_c) = cell('\0');

        bus.subscribe(move |e: &BoolCharEvent| {
            sent_f.set(e.flag);
            sent_c.set(e.ch);
        });

        bus.publish(BoolCharEvent {
            flag: true,
            ch: '🚀',
        });

        assert!(recv_f.get());
        assert_eq!(recv_c.get(), '🚀');
    }

    #[test]
    fn array_fields() {
        let mut bus = EventBus::new();
        let buf = std::rc::Rc::new(std::cell::RefCell::new([0u64; 8]));

        {
            let buf = std::rc::Rc::clone(&buf);
            bus.subscribe(move |e: &ArrayEvent| {
                *buf.borrow_mut() = e.values;
            });
        }

        let payload = [10, 20, 30, 40, 50, 60, 70, 80];
        bus.publish(ArrayEvent { values: payload });

        assert_eq!(*buf.borrow(), payload);
    }

    #[test]
    fn nested_struct_fields() {
        let mut bus = EventBus::new();
        let (id_sent, id_recv) = cell(0u32);
        let (x_sent, x_recv) = cell(0.0f32);
        let (y_sent, y_recv) = cell(0.0f32);

        bus.subscribe(move |e: &NestedEvent| {
            id_sent.set(e.id);
            x_sent.set(e.inner.x);
            y_sent.set(e.inner.y);
        });

        bus.publish(NestedEvent {
            id: 999,
            inner: NestedInner {
                x: 1.5,
                y: -3.25,
            },
        });

        assert_eq!(id_recv.get(), 999);
        assert!((x_recv.get() - 1.5).abs() < f32::EPSILON);
        assert!((y_recv.get() + 3.25).abs() < f32::EPSILON);
    }

    #[test]
    fn tuple_struct_fields() {
        let mut bus = EventBus::new();
        let (a_sent, a_recv) = cell(0u32);
        let (b_sent, b_recv) = cell(0.0f64);
        let (c_sent, c_recv) = cell(false);

        bus.subscribe(move |e: &TupleEvent| {
            a_sent.set(e.0);
            b_sent.set(e.1);
            c_sent.set(e.2);
        });

        bus.publish(TupleEvent(42, 2.71828, true));

        assert_eq!(a_recv.get(), 42);
        assert!((b_recv.get() - 2.71828).abs() < f64::EPSILON);
        assert!(c_recv.get());
    }

    #[test]
    fn unit_struct() {
        let mut bus = EventBus::new();
        let called = std::rc::Rc::new(std::cell::Cell::new(false));
        let cb = std::rc::Rc::clone(&called);

        bus.subscribe(move |_: &EmptyEvent| cb.set(true));
        bus.publish(EmptyEvent);

        assert!(called.get());
    }

    #[test]
    fn large_event_copy() {
        let mut bus = EventBus::new();
        let buf = std::rc::Rc::new(std::cell::RefCell::new([0u8; 1024]));

        {
            let buf = std::rc::Rc::clone(&buf);
            bus.subscribe(move |e: &LargeEvent| {
                *buf.borrow_mut() = e.buffer;
            });
        }

        let mut payload = [0u8; 1024];
        for i in 0..1024 {
            payload[i] = (i % 256) as u8;
        }
        let cksum: u64 = payload.iter().map(|&b| b as u64).sum();

        bus.publish(LargeEvent {
            buffer: payload,
            checksum: cksum,
        });

        assert_eq!(*buf.borrow(), payload);
    }

    // -----------------------------------------------------------------------
    // Multiple publishes and accumulation
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_publishes_accumulate() {
        let mut bus = EventBus::new();
        let log = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));

        {
            let log = std::rc::Rc::clone(&log);
            bus.subscribe(move |e: &TupleEvent| {
                log.borrow_mut().push((e.0, e.1));
            });
        }

        bus.publish(TupleEvent(1, 1.1, false));
        bus.publish(TupleEvent(2, 2.2, true));
        bus.publish(TupleEvent(3, 3.3, false));

        assert_eq!(log.borrow().len(), 3);
        assert_eq!(log.borrow()[0], (1, 1.1));
        assert_eq!(log.borrow()[1], (2, 2.2));
        assert_eq!(log.borrow()[2], (3, 3.3));
    }

    #[test]
    fn publish_then_subscribe_receives_nothing_past() {
        let mut bus = EventBus::new();
        let called = std::rc::Rc::new(std::cell::Cell::new(false));

        bus.publish(EmptyEvent);

        let cb = std::rc::Rc::clone(&called);
        bus.subscribe(move |_: &EmptyEvent| cb.set(true));

        assert!(!called.get());
    }

    #[test]
    fn interleaved_event_types() {
        let mut bus = EventBus::new();
        let int_log = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let float_log = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));

        {
            let log = std::rc::Rc::clone(&int_log);
            bus.subscribe(move |e: &PrimitiveEvent| log.borrow_mut().push(e.c));
        }
        {
            let log = std::rc::Rc::clone(&float_log);
            bus.subscribe(move |e: &BoolCharEvent| log.borrow_mut().push(e.ch));
        }

        bus.publish(PrimitiveEvent {
            a: 0, b: 0, c: 100, d: 0, e: 0, f: 0, g: 0, h: 0, i: 0.0, j: 0.0,
        });
        bus.publish(BoolCharEvent { flag: true, ch: 'A' });
        bus.publish(PrimitiveEvent {
            a: 0, b: 0, c: 200, d: 0, e: 0, f: 0, g: 0, h: 0, i: 0.0, j: 0.0,
        });
        bus.publish(BoolCharEvent { flag: false, ch: 'B' });

        assert_eq!(*int_log.borrow(), vec![100, 200]);
        assert_eq!(*float_log.borrow(), vec!['A', 'B']);
    }

    // -----------------------------------------------------------------------
    // Type ID stability and uniqueness
    // -----------------------------------------------------------------------

    #[test]
    fn stable_type_id_is_deterministic() {
        assert_eq!(
            EmptyEvent::stable_type_id(),
            EmptyEvent::stable_type_id()
        );
        assert_eq!(
            PrimitiveEvent::stable_type_id(),
            PrimitiveEvent::stable_type_id()
        );
    }

    #[test]
    fn all_type_ids_are_unique() {
        let ids = vec![
            EmptyEvent::stable_type_id(),
            PrimitiveEvent::stable_type_id(),
            BoolCharEvent::stable_type_id(),
            ArrayEvent::stable_type_id(),
            NestedInner::stable_type_id(),
            NestedEvent::stable_type_id(),
            TupleEvent::stable_type_id(),
            LargeEvent::stable_type_id(),
        ];

        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();

        assert_eq!(
            ids.len(),
            sorted.len(),
            "duplicate stable_type_id detected among event types"
        );
    }

    #[test]
    fn same_layout_different_names_produce_different_ids() {
        #[repr(C)]
        struct Pos2d {
            x: f32,
            y: f32,
        }

        impl Event for Pos2d {
            fn stable_type_id() -> u64 {
                let mut hash: u64 = 0xcbf29ce484222325;
                for byte in stringify!(Pos2d).as_bytes() {
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

        // NestedInner already exists with the exact same layout (two f32s)
        // but a different name — they must NOT collide.
        assert_ne!(NestedInner::stable_type_id(), Pos2d::stable_type_id());
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn zero_sized_event() {
        let mut bus = EventBus::new();
        let called = std::rc::Rc::new(std::cell::Cell::new(false));

        let cb = std::rc::Rc::clone(&called);
        bus.subscribe(move |_: &EmptyEvent| cb.set(true));

        bus.publish(EmptyEvent);
        assert!(called.get());
    }

    #[test]
    fn event_with_max_values() {
        let mut bus = EventBus::new();
        let (sent, recv) = cell(0u64);

        bus.subscribe(move |e: &PrimitiveEvent| sent.set(e.d));

        bus.publish(PrimitiveEvent {
            a: u8::MAX,
            b: u16::MAX,
            c: u32::MAX,
            d: u64::MAX,
            e: i8::MIN,
            f: i16::MIN,
            g: i32::MIN,
            h: i64::MIN,
            i: f32::MAX,
            j: f64::MAX,
        });

        assert_eq!(recv.get(), u64::MAX);
    }

    #[test]
    fn bus_can_be_shared_across_many_publishes() {
        let mut bus = EventBus::new();
        let count = std::rc::Rc::new(std::cell::Cell::new(0u32));

        {
            let c = std::rc::Rc::clone(&count);
            bus.subscribe(move |_: &EmptyEvent| c.set(c.get() + 1));
        }

        for _ in 0..1000 {
            bus.publish(EmptyEvent);
        }

        assert_eq!(count.get(), 1000);
    }

    #[test]
    fn bus_default_is_empty() {
        let bus: EventBus = Default::default();
        bus.publish(EmptyEvent);
    }

}
