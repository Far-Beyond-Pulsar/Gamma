//! Machinery shared by [`EventBus`](crate::EventBus) and
//! [`SyncEventBus`](crate::SyncEventBus).
//!
//! The two buses differ only in the handler type they store
//! (`dyn ErasedHandler` versus `dyn ErasedHandler + Send + Sync`), so the
//! core is generic over that.
//!
//! # Re-entrancy
//!
//! Per `(event id, channel)` the core keeps an `Arc<Vec<Arc<Entry>>>` sorted
//! by (priority descending, subscription order). A dispatch clones the outer
//! `Arc` under a short read lock and iterates **without holding any lock**,
//! so handlers may subscribe, unsubscribe or publish. Mutations use
//! `Arc::make_mut`: in place when no dispatch is running, copy-on-write when
//! one is. An unsubscribed entry is marked dead (so the running dispatch
//! skips it) and is dropped only when the last dispatch holding it ends.

use std::alloc::Layout;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use rustc_hash::FxHashMap;

use crate::ffi::{ForeignHandler, RawDropFn, RawSink, RawToDynFn};
use crate::dynamic::SinkSpares;
use crate::{
    Channel, DynEvent, DynEventError, DynEventSink, DynValue, Event, EventDescriptor, RegistryError,
};

/// Pool cap defaults: 64 recycled typed buffers, 256 dynamic field buffers
/// per shape (`Pools::buffers_cap` / `dyn_cap`).
pub(crate) const DEFAULT_POOL_BUFFERS_CAP: usize = 64;
pub(crate) const DEFAULT_POOL_DYN_CAP: usize = 256;

pub(crate) type Key = (u64, Channel);

// ---------------------------------------------------------------------------
// Flavour-specific interior mutability
//
// `EventBus` uses `RefCell` and `Rc` (no atomics on the publish path);
// `SyncEventBus` uses `RwLock` and `Arc`. Handlers never run inside these
// closures, so the `RefCell` is never re-borrowed and a poisoned `RwLock`
// only means a panic in Gamma itself (the data is recovered).
// ---------------------------------------------------------------------------

pub(crate) trait Lock<T> {
    fn new(value: T) -> Self;
    fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R;
    fn write<R>(&self, f: impl FnOnce(&mut T) -> R) -> R;
}

impl<T> Lock<T> for RefCell<T> {
    fn new(value: T) -> Self {
        RefCell::new(value)
    }
    fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&self.borrow())
    }
    fn write<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(&mut self.borrow_mut())
    }
}

impl<T> Lock<T> for RwLock<T> {
    fn new(value: T) -> Self {
        RwLock::new(value)
    }
    fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&self.read().unwrap_or_else(|e| e.into_inner()))
    }
    fn write<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(&mut self.write().unwrap_or_else(|e| e.into_inner()))
    }
}

impl<T> Lock<T> for Mutex<T> {
    fn new(value: T) -> Self {
        Mutex::new(value)
    }
    fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&self.lock().unwrap_or_else(|e| e.into_inner()))
    }
    fn write<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(&mut self.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

/// A shared, copy-on-write subscriber list (`Rc` or `Arc`).
pub(crate) trait SharedList<E>: Clone + Default + Deref<Target = Vec<E>> {
    fn make_mut(&mut self) -> &mut Vec<E>;
}

impl<E: Clone> SharedList<E> for Rc<Vec<E>> {
    fn make_mut(&mut self) -> &mut Vec<E> {
        Rc::make_mut(self)
    }
}

impl<E: Clone> SharedList<E> for Arc<Vec<E>> {
    fn make_mut(&mut self) -> &mut Vec<E> {
        Arc::make_mut(self)
    }
}

// ---------------------------------------------------------------------------
// Event views and conversions
// ---------------------------------------------------------------------------

/// How to turn a typed payload into a [`DynEvent`].
pub(crate) enum ToDyn {
    None,
    /// Writes this copy of gamma-core's event into a [`DynEventSink`] via
    /// `Event::to_dyn_into` (equivalent to `T::to_dyn`, pooled-capable).
    Local(unsafe fn(*const u8, &mut DynEventSink) -> bool),
    /// A plugin's encoder (writes the wire format into a sink).
    Foreign(RawToDynFn),
}

impl ToDyn {
    pub(crate) fn of<T: Event>() -> Self {
        if T::REFLECTED {
            ToDyn::Local(to_dyn_thunk::<T>)
        } else {
            ToDyn::None
        }
    }

    /// The dynamic form with fresh buffers (no pool in reach).
    ///
    /// # Safety
    /// `ptr` must point to a live value of the type this converter was made for.
    pub(crate) fn convert(&self, ptr: *const u8) -> Option<DynEvent> {
        match self {
            ToDyn::None => None,
            // SAFETY: forwarded from the caller.
            ToDyn::Local(f) => unsafe {
                let mut sink = DynEventSink::empty();
                let ok = f(ptr, &mut sink);
                let (event, spares) = sink.finish();
                drop(spares);
                if ok {
                    event
                } else {
                    None
                }
            },
            ToDyn::Foreign(f) => {
                let mut buf: Vec<u8> = Vec::new();
                let mut sink = RawSink {
                    ctx: (&mut buf as *mut Vec<u8>).cast::<c_void>(),
                    write: sink_write,
                };
                // SAFETY: `ptr` is valid (caller); `sink` outlives the call.
                unsafe { f(ptr, &mut sink) };
                DynEvent::decode(&buf).ok()
            }
        }
    }
}

unsafe fn to_dyn_thunk<T: Event>(ptr: *const u8, sink: &mut DynEventSink) -> bool {
    // SAFETY: only built by `ToDyn::of::<T>` and called with a `*const T`.
    unsafe { &*ptr.cast::<T>() }.to_dyn_into(sink)
}

/// The sink a plugin's `to_dyn` writes into. The bytes are copied into a
/// host-owned `Vec`, so no allocation crosses the boundary.
unsafe extern "C" fn sink_write(ctx: *mut c_void, data: *const u8, len: usize) {
    if len == 0 {
        return;
    }
    // SAFETY: `ctx` is the `Vec<u8>` set up in `ToDyn::convert`; the plugin
    // passes `len` readable bytes at `data`.
    unsafe {
        let buf = &mut *ctx.cast::<Vec<u8>>();
        buf.extend_from_slice(std::slice::from_raw_parts(data, len));
    }
}

pub(crate) enum Payload<'a> {
    /// Borrowed typed value: pointer plus the publisher's layout.
    Typed {
        ptr: *const u8,
        size: usize,
        align: usize,
        to_dyn: &'a ToDyn,
    },
    Dyn(&'a DynEvent),
}

/// One event being dispatched, with lazily computed dynamic forms (computed
/// at most once per dispatch, whatever the number of dynamic subscribers).
pub(crate) struct EventView<'a> {
    pub id: u64,
    pub channel: Channel,
    pub payload: Payload<'a>,
    dyn_cache: OnceLock<Option<DynEvent>>,
    /// The pooled dynamic form, built by the flush before dispatch (see
    /// `build_pooled_dyn`). Shared so the flush can recycle its buffers
    /// after delivery; it takes precedence over `dyn_cache`.
    pub(crate) dyn_form: OnceLock<Arc<DynEvent>>,
    enc_cache: OnceLock<Option<Vec<u8>>>,
}

// SAFETY: the only non-Sync part is the typed pointer. Views are shared
// across threads only by `SyncEventBus::parallel_publish`, which requires
// `T: Sync`; everywhere else they stay on the publishing thread.
unsafe impl Sync for EventView<'_> {}

impl<'a> EventView<'a> {
    pub(crate) fn typed<T: Event>(channel: Channel, value: &'a T, to_dyn: &'a ToDyn) -> Self {
        Self::raw_typed(
            T::stable_type_id(),
            channel,
            (value as *const T).cast(),
            size_of::<T>(),
            align_of::<T>(),
            to_dyn,
        )
    }

    pub(crate) fn raw_typed(
        id: u64,
        channel: Channel,
        ptr: *const u8,
        size: usize,
        align: usize,
        to_dyn: &'a ToDyn,
    ) -> Self {
        Self {
            id,
            channel,
            payload: Payload::Typed {
                ptr,
                size,
                align,
                to_dyn,
            },
            dyn_cache: OnceLock::new(),
            dyn_form: OnceLock::new(),
            enc_cache: OnceLock::new(),
        }
    }

    pub(crate) fn dynamic(channel: Channel, ev: &'a DynEvent) -> Self {
        Self {
            id: ev.id,
            channel,
            payload: Payload::Dyn(ev),
            dyn_cache: OnceLock::new(),
            dyn_form: OnceLock::new(),
            enc_cache: OnceLock::new(),
        }
    }

    /// The event as a [`DynEvent`], if it is dynamic or its type is reflected.
    pub(crate) fn as_dyn(&self) -> Option<&DynEvent> {
        // The flush built the pooled form; deliver that.
        if let Payload::Typed { .. } = &self.payload {
            if let Some(f) = self.dyn_form.get() {
                return Some(f);
            }
        }
        match &self.payload {
            Payload::Dyn(d) => Some(d),
            // SAFETY: `ptr` is the live value this view was built from.
            Payload::Typed { ptr, to_dyn, .. } => self
                .dyn_cache
                .get_or_init(|| to_dyn.convert(*ptr))
                .as_ref(),
        }
    }

    /// The event in the wire encoding (for foreign dynamic handlers).
    pub(crate) fn encoded(&self) -> Option<&[u8]> {
        self.enc_cache
            .get_or_init(|| self.as_dyn().map(DynEvent::to_bytes))
            .as_deref()
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// A subscriber, type-erased. Implementations live in the library that
/// created them (see `ForeignHandler` for plugin subscribers).
pub(crate) trait ErasedHandler {
    fn deliver(&self, ev: &EventView<'_>);
    /// `true` when this handler invokes its callback with the *dynamic*
    /// form (so the flush can build it from pooled buffers up front).
    fn is_dyn(&self) -> bool {
        false
    }
}

/// The handler object type stored by one bus flavour, and the
/// synchronisation that goes with it.
pub(crate) trait Slot: ErasedHandler + 'static {
    const THREAD_SAFE: bool;
    /// Read-mostly state (subscriber table, registry).
    type Lock<T>: Lock<T>;
    /// The deferred queue (only needs `Send` contents).
    type Mutex<T>: Lock<T>;
    type List: SharedList<Arc<Entry<Self>>>;
    fn foreign(token: u64, priority: i32, handler: ForeignHandler) -> Arc<Entry<Self>>;
}

impl Slot for dyn ErasedHandler {
    const THREAD_SAFE: bool = false;
    type Lock<T> = RefCell<T>;
    type Mutex<T> = RefCell<T>;
    type List = Rc<Vec<Arc<Entry<Self>>>>;
    fn foreign(token: u64, priority: i32, handler: ForeignHandler) -> Arc<Entry<Self>> {
        entry(token, priority, handler)
    }
}

impl Slot for dyn ErasedHandler + Send + Sync {
    const THREAD_SAFE: bool = true;
    type Lock<T> = RwLock<T>;
    type Mutex<T> = Mutex<T>;
    type List = Arc<Vec<Arc<Entry<Self>>>>;
    fn foreign(token: u64, priority: i32, handler: ForeignHandler) -> Arc<Entry<Self>> {
        entry(token, priority, handler)
    }
}

pub(crate) struct Entry<H: ?Sized> {
    token: u64,
    priority: i32,
    alive: AtomicBool,
    handler: H,
}

pub(crate) fn entry<X>(token: u64, priority: i32, handler: X) -> Arc<Entry<X>> {
    Arc::new(Entry {
        token,
        priority,
        alive: AtomicBool::new(true),
        handler,
    })
}

/// A typed Rust subscriber.
pub(crate) struct TypedHandler<T, F> {
    f: F,
    _t: PhantomData<fn(&T)>,
}

impl<T, F> TypedHandler<T, F> {
    pub(crate) fn new(f: F) -> Self {
        Self { f, _t: PhantomData }
    }
}

impl<T: Event, F: Fn(&T)> ErasedHandler for TypedHandler<T, F> {
    fn deliver(&self, ev: &EventView<'_>) {
        match &ev.payload {
            Payload::Typed {
                ptr, size, align, ..
            } => {
                // Layout check: the id already hashes size and alignment, but
                // the publisher may be another library, so check again.
                if *size == size_of::<T>() && *align == align_of::<T>() {
                    // SAFETY: the dispatch key guarantees the publisher's
                    // stable id equals `T::stable_type_id()` (same name, size,
                    // alignment and, for dynamic events, field schema), the
                    // layout matches, and `#[pulsar_event]` types are
                    // `#[repr(C)]`. `ptr` is live for the whole dispatch.
                    let value = unsafe { &*ptr.cast::<T>() };
                    (self.f)(value);
                }
            }
            Payload::Dyn(d) => {
                if T::REFLECTED {
                    if let Some(value) = T::from_dyn(d) {
                        (self.f)(&value);
                    }
                }
            }
        }
    }
}

/// A dynamic Rust subscriber.
pub(crate) struct DynHandler<F>(pub(crate) F);

impl<F: Fn(&DynEvent)> ErasedHandler for DynHandler<F> {
    fn deliver(&self, ev: &EventView<'_>) {
        if let Some(d) = ev.as_dyn() {
            (self.0)(d);
        }
    }

    fn is_dyn(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Queued (deferred) events
// ---------------------------------------------------------------------------

enum DropFn {
    None,
    Local(unsafe fn(*mut u8)),
    Foreign(RawDropFn),
}

/// An owned typed event in a host-allocated buffer. Its *contents* are
/// dropped by the publisher's drop function (so a plugin's heap data is
/// freed by the plugin); the buffer itself is the host's.
pub(crate) struct OwnedTyped {
    ptr: NonNull<u8>,
    layout: Layout,
    drop_fn: DropFn,
    to_dyn: ToDyn,
}

// SAFETY: `SyncEventBus::publish_deferred` requires `T: Send`; foreign
// publishers promise the same for thread-safe buses (documented in `ffi`).
unsafe impl Send for OwnedTyped {}

unsafe fn drop_thunk<T>(p: *mut u8) {
    // SAFETY: called once on the buffer the pooled constructor wrote.
    unsafe { std::ptr::drop_in_place(p.cast::<T>()) }
}

impl OwnedTyped {
    fn alloc(layout: Layout) -> NonNull<u8> {
        if layout.size() == 0 {
            // Dangling but aligned, never dereferenced for more than 0 bytes.
            return NonNull::new(std::ptr::without_provenance_mut(layout.align()))
                .expect("align is non-zero");
        }
        // SAFETY: size is non-zero.
        let p = unsafe { std::alloc::alloc(layout) };
        NonNull::new(p).unwrap_or_else(|| std::alloc::handle_alloc_error(layout))
    }

    /// Take the value with a pooled buffer instead of a fresh allocation.
    ///
    /// # Safety
    /// `buffer` must be a live allocation of exactly `T`'s layout whose
    /// contents were already dropped, or `None`.
    pub(crate) unsafe fn with_buffer<T: Event>(value: T, buffer: Option<NonNull<u8>>) -> Self {
        let layout = Layout::new::<T>();
        let ptr = match buffer {
            Some(p) if layout.size() != 0 => p,
            _ => Self::alloc(layout),
        };
        // SAFETY: the buffer holds `T`'s layout (contents already dropped, so
        // writing over them is a plain memory write, not a double drop).
        unsafe { ptr.as_ptr().cast::<T>().write(value) };
        Self {
            ptr,
            layout,
            drop_fn: if std::mem::needs_drop::<T>() {
                DropFn::Local(drop_thunk::<T>)
            } else {
                DropFn::None
            },
            to_dyn: ToDyn::of::<T>(),
        }
    }

    /// Drop the value, keep the buffer for the pool.
    ///
    /// # Safety
    /// The buffer must not be used again until it is pooled or freed; this
    /// consumes the event (its drop fn runs exactly once here).
    unsafe fn drop_contents_keep_buffer(self) -> (NonNull<u8>, Layout) {
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: runs once for the live value in the buffer.
        unsafe {
            match this.drop_fn {
                DropFn::None => {}
                DropFn::Local(f) => f(this.ptr.as_ptr()),
                DropFn::Foreign(f) => f(this.ptr.as_ptr()),
            }
        }
        (this.ptr, this.layout)
    }

    /// Move a plugin's value into a host buffer (bitwise, like any Rust move).
    ///
    /// # Safety
    /// `src` must point to `size` readable bytes of a value the caller gives
    /// up ownership of; `drop_fn` and `to_dyn` must stay callable until the
    /// value is delivered or dropped. `buffer` must be a live allocation of
    /// exactly this `(size, align)` layout whose contents were already
    /// dropped, or `None` to allocate.
    pub(crate) unsafe fn from_foreign(
        src: *const u8,
        size: usize,
        align: usize,
        drop_fn: Option<RawDropFn>,
        to_dyn: Option<RawToDynFn>,
        buffer: Option<NonNull<u8>>,
    ) -> Option<Self> {
        let layout = Layout::from_size_align(size, align).ok()?;
        let ptr = match buffer {
            Some(p) if layout.size() != 0 => p,
            _ => Self::alloc(layout),
        };
        if size != 0 {
            // SAFETY: caller guarantees `size` readable bytes; `ptr` has room.
            unsafe { std::ptr::copy_nonoverlapping(src, ptr.as_ptr(), size) };
        }
        Some(Self {
            ptr,
            layout,
            drop_fn: drop_fn.map_or(DropFn::None, DropFn::Foreign),
            to_dyn: to_dyn.map_or(ToDyn::None, ToDyn::Foreign),
        })
    }
}

impl Drop for OwnedTyped {
    fn drop(&mut self) {
        // SAFETY: the buffer holds a live value; each drop fn runs once.
        unsafe {
            match self.drop_fn {
                DropFn::None => {}
                DropFn::Local(f) => f(self.ptr.as_ptr()),
                DropFn::Foreign(f) => f(self.ptr.as_ptr()),
            }
            if self.layout.size() != 0 {
                std::alloc::dealloc(self.ptr.as_ptr(), self.layout);
            }
        }
    }
}

pub(crate) enum OwnedPayload {
    Typed { id: u64, value: OwnedTyped },
    Dyn(DynEvent),
}

pub(crate) struct Queued {
    pub channel: Channel,
    pub payload: OwnedPayload,
}

/// What a [`flush`](crate::EventBus::flush) did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlushReport {
    /// Events dispatched (each counts once, whatever its subscriber count).
    pub delivered: usize,
    /// Rounds run. Round 1 is the queue as it was when the flush started;
    /// each later round is what the previous round's handlers queued.
    pub rounds: u32,
    /// `true` if the round limit stopped the flush with events still queued.
    pub hit_round_limit: bool,
    /// Events left in the queue for the next flush.
    pub remaining: usize,
    /// `true` if another flush of this bus was already running (re-entrant
    /// call from a handler, or another thread) and this call delivered
    /// nothing. The running flush re-checks the queue before it returns, so
    /// events queued before this call are still delivered by it.
    pub already_flushing: bool,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct Registry {
    by_id: FxHashMap<u64, Arc<EventDescriptor>>,
    /// Names in the main namespace (descriptors without a namespace).
    by_name: FxHashMap<String, u64>,
    /// Names in explicit namespaces. Never holds a reserved namespace.
    by_ns: FxHashMap<String, FxHashMap<String, u64>>,
}

// ---------------------------------------------------------------------------
// Core
// ---------------------------------------------------------------------------

pub(crate) struct Table<H: ?Sized + Slot> {
    lists: FxHashMap<Key, H::List>,
    tokens: FxHashMap<u64, Key>,
}

pub(crate) struct Core<H: ?Sized + Slot> {
    table: H::Lock<Table<H>>,
    /// Deferred events, sharded per channel (`queue_shard`). Each queued
    /// event carries its global sequence number; flush merging restores the
    /// publish order across shards.
    queue: [H::Mutex<Shard>; QUEUE_SHARDS],
    /// The flush side's merge buffer: shard batches drained into it and
    /// ordered by sequence number. Touched only while the flush's
    /// `flushing` flag excludes every other flush and nested flushes are
    /// refused, so it is single-owner for the whole flush; the owner swaps
    /// contents in and out, keeping capacity across flushes.
    batch_scratch: H::Mutex<Vec<(u64, Queued)>>,
    /// Stamps every queued event with its publish sequence.
    next_seq: AtomicU64,
    /// Recycled payload buffers (typed layouts and dynamic field buffers).
    pools: H::Mutex<Pools>,
    registry: H::Lock<Registry>,
    next_token: AtomicU64,
    flushing: AtomicBool,
    max_rounds: AtomicU32,
}

// ---------------------------------------------------------------------------
// Deferred queue
// ---------------------------------------------------------------------------

/// The deferred queue is sharded per channel (see [`queue_shard`]) so a
/// publish locks only its own channel's shard. The cross-shard publish order
/// is restored at flush time by the sequence number stamped on each queued
/// event, so the delivery contract stays exactly what it was with one queue:
/// published order across everything.
const QUEUE_SHARDS: usize = 16;

/// The shard holding the deferred queue for `channel`. Cheap and stable:
/// two publishes of the same channel always land on the same shard (kept in
/// that shard's FIFO), independent channels mostly split across shards.
fn queue_shard(channel: Channel) -> usize {
    let (discriminant, value) = match channel {
        Channel::Global => (0u64, 0),
        Channel::Entity(value) => (1, value),
        Channel::Class(value) => (2, value),
    };
    // FNV-1a over the discriminant then the value.
    let mut hash = 0xcbf2_9ce4_8422_2325;
    hash = (hash ^ discriminant).wrapping_mul(0x100_0000_01b3);
    hash = (hash ^ value).wrapping_mul(0x100_0000_01b3);
    (hash % QUEUE_SHARDS as u64) as usize
}

/// One shard of the deferred queue: channel ordered events, in publish order
/// (the sequence stamp restores the global order at flush time).
#[derive(Default)]
struct Shard(VecDeque<(u64, Queued)>);

// ---------------------------------------------------------------------------
// Payload pools
// ---------------------------------------------------------------------------

/// Recycled payloads: typed event buffers keyed by `(size, align)` and the
/// field buffers of dynamic events. Delivery round-trips pool to pool (see
/// `dispatch_owned` / `drain`), so a full pipeline stop hits nothing rather
/// than a new buffer transaction.
///
/// Caps bound the memory a busy pipeline can hold: pooled buffers past a cap
/// are dropped rather than stored. Buffers are deallocated (contents were
/// dropped at harvest time) with the core.
pub(crate) struct Pools {
    /// Typed payload buffers by `(size, align)`; each buffer holds no value
    /// (its contents were dropped before pooling).
    buffers: FxHashMap<(usize, usize), Vec<NonNull<u8>>>,
    buffers_len: usize,
    buffers_cap: usize,
    /// Fields vectors of dynamic events (`DynEvent::fields`).
    fields: Vec<Vec<crate::DynValue>>,
    /// `DynValue::Str` buffers, seeded from delivered events.
    strings: Vec<String>,
    /// `DynValue::Bytes` buffers.
    bytes: Vec<Vec<u8>>,
    dyn_cap: usize,
}

impl Pools {
    fn new(buffers_cap: usize, dyn_cap: usize) -> Self {
        Self {
            buffers: FxHashMap::default(),
            buffers_len: 0,
            buffers_cap,
            fields: Vec::new(),
            strings: Vec::new(),
            bytes: Vec::new(),
            dyn_cap,
        }
    }

    /// A typed payload buffer of exactly this layout; `None` empties it.
    pub(crate) fn take_buffer(&mut self, layout: &Layout) -> Option<NonNull<u8>> {
        if layout.size() == 0 {
            return None;
        }
        let key = (layout.size(), layout.align());
        match self.buffers.get_mut(&key).and_then(Vec::pop) {
            Some(p) => {
                self.buffers_len -= 1;
                Some(p)
            }
            None => None,
        }
    }

    /// Recycle a typed payload buffer (contents were dropped by the caller).
    pub(crate) fn put_buffer(&mut self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() == 0 || self.buffers_len >= self.buffers_cap {
            return;
        }
        self.buffers
            .entry((layout.size(), layout.align()))
            .or_default()
            .push(ptr);
        self.buffers_len += 1;
    }

    pub(crate) fn take_fields(&mut self) -> Vec<crate::DynValue> {
        self.fields.pop().unwrap_or_default()
    }

    /// Recycle the fields vector; already emptied before being offered here.
    pub(crate) fn put_fields(&mut self, buf: Vec<crate::DynValue>) {
        if self.fields.len() < self.dyn_cap {
            self.fields.push(buf);
        }
    }

    pub(crate) fn put_string(&mut self, s: String) {
        if self.strings.len() < self.dyn_cap {
            self.strings.push(s);
        }
    }

    pub(crate) fn put_bytes(&mut self, b: Vec<u8>) {
        if self.bytes.len() < self.dyn_cap {
            self.bytes.push(b);
        }
    }

    /// Recycle a delivered dynamic event: `Str`/`Bytes` buffers split out,
    /// the `fields` vector emptied in place and reused.
    pub(crate) fn put_dyn_fields(&mut self, mut fields: Vec<crate::DynValue>) {
        for v in fields.drain(..) {
            match v {
                DynValue::Str(s) => self.put_string(s),
                DynValue::Bytes(b) => self.put_bytes(b),
                _ => drop(v),
            }
        }
        self.put_fields(fields);
    }
}

// SAFETY: a pool is only reachable through its core's lock, and its raw
// buffers are owned by exactly one pool (as documented on `put_buffer`).
// The same thread-boundary promise the buffers' live values carry as
// `OwnedTyped` applies here.
unsafe impl Send for Pools {}

impl Drop for Pools {
    fn drop(&mut self) {
        for (key, buf) in self.buffers.drain() {
            let layout = std::alloc::Layout::from_size_align(key.0, key.1).expect("pooled layout");
            for p in buf {
                // SAFETY: buffers pooled here were allocated with this layout.
                unsafe {
                    std::alloc::dealloc(p.as_ptr(), layout);
                }
            }
        }
    }
}

impl<H: ?Sized + Slot> Core<H> {
    pub(crate) fn new() -> Self {
        Self {
            table: Lock::new(Table {
                lists: FxHashMap::default(),
                tokens: FxHashMap::default(),
            }),
            queue: std::array::from_fn(|_| Lock::new(Shard::default())),
            batch_scratch: Lock::new(Vec::new()),
            next_seq: AtomicU64::new(0),
            pools: Lock::new(Pools::new(DEFAULT_POOL_BUFFERS_CAP, DEFAULT_POOL_DYN_CAP)),
            registry: Lock::new(Registry::default()),
            next_token: AtomicU64::new(1),
            flushing: AtomicBool::new(false),
            max_rounds: AtomicU32::new(crate::DEFAULT_MAX_FLUSH_ROUNDS),
        }
    }

    /// Tokens start at 1 (0 means "failed" at the FFI boundary) and only
    /// grow, so they double as the tie-breaker for equal priorities.
    pub(crate) fn next_token(&self) -> u64 {
        self.next_token.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn insert(&self, id: u64, channel: Channel, e: Arc<Entry<H>>) {
        let key = (id, channel);
        self.table.write(|t| {
            t.tokens.insert(e.token, key);
            let list = t.lists.entry(key).or_default().make_mut();
            // Tokens are increasing, so the new entry goes after every entry
            // of higher or equal priority: stable order for ties.
            let pos = list.partition_point(|x| x.priority >= e.priority);
            list.insert(pos, e);
        })
    }

    pub(crate) fn remove(&self, token: u64) -> bool {
        let (removed, emptied) = self.table.write(|t| {
            let Some(key) = t.tokens.remove(&token) else {
                return (None, None);
            };
            let Some(list) = t.lists.get_mut(&key) else {
                return (None, None);
            };
            let v = list.make_mut();
            let removed = v.iter().position(|e| e.token == token).map(|i| v.remove(i));
            if let Some(e) = &removed {
                e.alive.store(false, Ordering::Release);
            }
            let emptied = if v.is_empty() {
                t.lists.remove(&key)
            } else {
                None
            };
            (removed, emptied)
        });
        // Dropped outside the lock: a handler's destructor may unsubscribe
        // other handlers or publish.
        let found = removed.is_some();
        drop(removed);
        drop(emptied);
        found
    }

    pub(crate) fn is_subscribed(&self, token: u64) -> bool {
        self.table.read(|t| t.tokens.contains_key(&token))
    }

    pub(crate) fn clear_channel(&self, channel: Channel) -> usize {
        let dropped: Vec<H::List> = self.table.write(|t| {
            let keys: Vec<Key> = t.lists.keys().filter(|k| k.1 == channel).copied().collect();
            let mut dropped = Vec::new();
            for k in keys {
                if let Some(list) = t.lists.remove(&k) {
                    for e in list.iter() {
                        e.alive.store(false, Ordering::Release);
                        t.tokens.remove(&e.token);
                    }
                    dropped.push(list);
                }
            }
            dropped
        });
        dropped.iter().map(|l| l.len()).sum()
    }

    pub(crate) fn subscriber_count(&self, id: u64, channel: Channel) -> usize {
        self.table
            .read(|t| t.lists.get(&(id, channel)).map_or(0, |l| l.len()))
    }

    #[inline]
    fn snapshot(&self, key: Key) -> Option<H::List> {
        self.table.read(|t| t.lists.get(&key).cloned())
    }

    /// Deliver to the subscribers of `(id, channel)`. The view is only
    /// built when there is at least one subscriber.
    #[inline]
    pub(crate) fn dispatch<'a>(
        &self,
        id: u64,
        channel: Channel,
        view: impl FnOnce() -> EventView<'a>,
    ) {
        if let Some(list) = self.snapshot((id, channel)) {
            let ev = view();
            for e in list.iter() {
                if e.alive.load(Ordering::Acquire) {
                    e.handler.deliver(&ev);
                }
            }
        }
    }

    pub(crate) fn dispatch_parallel<'a>(
        &self,
        id: u64,
        channel: Channel,
        view: impl FnOnce() -> EventView<'a>,
    ) where
        H: Send + Sync,
    {
        let Some(list) = self.snapshot((id, channel)) else {
            return;
        };
        let ev = &view();
        let run = |e: &Arc<Entry<H>>| {
            if e.alive.load(Ordering::Acquire) {
                e.handler.deliver(ev);
            }
        };
        #[cfg(feature = "parallel")]
        {
            use rayon::prelude::*;
            list.par_iter().for_each(run);
        }
        #[cfg(not(feature = "parallel"))]
        {
            std::thread::scope(|s| {
                for e in list.iter() {
                    s.spawn(move || run(e));
                }
            });
        }
    }

    /// The dynamic form of a queued typed payload, written into pooled
    /// buffers so delivering to dynamic subscribers allocates nothing once
    /// the pool is primed (first use allocates, later events reuse the
    /// buffers the previous event returned).
    ///
    /// Shared as an `Arc` so the flush can recycle its buffers only after
    /// the subscribers have run.
    fn build_pooled_dyn(&self, event: &OwnedTyped) -> Option<Arc<DynEvent>> {
        let mut sink = self.pools.write(|p| {
            let mut sink = DynEventSink::empty();
            sink.give_fields(p.take_fields());
            sink.give_spares(std::mem::take(&mut p.strings), std::mem::take(&mut p.bytes));
            sink
        });
        // SAFETY: `ptr` is the live value `OwnedTyped` holds until delivery;
        // the thunk is the one built for its exact type.
        let ok = unsafe {
            match &event.to_dyn {
                ToDyn::Local(f) => f(event.ptr.as_ptr(), &mut sink),
                _ => false,
            }
        };
        let (event_dyn, spares) = sink.finish();
        self.put_spares(spares);
        if ok {
            event_dyn.map(Arc::new)
        } else {
            None
        }
    }

    /// Unused sink buffers return to the pool.
    fn put_spares(&self, spares: SinkSpares) {
        self.pools.write(|p| {
            p.strings = spares.strings;
            p.bytes = spares.bytes;
            p.strings.truncate(p.dyn_cap);
            p.bytes.truncate(p.dyn_cap);
        });
    }

    /// Return a delivered queued event's buffers to the pools. Contents are
    /// dropped first (the drop-once delivery contract), the memory is kept.
    #[inline]
    fn recycle_queued(&self, q: Queued) {
        match q.payload {
            OwnedPayload::Typed { value, .. } => {
                // SAFETY: the buffer is dropped here exactly once and pooled
                // (or handed to the allocator) right after; never reused while
                // drop_fn runs.
                let (ptr, layout) = unsafe { value.drop_contents_keep_buffer() };
                self.pools.write(|p| p.put_buffer(ptr, layout));
            }
            OwnedPayload::Dyn(d) => {
                self.pools.write(|p| p.put_dyn_fields(d.fields));
            }
        }
    }

    pub(crate) fn dispatch_owned(&self, q: &Queued) {
        let mut prebuilt: Option<Arc<DynEvent>> = None;
        {
            let view = match &q.payload {
                OwnedPayload::Typed { id, value } => {
                    // Dynamic subscribers in this channel's list get the
                    // pooled dynamic form up front; dispatch never converts
                    // lazily.
                    if let Some(list) = self.snapshot((*id, q.channel)) {
                        if list.iter().any(|e| e.handler.is_dyn()) {
                            prebuilt = self.build_pooled_dyn(value);
                        }
                    }
                    let view = self.make_typed_view(*id, q.channel, value);
                    if let Some(f) = &prebuilt {
                        let _ = view.dyn_form.set(Arc::clone(f));
                    }
                    view
                }
                OwnedPayload::Dyn(d) => EventView::dynamic(q.channel, d),
            };
            self.run_dispatch(view);
        } // the view (and its Arc clone) drop here
        // Handlers are done; the pooled dynamic form's buffers go back.
        self.harvest_prebuilt(&mut prebuilt);
    }

    fn make_typed_view<'a>(
        &self,
        id: u64,
        channel: Channel,
        value: &'a OwnedTyped,
    ) -> EventView<'a> {
        EventView::raw_typed(
            id,
            channel,
            value.ptr.as_ptr(),
            value.layout.size(),
            value.layout.align(),
            &value.to_dyn,
        )
    }

    fn run_dispatch(&self, ev: EventView<'_>) {
        if let Some(list) = self.snapshot((ev.id, ev.channel)) {
            for e in list.iter() {
                if e.alive.load(Ordering::Acquire) {
                    e.handler.deliver(&ev);
                }
            }
        }
    }

    /// Handlers are done; the pooled dynamic form's buffers go back.
    fn harvest_prebuilt(&self, prebuilt: &mut Option<Arc<DynEvent>>) {
        if let Some(arc) = prebuilt.take() {
            if let Ok(ev) = Arc::try_unwrap(arc) {
                self.pools.write(|p| p.put_dyn_fields(ev.fields));
            }
        }
    }

    pub(crate) fn enqueue(&self, q: Queued) {
        let shard = queue_shard(q.channel);
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        self.queue[shard].write(|shard| shard.0.push_back((seq, q)));
    }

    pub(crate) fn queued_len(&self) -> usize {
        self.queue
            .iter()
            .map(|shard| shard.read(|shard| shard.0.len()))
            .sum()
    }

    pub(crate) fn max_rounds(&self) -> u32 {
        self.max_rounds.load(Ordering::Relaxed)
    }

    pub(crate) fn set_max_rounds(&self, n: u32) {
        self.max_rounds.store(n.max(1), Ordering::Relaxed);
    }

    pub(crate) fn flush(&self, max_rounds: u32) -> FlushReport {
        let mut report = FlushReport::default();
        loop {
            if self.flushing.swap(true, Ordering::AcqRel) {
                // Another flush is running; it re-checks the queue after it
                // releases the flag (below), so our events are not stranded.
                report.already_flushing = report.rounds == 0;
                break;
            }
            {
                struct Reset<'a>(&'a AtomicBool);
                impl Drop for Reset<'_> {
                    fn drop(&mut self) {
                        self.0.store(false, Ordering::Release);
                    }
                }
                let _reset = Reset(&self.flushing);
                self.drain(max_rounds, &mut report);
            }
            // Events queued (and flushes refused) between our last empty
            // check and the flag release would otherwise wait for the next
            // flush: go again.
            if report.hit_round_limit || self.queued_len() == 0 {
                break;
            }
        }
        report.remaining = self.queued_len();
        report
    }

    fn drain(&self, max_rounds: u32, report: &mut FlushReport) {
        loop {
            // Anything queued next round: dispatch, then go again.
            if self.queued_len() == 0 {
                return;
            }
            if report.rounds >= max_rounds {
                report.hit_round_limit = true;
                return;
            }
            // One shard lock at a time; publishers to other shards (and to
            // this one, on another thread) keep going while we collect.
            let mut collected = false;
            for shard in &self.queue {
                let had_events = shard.write(|shard| !shard.0.is_empty());
                if had_events {
                    self.batch_scratch
                        .write(|batch| shard.write(|shard| batch.extend(shard.0.drain(..))));
                    collected = true;
                }
            }
            if !collected {
                return;
            }
            // Restore the publish order across shard batches by sequence
            // stamp, then deliver. Events queued by handlers during these
            // deliveries only go to the shards, never to the scratch: take
            // the merged batch out, deliver it, and return the buffer after
            // the round so its capacity carries over.
            let mut merged = self.batch_scratch.write(|batch| {
                batch.sort_unstable_by_key(|(seq, _)| *seq);
                std::mem::take(batch)
            });
            report.rounds += 1;
            for (_, event) in merged.drain(..) {
                // Delivered now; its buffers go to the pools right after.
                self.dispatch_owned(&event);
                self.recycle_queued(event);
                report.delivered += 1;
            }
            self.batch_scratch.write(|batch| *batch = merged);
        }
    }

    // --- payload pools ---------------------------------------------------

    /// A pooled buffer matching `layout`, or `None` (allocate fresh).
    pub(crate) fn take_buffer(&self, layout: &Layout) -> Option<NonNull<u8>> {
        self.pools.write(|p| p.take_buffer(layout))
    }

    /// Overrides for the pool caps (tests/tools); excess buffers are
    /// dropped at the next harvest.
    pub(crate) fn set_pool_caps(&self, buffers: usize, dyn_cap: usize) {
        self.pools.write(|p| {
            p.buffers_cap = buffers;
            p.dyn_cap = dyn_cap;
            p.fields.truncate(dyn_cap);
            p.strings.truncate(dyn_cap);
            p.bytes.truncate(dyn_cap);
        });
    }

    /// (buffers in pool, fields vecs, strings, bytes) — diagnostics.
    pub(crate) fn pool_sizes(&self) -> (usize, usize, usize, usize) {
        self.pools.write(|p| {
            (
                p.buffers_len,
                p.fields.len(),
                p.strings.len(),
                p.bytes.len(),
            )
        })
    }

    // --- registry --------------------------------------------------------

    pub(crate) fn register(&self, desc: EventDescriptor) -> Result<u64, RegistryError> {
        if let Some(ns) = &desc.namespace {
            if crate::stable_id::namespace_is_reserved(ns) {
                return Err(RegistryError::ReservedNamespace(ns.clone()));
            }
        }
        self.registry.write(|r| {
            if let Some(existing) = r.by_id.get(&desc.id) {
                return if **existing == desc {
                    Ok(desc.id)
                } else {
                    Err(RegistryError::IdConflict {
                        id: desc.id,
                        existing: existing.name.clone(),
                    })
                };
            }
            let names = match &desc.namespace {
                None => &mut r.by_name,
                Some(ns) => r.by_ns.entry(ns.clone()).or_default(),
            };
            if let Some(&existing_id) = names.get(&desc.name) {
                return Err(RegistryError::NameConflict {
                    name: desc.name,
                    existing_id,
                });
            }
            let id = desc.id;
            names.insert(desc.name.clone(), id);
            r.by_id.insert(id, Arc::new(desc));
            Ok(id)
        })
    }

    pub(crate) fn descriptor(&self, id: u64) -> Option<Arc<EventDescriptor>> {
        self.registry.read(|r| r.by_id.get(&id).cloned())
    }

    pub(crate) fn descriptor_by_name(&self, name: &str) -> Option<Arc<EventDescriptor>> {
        self.registry
            .read(|r| r.by_name.get(name).and_then(|id| r.by_id.get(id)).cloned())
    }

    /// Descriptors in explicit namespace `ns`; `None` for a reserved name
    /// (the main namespace is never reachable this way).
    pub(crate) fn descriptor_by_name_in(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<Arc<EventDescriptor>> {
        self.registry.read(|r| {
            r.by_ns
                .get(ns)
                .and_then(|m| m.get(name))
                .and_then(|id| r.by_id.get(id))
                .cloned()
        })
    }

    /// Main-namespace descriptors, sorted by name.
    pub(crate) fn descriptors(&self) -> Vec<Arc<EventDescriptor>> {
        let mut v: Vec<_> = self.registry.read(|r| {
            r.by_id
                .values()
                .filter(|d| d.namespace.is_none())
                .cloned()
                .collect()
        });
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// Descriptors of explicit namespace `ns`, sorted by name.
    pub(crate) fn descriptors_in(&self, ns: &str) -> Vec<Arc<EventDescriptor>> {
        let mut v: Vec<_> = self.registry.read(|r| {
            r.by_id
                .values()
                .filter(|d| d.namespace.as_deref() == Some(ns))
                .cloned()
                .collect()
        });
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// Every descriptor, sorted by (namespace, name); main first.
    pub(crate) fn all_descriptors(&self) -> Vec<Arc<EventDescriptor>> {
        let mut v: Vec<_> = self.registry.read(|r| r.by_id.values().cloned().collect());
        v.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
        v
    }

    /// Explicit namespaces that have at least one descriptor, sorted.
    pub(crate) fn namespaces(&self) -> Vec<String> {
        let mut v: Vec<String> = self.registry.read(|r| r.by_ns.keys().cloned().collect());
        v.sort();
        v
    }

    pub(crate) fn check_dyn(&self, ev: &DynEvent) -> Result<(), DynEventError> {
        self.registry.read(|r| match r.by_id.get(&ev.id) {
            Some(d) => d.check(ev),
            None => Err(DynEventError::UnknownEvent(ev.id)),
        })
    }
}
