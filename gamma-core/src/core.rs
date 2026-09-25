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
use crate::{Channel, DynEvent, DynEventError, Event, EventDescriptor, RegistryError};

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
    /// `T::to_dyn` from this copy of gamma-core.
    Local(unsafe fn(*const u8) -> Option<DynEvent>),
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

    /// # Safety
    /// `ptr` must point to a live value of the type this converter was made for.
    unsafe fn convert(&self, ptr: *const u8) -> Option<DynEvent> {
        match self {
            ToDyn::None => None,
            // SAFETY: forwarded from the caller.
            ToDyn::Local(f) => unsafe { f(ptr) },
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

unsafe fn to_dyn_thunk<T: Event>(ptr: *const u8) -> Option<DynEvent> {
    // SAFETY: only built by `ToDyn::of::<T>` and called with a `*const T`.
    unsafe { &*ptr.cast::<T>() }.to_dyn()
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
            enc_cache: OnceLock::new(),
        }
    }

    pub(crate) fn dynamic(channel: Channel, ev: &'a DynEvent) -> Self {
        Self {
            id: ev.id,
            channel,
            payload: Payload::Dyn(ev),
            dyn_cache: OnceLock::new(),
            enc_cache: OnceLock::new(),
        }
    }

    /// The event as a [`DynEvent`], if it is dynamic or its type is reflected.
    pub(crate) fn as_dyn(&self) -> Option<&DynEvent> {
        match &self.payload {
            Payload::Dyn(d) => Some(d),
            // SAFETY: `ptr` is the live value this view was built from.
            Payload::Typed { ptr, to_dyn, .. } => self
                .dyn_cache
                .get_or_init(|| unsafe { to_dyn.convert(*ptr) })
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
    // SAFETY: called once on the buffer `OwnedTyped::new::<T>` wrote.
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

    pub(crate) fn new<T: Event>(value: T) -> Self {
        let layout = Layout::new::<T>();
        let ptr = Self::alloc(layout);
        // SAFETY: freshly allocated for `T`'s layout.
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

    /// Move a plugin's value into a host buffer (bitwise, like any Rust move).
    ///
    /// # Safety
    /// `src` must point to `size` readable bytes of a value the caller gives
    /// up ownership of; `drop_fn` and `to_dyn` must stay callable until the
    /// value is delivered or dropped.
    pub(crate) unsafe fn from_foreign(
        src: *const u8,
        size: usize,
        align: usize,
        drop_fn: Option<RawDropFn>,
        to_dyn: Option<RawToDynFn>,
    ) -> Option<Self> {
        let layout = Layout::from_size_align(size, align).ok()?;
        let ptr = Self::alloc(layout);
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
    by_name: FxHashMap<String, u64>,
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
    queue: H::Mutex<VecDeque<Queued>>,
    registry: H::Lock<Registry>,
    next_token: AtomicU64,
    flushing: AtomicBool,
    max_rounds: AtomicU32,
}

impl<H: ?Sized + Slot> Core<H> {
    pub(crate) fn new() -> Self {
        Self {
            table: Lock::new(Table {
                lists: FxHashMap::default(),
                tokens: FxHashMap::default(),
            }),
            queue: Lock::new(VecDeque::new()),
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

    fn dispatch_owned(&self, q: &Queued) {
        match &q.payload {
            OwnedPayload::Typed { id, value } => self.dispatch(*id, q.channel, || {
                EventView::raw_typed(
                    *id,
                    q.channel,
                    value.ptr.as_ptr(),
                    value.layout.size(),
                    value.layout.align(),
                    &value.to_dyn,
                )
            }),
            OwnedPayload::Dyn(d) => {
                self.dispatch(d.id, q.channel, || EventView::dynamic(q.channel, d))
            }
        }
    }

    pub(crate) fn enqueue(&self, q: Queued) {
        self.queue.write(|queue| queue.push_back(q));
    }

    pub(crate) fn queued_len(&self) -> usize {
        self.queue.read(VecDeque::len)
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
            let batch = self.queue.write(|q| {
                if q.is_empty() {
                    None
                } else if report.rounds >= max_rounds {
                    report.hit_round_limit = true;
                    None
                } else {
                    Some(std::mem::take(q))
                }
            });
            let Some(batch) = batch else { return };
            report.rounds += 1;
            // Delivered after the queue lock is released; each event is
            // dropped right after its delivery.
            for ev in batch {
                self.dispatch_owned(&ev);
                report.delivered += 1;
            }
        }
    }

    // --- registry --------------------------------------------------------

    pub(crate) fn register(&self, desc: EventDescriptor) -> Result<u64, RegistryError> {
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
            if let Some(&existing_id) = r.by_name.get(&desc.name) {
                return Err(RegistryError::NameConflict {
                    name: desc.name,
                    existing_id,
                });
            }
            let id = desc.id;
            r.by_name.insert(desc.name.clone(), id);
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

    pub(crate) fn descriptors(&self) -> Vec<Arc<EventDescriptor>> {
        let mut v: Vec<_> = self.registry.read(|r| r.by_id.values().cloned().collect());
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    pub(crate) fn check_dyn(&self, ev: &DynEvent) -> Result<(), DynEventError> {
        self.registry.read(|r| match r.by_id.get(&ev.id) {
            Some(d) => d.check(ev),
            None => Err(DynEventError::UnknownEvent(ev.id)),
        })
    }
}
