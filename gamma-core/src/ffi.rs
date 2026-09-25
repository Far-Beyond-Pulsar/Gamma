//! The plugin boundary.
//!
//! A host exports a bus with [`EventBus::export_raw`](crate::EventBus::export_raw)
//! or [`SyncEventBus::export_raw`](crate::SyncEventBus::export_raw) and passes
//! the resulting [`RawBus`] to a plugin (for example as the argument of an
//! `extern "C"` init function). The plugin wraps it in a [`ForeignBus`] and
//! gets a typed API that works whatever compiler, flags or Gamma copy the
//! plugin was built with, as long as both sides agree on [`ABI_VERSION`].
//!
//! # What crosses the boundary
//!
//! Only `#[repr(C)]` structs, integers, raw pointers and `extern "C"`
//! function pointers:
//!
//! * **Typed events** as `(id, size, align, pointer)` in a [`RawEventRef`].
//!   The receiving side (compiled into the subscriber's library) checks all
//!   three before casting. Nothing is `downcast` with `TypeId`.
//! * **Dynamic events and descriptors** in Gamma's wire encoding
//!   ([`DynEvent::encode`]).
//! * **Handlers** as a [`RawHandler`]: data pointer plus `call` and `drop`
//!   functions from the subscriber's library.
//!
//! # Allocation and drop rules
//!
//! * A handler is created by the library that subscribes it and dropped by
//!   calling its own `drop` function, so it is freed by the allocator that
//!   allocated it. The bus stores it opaquely.
//! * An immediately published typed event is only borrowed for the call.
//! * A deferred typed event is moved (bitwise) into a host buffer; its
//!   contents are later dropped with the publisher's `drop_fn`, the buffer by
//!   the host.
//! * Encoded bytes are always copied by the receiver.
//!
//! No shared global allocator is needed. The plugin must drop every
//! [`ForeignSubscription`] and [`ForeignBus`] and make sure its deferred
//! events are flushed before it is unloaded, because the bus will call
//! back into its code.
//!
//! # Threads and panics
//!
//! A [`RawBus`] exported from an [`EventBus`](crate::EventBus) has no
//! [`FLAG_THREAD_SAFE`] and must only be used on the host's bus thread. Plugin
//! handlers subscribed through [`ForeignBus`] must be `Send + Sync`, since the
//! plugin cannot know which bus it was given. A panic that reaches an
//! `extern "C"` function aborts the process.

use std::ffi::c_void;
use std::fmt;
use std::mem::ManuallyDrop;
use std::sync::{Arc, Weak};

use crate::core::{
    Core, ErasedHandler, EventView, OwnedPayload, OwnedTyped, Payload, Queued, Slot, ToDyn,
};
use crate::{Channel, DynEvent, Event, EventDescriptor, SubscribeOptions};

/// Version of this boundary. Bumped on any change to the `Raw*` structs,
/// their functions' signatures or the wire encoding.
pub const ABI_VERSION: u32 = 1;

/// [`RawBus::flags`]: the bus may be used from any thread.
pub const FLAG_THREAD_SAFE: u32 = 1;

/// [`RawEventRef::kind`]: `data` points to a `#[repr(C)]` value of `len`
/// bytes aligned to `align`.
pub const EVENT_TYPED: u32 = 0;
/// [`RawEventRef::kind`]: `data`/`len` hold an encoded [`DynEvent`].
pub const EVENT_DYN: u32 = 1;

/// [`RawHandler::kind`]: wants typed events of `size`/`align`.
pub const HANDLER_TYPED: u32 = 0;
/// [`RawHandler::kind`]: wants encoded dynamic events.
pub const HANDLER_DYN: u32 = 1;

/// Status: success.
pub const STATUS_OK: i32 = 0;
/// Status: no descriptor registered for a dynamic event.
pub const STATUS_UNKNOWN_EVENT: i32 = 1;
/// Status: dynamic event fields do not match the descriptor.
pub const STATUS_BAD_FIELDS: i32 = 2;
/// Status: invalid size/alignment or event kind.
pub const STATUS_BAD_LAYOUT: i32 = 3;
/// Status: malformed wire bytes.
pub const STATUS_DECODE: i32 = 4;
/// Status: the descriptor conflicts with a registered one.
pub const STATUS_REGISTRY: i32 = 5;
/// Status: invalid channel.
pub const STATUS_BAD_CHANNEL: i32 = 6;

// ---------------------------------------------------------------------------
// Raw types
// ---------------------------------------------------------------------------

/// [`Channel`] at the boundary: `kind` 0 = global, 1 = entity, 2 = class.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawChannel {
    /// Channel kind.
    pub kind: u32,
    /// Entity or class id (0 for global).
    pub value: u64,
}

impl From<Channel> for RawChannel {
    fn from(c: Channel) -> Self {
        match c {
            Channel::Global => Self { kind: 0, value: 0 },
            Channel::Entity(v) => Self { kind: 1, value: v },
            Channel::Class(v) => Self { kind: 2, value: v },
        }
    }
}

impl RawChannel {
    /// Back to a [`Channel`]; `None` for an unknown kind.
    pub fn to_channel(self) -> Option<Channel> {
        match self.kind {
            0 => Some(Channel::Global),
            1 => Some(Channel::Entity(self.value)),
            2 => Some(Channel::Class(self.value)),
            _ => None,
        }
    }
}

/// Encodes the typed value at `event` as a [`DynEvent`] and writes the bytes
/// to `sink` (from the publisher's library).
pub type RawToDynFn = unsafe extern "C" fn(event: *const u8, sink: *mut RawSink);
/// Drops the typed value at `event` in place (from the publisher's library).
pub type RawDropFn = unsafe extern "C" fn(event: *mut u8);

/// A byte sink owned by the caller; the callee copies bytes into it.
#[repr(C)]
pub struct RawSink {
    /// Sink state.
    pub ctx: *mut c_void,
    /// Append `len` bytes.
    pub write: unsafe extern "C" fn(ctx: *mut c_void, data: *const u8, len: usize),
}

/// An event at the boundary (borrowed for the duration of a call).
#[repr(C)]
pub struct RawEventRef {
    /// Stable event id.
    pub id: u64,
    /// Target channel.
    pub channel: RawChannel,
    /// [`EVENT_TYPED`] or [`EVENT_DYN`].
    pub kind: u32,
    /// Value or encoded bytes.
    pub data: *const u8,
    /// Size of the value, or number of encoded bytes.
    pub len: usize,
    /// Alignment of the value (typed only).
    pub align: usize,
    /// Converter for reflected typed events (publish calls only).
    pub to_dyn: Option<RawToDynFn>,
}

/// A subscriber at the boundary. Owned by the bus after a successful or
/// failed `subscribe` call, which will eventually call `drop(data)` exactly
/// once.
#[repr(C)]
pub struct RawHandler {
    /// Handler state.
    pub data: *mut c_void,
    /// Deliver one event.
    pub call: unsafe extern "C" fn(data: *mut c_void, event: *const RawEventRef),
    /// Free `data` (in the subscriber's library).
    pub drop: unsafe extern "C" fn(data: *mut c_void),
    /// [`HANDLER_TYPED`] or [`HANDLER_DYN`].
    pub kind: u32,
    /// Non-zero if a typed handler also accepts dynamic events with its id.
    pub accepts_dyn: u32,
    /// Expected size (typed only).
    pub size: usize,
    /// Expected alignment (typed only).
    pub align: usize,
}

/// A bus exported to another library. Holds one strong reference to the
/// bus; release it with `release(ctx)` (done by [`ForeignBus`]'s `Drop`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawBus {
    /// Must equal [`ABI_VERSION`].
    pub abi_version: u32,
    /// `size_of::<RawBus>()` on the exporting side.
    pub struct_size: u32,
    /// [`FLAG_THREAD_SAFE`] or 0.
    pub flags: u32,
    /// Opaque bus pointer.
    pub ctx: *const c_void,
    /// Add a strong reference.
    pub retain: unsafe extern "C" fn(ctx: *const c_void),
    /// Drop a strong reference.
    pub release: unsafe extern "C" fn(ctx: *const c_void),
    /// Make a weak reference (returned pointer).
    pub downgrade: unsafe extern "C" fn(ctx: *const c_void) -> *const c_void,
    /// Drop a weak reference.
    pub weak_release: unsafe extern "C" fn(weak: *const c_void),
    /// Subscribe; returns a token, or 0 on failure (the handler has then
    /// already been dropped).
    pub subscribe: unsafe extern "C" fn(
        ctx: *const c_void,
        id: u64,
        channel: RawChannel,
        priority: i32,
        handler: RawHandler,
    ) -> u64,
    /// Unsubscribe through a weak reference; returns 1 if removed.
    pub unsubscribe_weak: unsafe extern "C" fn(weak: *const c_void, token: u64) -> u32,
    /// Publish now; returns a status.
    pub publish: unsafe extern "C" fn(ctx: *const c_void, event: *const RawEventRef) -> i32,
    /// Queue for the next flush. For typed events the bus takes ownership
    /// of the value on [`STATUS_OK`] (the caller must not drop it) and later
    /// drops it with `drop_fn`.
    pub publish_deferred: unsafe extern "C" fn(
        ctx: *const c_void,
        event: *const RawEventRef,
        drop_fn: Option<RawDropFn>,
    ) -> i32,
    /// Register an encoded [`EventDescriptor`]; returns a status.
    pub register_descriptor:
        unsafe extern "C" fn(ctx: *const c_void, data: *const u8, len: usize) -> i32,
}

// ---------------------------------------------------------------------------
// Host side
// ---------------------------------------------------------------------------

/// A plugin's handler, stored in a host bus.
pub(crate) struct ForeignHandler(RawHandler);

// SAFETY: plugins subscribe through `ForeignBus`, which requires
// `Send + Sync` handlers; raw users accept the same contract (module docs).
unsafe impl Send for ForeignHandler {}
unsafe impl Sync for ForeignHandler {}

impl Drop for ForeignHandler {
    fn drop(&mut self) {
        // SAFETY: the bus owns the handler and drops it exactly once.
        unsafe { (self.0.drop)(self.0.data) }
    }
}

impl ForeignHandler {
    fn call(&self, ev: &EventView<'_>, kind: u32, data: *const u8, len: usize, align: usize) {
        let raw = RawEventRef {
            id: ev.id,
            channel: ev.channel.into(),
            kind,
            data,
            len,
            align,
            to_dyn: None,
        };
        // SAFETY: `raw` and the memory it points to live for the call.
        unsafe { (self.0.call)(self.0.data, &raw) }
    }

    fn call_dyn(&self, ev: &EventView<'_>) {
        if let Some(bytes) = ev.encoded() {
            self.call(ev, EVENT_DYN, bytes.as_ptr(), bytes.len(), 1);
        }
    }
}

impl ErasedHandler for ForeignHandler {
    fn deliver(&self, ev: &EventView<'_>) {
        let h = &self.0;
        match (&ev.payload, h.kind) {
            (
                Payload::Typed {
                    ptr, size, align, ..
                },
                HANDLER_TYPED,
            ) => {
                if *size == h.size && *align == h.align {
                    self.call(ev, EVENT_TYPED, *ptr, *size, *align);
                }
            }
            (Payload::Typed { .. }, _) => self.call_dyn(ev),
            (Payload::Dyn(_), HANDLER_TYPED) if h.accepts_dyn == 0 => {}
            (Payload::Dyn(_), _) => self.call_dyn(ev),
        }
    }
}

pub(crate) fn export<H: ?Sized + Slot>(core: Arc<Core<H>>) -> RawBus {
    RawBus {
        abi_version: ABI_VERSION,
        struct_size: size_of::<RawBus>() as u32,
        flags: if H::THREAD_SAFE { FLAG_THREAD_SAFE } else { 0 },
        ctx: Arc::into_raw(core).cast(),
        retain: host_retain::<H>,
        release: host_release::<H>,
        downgrade: host_downgrade::<H>,
        weak_release: host_weak_release::<H>,
        subscribe: host_subscribe::<H>,
        unsubscribe_weak: host_unsubscribe_weak::<H>,
        publish: host_publish::<H>,
        publish_deferred: host_publish_deferred::<H>,
        register_descriptor: host_register::<H>,
    }
}

// SAFETY (all `host_*`): `ctx` is a pointer from `Arc::into_raw` in `export`
// (or a retained copy) that the caller still holds a strong reference
// through; `weak` comes from `host_downgrade`.

unsafe fn core_ref<'a, H: ?Sized + Slot>(ctx: *const c_void) -> &'a Core<H> {
    // SAFETY: see above.
    unsafe { &*ctx.cast::<Core<H>>() }
}

unsafe extern "C" fn host_retain<H: ?Sized + Slot>(ctx: *const c_void) {
    unsafe { Arc::increment_strong_count(ctx.cast::<Core<H>>()) }
}

unsafe extern "C" fn host_release<H: ?Sized + Slot>(ctx: *const c_void) {
    unsafe { Arc::decrement_strong_count(ctx.cast::<Core<H>>()) }
}

unsafe extern "C" fn host_downgrade<H: ?Sized + Slot>(ctx: *const c_void) -> *const c_void {
    let arc = ManuallyDrop::new(unsafe { Arc::from_raw(ctx.cast::<Core<H>>()) });
    Weak::into_raw(Arc::downgrade(&arc)).cast()
}

unsafe extern "C" fn host_weak_release<H: ?Sized + Slot>(weak: *const c_void) {
    drop(unsafe { Weak::from_raw(weak.cast::<Core<H>>()) })
}

unsafe extern "C" fn host_unsubscribe_weak<H: ?Sized + Slot>(
    weak: *const c_void,
    token: u64,
) -> u32 {
    let weak = ManuallyDrop::new(unsafe { Weak::from_raw(weak.cast::<Core<H>>()) });
    match weak.upgrade() {
        Some(core) => core.remove(token) as u32,
        None => 0,
    }
}

unsafe extern "C" fn host_subscribe<H: ?Sized + Slot>(
    ctx: *const c_void,
    id: u64,
    channel: RawChannel,
    priority: i32,
    handler: RawHandler,
) -> u64 {
    let valid = handler.kind == HANDLER_DYN
        || (handler.kind == HANDLER_TYPED && handler.align.is_power_of_two());
    let handler = ForeignHandler(handler);
    let (Some(channel), true) = (channel.to_channel(), valid) else {
        return 0; // `handler` is dropped here, through its own drop fn.
    };
    let core = unsafe { core_ref::<H>(ctx) };
    let token = core.next_token();
    core.insert(id, channel, H::foreign(token, priority, handler));
    token
}

/// A decoded foreign event, ready to dispatch or queue.
enum Incoming {
    Typed {
        ptr: *const u8,
        size: usize,
        align: usize,
        to_dyn: Option<RawToDynFn>,
    },
    Dyn(DynEvent),
}

unsafe fn incoming<H: ?Sized + Slot>(
    core: &Core<H>,
    ev: &RawEventRef,
) -> Result<(Channel, Incoming), i32> {
    let channel = ev.channel.to_channel().ok_or(STATUS_BAD_CHANNEL)?;
    match ev.kind {
        EVENT_TYPED => {
            // Handlers turn `data` into a reference, which must be non-null
            // and aligned even for zero-sized events.
            if !ev.align.is_power_of_two()
                || ev.data.is_null()
                || (ev.data as usize) % ev.align != 0
            {
                return Err(STATUS_BAD_LAYOUT);
            }
            Ok((
                channel,
                Incoming::Typed {
                    ptr: ev.data,
                    size: ev.len,
                    align: ev.align,
                    to_dyn: ev.to_dyn,
                },
            ))
        }
        EVENT_DYN => {
            let bytes = unsafe { byte_slice(ev.data, ev.len) };
            let d = DynEvent::decode(bytes).map_err(|_| STATUS_DECODE)?;
            if d.id != ev.id {
                return Err(STATUS_DECODE);
            }
            core.check_dyn(&d).map_err(|e| match e {
                crate::DynEventError::UnknownEvent(_) => STATUS_UNKNOWN_EVENT,
                _ => STATUS_BAD_FIELDS,
            })?;
            Ok((channel, Incoming::Dyn(d)))
        }
        _ => Err(STATUS_BAD_LAYOUT),
    }
}

unsafe extern "C" fn host_publish<H: ?Sized + Slot>(
    ctx: *const c_void,
    ev: *const RawEventRef,
) -> i32 {
    let core = unsafe { core_ref::<H>(ctx) };
    let ev = unsafe { &*ev };
    match unsafe { incoming(core, ev) } {
        Ok((
            channel,
            Incoming::Typed {
                ptr,
                size,
                align,
                to_dyn,
            },
        )) => {
            let to_dyn = to_dyn.map_or(ToDyn::None, ToDyn::Foreign);
            core.dispatch(ev.id, channel, || {
                EventView::raw_typed(ev.id, channel, ptr, size, align, &to_dyn)
            });
            STATUS_OK
        }
        Ok((channel, Incoming::Dyn(d))) => {
            core.dispatch(d.id, channel, || EventView::dynamic(channel, &d));
            STATUS_OK
        }
        Err(status) => status,
    }
}

unsafe extern "C" fn host_publish_deferred<H: ?Sized + Slot>(
    ctx: *const c_void,
    ev: *const RawEventRef,
    drop_fn: Option<RawDropFn>,
) -> i32 {
    let core = unsafe { core_ref::<H>(ctx) };
    let ev = unsafe { &*ev };
    let (channel, payload) = match unsafe { incoming(core, ev) } {
        Ok((
            channel,
            Incoming::Typed {
                ptr,
                size,
                align,
                to_dyn,
            },
        )) => match unsafe { OwnedTyped::from_foreign(ptr, size, align, drop_fn, to_dyn) } {
            Some(value) => (channel, OwnedPayload::Typed { id: ev.id, value }),
            None => return STATUS_BAD_LAYOUT,
        },
        Ok((channel, Incoming::Dyn(d))) => (channel, OwnedPayload::Dyn(d)),
        Err(status) => return status,
    };
    core.enqueue(Queued { channel, payload });
    STATUS_OK
}

unsafe extern "C" fn host_register<H: ?Sized + Slot>(
    ctx: *const c_void,
    data: *const u8,
    len: usize,
) -> i32 {
    let core = unsafe { core_ref::<H>(ctx) };
    let Ok(desc) = EventDescriptor::decode(unsafe { byte_slice(data, len) }) else {
        return STATUS_DECODE;
    };
    match core.register(desc) {
        Ok(_) => STATUS_OK,
        Err(_) => STATUS_REGISTRY,
    }
}

unsafe fn byte_slice<'a>(data: *const u8, len: usize) -> &'a [u8] {
    if len == 0 || data.is_null() {
        &[]
    } else {
        // SAFETY: the caller passes `len` readable bytes.
        unsafe { std::slice::from_raw_parts(data, len) }
    }
}

// ---------------------------------------------------------------------------
// Plugin side
// ---------------------------------------------------------------------------

/// Errors from a [`ForeignBus`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForeignError {
    /// The [`RawBus`] has a different [`ABI_VERSION`] or struct size.
    AbiMismatch {
        /// Version reported by the host.
        host_version: u32,
    },
    /// The host refused the call with this status (`STATUS_*`).
    Status(i32),
}

impl fmt::Display for ForeignError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AbiMismatch { host_version } => {
                write!(
                    f,
                    "gamma ABI mismatch: host {host_version}, plugin {ABI_VERSION}"
                )
            }
            Self::Status(s) => write!(f, "gamma host returned status {s}"),
        }
    }
}
impl std::error::Error for ForeignError {}

fn status(s: i32) -> Result<(), ForeignError> {
    if s == STATUS_OK {
        Ok(())
    } else {
        Err(ForeignError::Status(s))
    }
}

/// A plugin's view of a host bus, built from a [`RawBus`].
///
/// All calls go through the host's `extern "C"` functions; this type (and
/// everything it creates) belongs to the calling library.
pub struct ForeignBus {
    raw: RawBus,
}

// SAFETY: the `from_raw` contract restricts non-thread-safe buses to the
// host's bus thread.
unsafe impl Send for ForeignBus {}
unsafe impl Sync for ForeignBus {}

impl ForeignBus {
    /// Take ownership of an exported bus (and its strong reference).
    ///
    /// # Safety
    /// `raw` must come from `export_raw` (possibly through another library)
    /// and its reference must not be released elsewhere. If
    /// [`is_thread_safe`](Self::is_thread_safe) is false, the bus and
    /// everything created from it must only be used on the host's bus
    /// thread. The host library must stay loaded while this exists.
    pub unsafe fn from_raw(raw: RawBus) -> Result<Self, ForeignError> {
        // Only the first two fields are read before the check; on a mismatch
        // the reference is leaked since the other fields cannot be trusted.
        if raw.abi_version != ABI_VERSION || raw.struct_size != size_of::<RawBus>() as u32 {
            return Err(ForeignError::AbiMismatch {
                host_version: raw.abi_version,
            });
        }
        Ok(Self { raw })
    }

    /// Give the strong reference back as a [`RawBus`].
    pub fn into_raw(self) -> RawBus {
        let me = ManuallyDrop::new(self);
        me.raw
    }

    /// Whether the host bus is a [`SyncEventBus`](crate::SyncEventBus).
    pub fn is_thread_safe(&self) -> bool {
        self.raw.flags & FLAG_THREAD_SAFE != 0
    }

    fn subscribe_raw(
        &self,
        id: u64,
        opts: SubscribeOptions,
        handler: RawHandler,
    ) -> ForeignSubscription {
        // SAFETY: valid ctx (we hold a strong ref); the handler's functions
        // are this library's.
        let token = unsafe {
            (self.raw.subscribe)(
                self.raw.ctx,
                id,
                opts.channel.into(),
                opts.priority,
                handler,
            )
        };
        let weak = if token == 0 {
            std::ptr::null()
        } else {
            unsafe { (self.raw.downgrade)(self.raw.ctx) }
        };
        ForeignSubscription {
            weak,
            token,
            unsubscribe_weak: self.raw.unsubscribe_weak,
            weak_release: self.raw.weak_release,
        }
    }

    /// Subscribe to `T` on the global channel.
    pub fn subscribe<T: Event, F: Fn(&T) + Send + Sync + 'static>(
        &self,
        handler: F,
    ) -> ForeignSubscription {
        self.subscribe_with(SubscribeOptions::default(), handler)
    }

    /// Subscribe to `T` with a channel and priority.
    pub fn subscribe_with<T: Event, F: Fn(&T) + Send + Sync + 'static>(
        &self,
        opts: SubscribeOptions,
        handler: F,
    ) -> ForeignSubscription {
        let raw = RawHandler {
            data: Box::into_raw(Box::new(handler)).cast(),
            call: typed_call::<T, F>,
            drop: drop_box::<F>,
            kind: HANDLER_TYPED,
            accepts_dyn: T::REFLECTED as u32,
            size: size_of::<T>(),
            align: align_of::<T>(),
        };
        self.subscribe_raw(T::stable_type_id(), opts, raw)
    }

    /// Subscribe to event `id` as a [`DynEvent`].
    pub fn subscribe_dyn<F: Fn(&DynEvent) + Send + Sync + 'static>(
        &self,
        id: u64,
        opts: SubscribeOptions,
        handler: F,
    ) -> ForeignSubscription {
        let raw = RawHandler {
            data: Box::into_raw(Box::new(handler)).cast(),
            call: dyn_call::<F>,
            drop: drop_box::<F>,
            kind: HANDLER_DYN,
            accepts_dyn: 1,
            size: 0,
            align: 1,
        };
        self.subscribe_raw(id, opts, raw)
    }

    fn typed_ref<T: Event>(channel: Channel, event: &T) -> RawEventRef {
        RawEventRef {
            id: T::stable_type_id(),
            channel: channel.into(),
            kind: EVENT_TYPED,
            data: (event as *const T).cast(),
            len: size_of::<T>(),
            align: align_of::<T>(),
            to_dyn: if T::REFLECTED {
                Some(to_dyn_call::<T>)
            } else {
                None
            },
        }
    }

    /// Publish `event` on the global channel now.
    pub fn publish<T: Event>(&self, event: T) {
        self.publish_to(Channel::Global, event)
    }

    /// Publish `event` on `channel` now. `event` is dropped here afterwards.
    pub fn publish_to<T: Event>(&self, channel: Channel, event: T) {
        let raw = Self::typed_ref(channel, &event);
        // A typed event with a valid layout and channel cannot be refused.
        let _ = unsafe { (self.raw.publish)(self.raw.ctx, &raw) };
    }

    /// Queue `event` on `channel` for the host's next flush. On success the
    /// host owns it and drops it through this library's drop glue.
    pub fn publish_deferred_to<T: Event + Send>(
        &self,
        channel: Channel,
        event: T,
    ) -> Result<(), ForeignError> {
        let event = ManuallyDrop::new(event);
        let raw = Self::typed_ref(channel, &*event);
        let drop_fn: Option<RawDropFn> = if std::mem::needs_drop::<T>() {
            Some(drop_in_place_call::<T>)
        } else {
            None
        };
        let s = unsafe { (self.raw.publish_deferred)(self.raw.ctx, &raw, drop_fn) };
        if s != STATUS_OK {
            drop(ManuallyDrop::into_inner(event));
        }
        status(s)
    }

    /// [`publish_deferred_to`](Self::publish_deferred_to) on the global channel.
    pub fn publish_deferred<T: Event + Send>(&self, event: T) -> Result<(), ForeignError> {
        self.publish_deferred_to(Channel::Global, event)
    }

    fn dyn_ref(channel: Channel, event: &DynEvent, bytes: &[u8]) -> RawEventRef {
        RawEventRef {
            id: event.id,
            channel: channel.into(),
            kind: EVENT_DYN,
            data: bytes.as_ptr(),
            len: bytes.len(),
            align: 1,
            to_dyn: None,
        }
    }

    /// Publish a dynamic event now (its descriptor must be registered).
    pub fn publish_dyn(&self, channel: Channel, event: &DynEvent) -> Result<(), ForeignError> {
        let bytes = event.to_bytes();
        let raw = Self::dyn_ref(channel, event, &bytes);
        status(unsafe { (self.raw.publish)(self.raw.ctx, &raw) })
    }

    /// Queue a dynamic event for the host's next flush.
    pub fn publish_dyn_deferred(
        &self,
        channel: Channel,
        event: &DynEvent,
    ) -> Result<(), ForeignError> {
        let bytes = event.to_bytes();
        let raw = Self::dyn_ref(channel, event, &bytes);
        status(unsafe { (self.raw.publish_deferred)(self.raw.ctx, &raw, None) })
    }

    /// Register a descriptor on the host bus.
    pub fn register_descriptor(&self, descriptor: &EventDescriptor) -> Result<(), ForeignError> {
        let mut bytes = Vec::new();
        descriptor.encode(&mut bytes);
        status(unsafe { (self.raw.register_descriptor)(self.raw.ctx, bytes.as_ptr(), bytes.len()) })
    }

    /// Register the descriptor of a `#[pulsar_event(dynamic)]` type.
    pub fn register_event<T: Event>(&self) -> Result<(), ForeignError> {
        match T::descriptor() {
            Some(d) => self.register_descriptor(&d),
            None => Err(ForeignError::Status(STATUS_REGISTRY)),
        }
    }
}

impl Clone for ForeignBus {
    fn clone(&self) -> Self {
        unsafe { (self.raw.retain)(self.raw.ctx) };
        Self { raw: self.raw }
    }
}

impl Drop for ForeignBus {
    fn drop(&mut self) {
        unsafe { (self.raw.release)(self.raw.ctx) }
    }
}

/// A plugin-side subscription handle. Unsubscribes on drop; holds only a
/// weak reference to the bus.
#[must_use = "dropping a subscription unsubscribes it; call .detach() to keep the handler for the bus's lifetime"]
pub struct ForeignSubscription {
    weak: *const c_void,
    token: u64,
    unsubscribe_weak: unsafe extern "C" fn(*const c_void, u64) -> u32,
    weak_release: unsafe extern "C" fn(*const c_void),
}

// SAFETY: see `ForeignBus`.
unsafe impl Send for ForeignSubscription {}
unsafe impl Sync for ForeignSubscription {}

impl ForeignSubscription {
    /// `false` if the host refused the subscription.
    pub fn is_valid(&self) -> bool {
        self.token != 0
    }

    /// The subscription token (0 if refused).
    pub fn token(&self) -> u64 {
        self.token
    }

    /// Remove the handler now.
    pub fn unsubscribe(self) {}

    /// Keep the handler for the bus's lifetime and discard the handle.
    pub fn detach(mut self) {
        self.release(false);
    }

    fn release(&mut self, unsubscribe: bool) {
        if self.weak.is_null() {
            return;
        }
        unsafe {
            if unsubscribe {
                (self.unsubscribe_weak)(self.weak, self.token);
            }
            (self.weak_release)(self.weak);
        }
        self.weak = std::ptr::null();
    }
}

impl Drop for ForeignSubscription {
    fn drop(&mut self) {
        self.release(true);
    }
}

// --- trampolines (compiled into the subscriber's / publisher's library) ----

unsafe extern "C" fn typed_call<T: Event, F: Fn(&T)>(data: *mut c_void, ev: *const RawEventRef) {
    // SAFETY: `data` is the `Box<F>` from `subscribe_with`; `ev` is valid for
    // the call.
    let (f, ev) = unsafe { (&*data.cast::<F>(), &*ev) };
    if ev.id != T::stable_type_id() {
        return;
    }
    match ev.kind {
        EVENT_TYPED if ev.len == size_of::<T>() && ev.align == align_of::<T>() => {
            // SAFETY: id (name, layout, schema), size and alignment match and
            // the type is `#[repr(C)]`; `data` is live for the call.
            f(unsafe { &*ev.data.cast::<T>() })
        }
        EVENT_DYN if T::REFLECTED => {
            if let Some(v) = DynEvent::decode(unsafe { byte_slice(ev.data, ev.len) })
                .ok()
                .and_then(|d| T::from_dyn(&d))
            {
                f(&v)
            }
        }
        _ => {}
    }
}

unsafe extern "C" fn dyn_call<F: Fn(&DynEvent)>(data: *mut c_void, ev: *const RawEventRef) {
    let (f, ev) = unsafe { (&*data.cast::<F>(), &*ev) };
    if ev.kind == EVENT_DYN {
        if let Ok(d) = DynEvent::decode(unsafe { byte_slice(ev.data, ev.len) }) {
            f(&d)
        }
    }
}

unsafe extern "C" fn drop_box<F>(data: *mut c_void) {
    // SAFETY: `data` came from `Box::into_raw(Box<F>)` in this library.
    drop(unsafe { Box::from_raw(data.cast::<F>()) })
}

unsafe extern "C" fn to_dyn_call<T: Event>(event: *const u8, sink: *mut RawSink) {
    let (event, sink) = unsafe { (&*event.cast::<T>(), &*sink) };
    if let Some(d) = event.to_dyn() {
        let bytes = d.to_bytes();
        unsafe { (sink.write)(sink.ctx, bytes.as_ptr(), bytes.len()) }
    }
}

unsafe extern "C" fn drop_in_place_call<T>(event: *mut u8) {
    unsafe { std::ptr::drop_in_place(event.cast::<T>()) }
}
