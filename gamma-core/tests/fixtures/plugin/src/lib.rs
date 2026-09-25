//! Test plugin for `tests/cross_library.rs`.
//!
//! Its event types are declared here, in a different crate from the host's
//! (so their `std::any::TypeId`s differ from the host's), with the same
//! names and layouts. Everything it does goes through `ForeignBus`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use gamma_core::ffi::{ForeignBus, ForeignSubscription, RawBus};
use gamma_core::{
    Channel, DynEvent, DynValue, Event, EventDescriptor, FieldType, SubscribeOptions,
};
use gamma_derive::pulsar_event;

#[pulsar_event]
pub struct Ping {
    pub value: u64,
}

#[pulsar_event(dynamic)]
pub struct Hit {
    pub target: u64,
    pub damage: f64,
    pub critical: bool,
}

/// Zero-sized, so `Tracked` has the host's layout, but it counts drops in
/// this library.
pub struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        TRACKED_DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

#[pulsar_event]
pub struct Tracked {
    pub value: u64,
    pub guard: Guard,
}

/// Counts drops of plugin handlers (they must run in this library).
struct HandlerGuard;
impl Drop for HandlerGuard {
    fn drop(&mut self) {
        HANDLER_DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

static PING_SUM: AtomicU64 = AtomicU64::new(0);
static HIT_TYPED: AtomicU64 = AtomicU64::new(0);
static HIT_DYN: AtomicU64 = AtomicU64::new(0);
static SCRIPT_SUM: AtomicU64 = AtomicU64::new(0);
static HANDLER_DROPS: AtomicU64 = AtomicU64::new(0);
static TRACKED_DROPS: AtomicU64 = AtomicU64::new(0);

struct State {
    bus: ForeignBus,
    // Held only to keep the subscriptions alive until shutdown.
    _subs: Vec<ForeignSubscription>,
}

static STATE: Mutex<Option<State>> = Mutex::new(None);

fn with_bus<R>(f: impl FnOnce(&ForeignBus) -> R) -> R {
    f(&STATE
        .lock()
        .unwrap()
        .as_ref()
        .expect("plugin not initialised")
        .bus)
}

fn script_event() -> EventDescriptor {
    EventDescriptor::dynamic("Plugin.Scored", [("points", FieldType::U64)])
}

/// # Safety
/// `bus` must be a fresh export from a host bus.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn plugin_init(bus: RawBus) -> u32 {
    let bus = match unsafe { ForeignBus::from_raw(bus) } {
        Ok(b) => b,
        Err(_) => return 1,
    };
    if bus.register_event::<Hit>().is_err() || bus.register_descriptor(&script_event()).is_err() {
        return 2;
    }
    let mut subs = Vec::new();
    let g = HandlerGuard;
    subs.push(bus.subscribe(move |p: &Ping| {
        let _ = &g;
        PING_SUM.fetch_add(p.value, Ordering::SeqCst);
    }));
    let g = HandlerGuard;
    subs.push(bus.subscribe_with(
        SubscribeOptions::channel(Channel::Entity(7)),
        move |h: &Hit| {
            let _ = &g;
            HIT_TYPED.fetch_add(h.target, Ordering::SeqCst);
        },
    ));
    let g = HandlerGuard;
    subs.push(bus.subscribe_dyn(
        Hit::stable_type_id(),
        SubscribeOptions::default(),
        move |d: &DynEvent| {
            let _ = &g;
            if let Some(DynValue::U64(t)) = d.field(0) {
                HIT_DYN.fetch_add(*t, Ordering::SeqCst);
            }
        },
    ));
    let g = HandlerGuard;
    subs.push(bus.subscribe_dyn(
        script_event().id,
        SubscribeOptions::default(),
        move |d: &DynEvent| {
            let _ = &g;
            if let Some(DynValue::U64(p)) = d.field(0) {
                SCRIPT_SUM.fetch_add(*p, Ordering::SeqCst);
            }
        },
    ));
    if subs.iter().any(|s| !s.is_valid()) {
        return 3;
    }
    *STATE.lock().unwrap() = Some(State { bus, _subs: subs });
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn plugin_publish_ping(value: u64) {
    with_bus(|b| b.publish(Ping { value }));
}

#[unsafe(no_mangle)]
pub extern "C" fn plugin_publish_hit(target: u64, entity: u64) {
    let channel = if entity == 0 {
        Channel::Global
    } else {
        Channel::Entity(entity)
    };
    with_bus(|b| {
        b.publish_to(
            channel,
            Hit {
                target,
                damage: 1.5,
                critical: true,
            },
        )
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn plugin_publish_hit_dyn(target: u64) -> i32 {
    let ev = DynEvent::new(
        Hit::stable_type_id(),
        vec![
            DynValue::U64(target),
            DynValue::F64(1.5),
            DynValue::Bool(true),
        ],
    );
    with_bus(|b| b.publish_dyn(Channel::Global, &ev)).map_or(-1, |_| 0)
}

#[unsafe(no_mangle)]
pub extern "C" fn plugin_publish_tracked_deferred(value: u64) -> i32 {
    with_bus(|b| {
        b.publish_deferred(Tracked {
            value,
            guard: Guard,
        })
    })
    .map_or(-1, |_| 0)
}

#[unsafe(no_mangle)]
pub extern "C" fn plugin_publish_bad_dyn() -> i32 {
    // Wrong arity for a registered descriptor: must be refused by the host.
    let ev = DynEvent::new(Hit::stable_type_id(), vec![DynValue::U64(1)]);
    with_bus(|b| b.publish_dyn(Channel::Global, &ev)).map_or(-1, |_| 0)
}

/// Drop all subscriptions and the bus handle.
#[unsafe(no_mangle)]
pub extern "C" fn plugin_shutdown() {
    let state = STATE.lock().unwrap().take();
    drop(state);
}

/// Read and reset one counter: 0 ping, 1 typed hit, 2 dyn hit, 3 script,
/// 4 handler drops, 5 tracked drops.
#[unsafe(no_mangle)]
pub extern "C" fn plugin_take_counter(which: u32) -> u64 {
    let c = match which {
        0 => &PING_SUM,
        1 => &HIT_TYPED,
        2 => &HIT_DYN,
        3 => &SCRIPT_SUM,
        4 => &HANDLER_DROPS,
        5 => &TRACKED_DROPS,
        _ => return u64::MAX,
    };
    c.swap(0, Ordering::SeqCst)
}

/// Hash of this library's `TypeId` for `Ping`, to show it differs from the
/// host's (so `Any::downcast_ref` could never match).
#[unsafe(no_mangle)]
pub extern "C" fn plugin_ping_type_id_hash() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    std::any::TypeId::of::<Ping>().hash(&mut h);
    h.finish()
}
