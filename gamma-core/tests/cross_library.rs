//! Cross-library delivery through a separately compiled `cdylib`.
//!
//! `tests/fixtures/plugin` is built here in its own cargo invocation, with
//! the release profile and its own target directory, so it contains its own
//! copy of gamma-core compiled independently of this test. Its event types
//! are declared in its own crate: an `Any::downcast_ref` based bus would
//! drop every one of these events, since the `TypeId`s differ.

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use gamma_core::ffi::RawBus;
use gamma_core::{
    Channel, DynEvent, DynValue, Event, EventBus, EventDescriptor, FieldType, SubscribeOptions,
    SyncEventBus,
};
use gamma_derive::pulsar_event;
use libloading::{Library, Symbol};

#[pulsar_event]
struct Ping {
    value: u64,
}

#[pulsar_event(dynamic)]
#[derive(Debug, Clone, PartialEq)]
struct Hit {
    target: u64,
    damage: f64,
    critical: bool,
}

/// Same name and layout as the plugin's `Tracked` (whose extra field is a
/// zero-sized drop guard).
#[pulsar_event]
struct Tracked {
    value: u64,
}

fn build_plugin() -> PathBuf {
    let manifest =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/plugin/Cargo.toml");
    let target_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("gamma-test-plugin");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .args(["build", "--release", "--quiet", "--manifest-path"])
        .arg(&manifest)
        .arg("--target-dir")
        .arg(&target_dir)
        // Do not inherit the outer build's flags (e.g. coverage, features).
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .status()
        .expect("failed to run cargo for the test plugin");
    assert!(status.success(), "building the test plugin failed");
    target_dir
        .join("release")
        .join(libloading::library_filename("gamma_test_plugin"))
}

struct Plugin {
    lib: Library,
}

impl Plugin {
    fn load() -> &'static Mutex<Plugin> {
        static PLUGIN: OnceLock<Mutex<Plugin>> = OnceLock::new();
        PLUGIN.get_or_init(|| {
            let path = build_plugin();
            // SAFETY: our own test library; it has no load-time side effects.
            let lib = unsafe { Library::new(&path) }
                .unwrap_or_else(|e| panic!("loading {}: {e}", path.display()));
            Mutex::new(Plugin { lib })
        })
    }

    fn sym<T>(&self, name: &str) -> Symbol<'_, T> {
        // SAFETY: the signatures below match the plugin's exports.
        unsafe { self.lib.get(name.as_bytes()) }.unwrap()
    }

    fn init(&self, bus: RawBus) -> u32 {
        unsafe { self.sym::<unsafe extern "C" fn(RawBus) -> u32>("plugin_init")(bus) }
    }
    fn call0(&self, name: &str) {
        self.sym::<extern "C" fn()>(name)()
    }
    fn call_i32(&self, name: &str) -> i32 {
        self.sym::<extern "C" fn() -> i32>(name)()
    }
    fn call1(&self, name: &str, a: u64) -> i32 {
        self.sym::<extern "C" fn(u64) -> i32>(name)(a)
    }
    fn publish_ping(&self, v: u64) {
        self.sym::<extern "C" fn(u64)>("plugin_publish_ping")(v)
    }
    fn publish_hit(&self, target: u64, entity: u64) {
        self.sym::<extern "C" fn(u64, u64)>("plugin_publish_hit")(target, entity)
    }
    fn take(&self, which: u32) -> u64 {
        self.sym::<extern "C" fn(u32) -> u64>("plugin_take_counter")(which)
    }
}

const PING: u32 = 0;
const HIT_TYPED: u32 = 1;
const HIT_DYN: u32 = 2;
const SCRIPT: u32 = 3;
const HANDLER_DROPS: u32 = 4;
const TRACKED_DROPS: u32 = 5;

/// The host API used by the scenario, implemented by both bus flavours.
trait Host {
    fn export(&self) -> RawBus;
    fn on_ping(&self, f: impl Fn(u64) + Send + Sync + 'static) -> Box<dyn std::any::Any>;
    fn on_hit(
        &self,
        ch: Channel,
        f: impl Fn(&Hit) + Send + Sync + 'static,
    ) -> Box<dyn std::any::Any>;
    fn on_hit_dyn(&self, f: impl Fn(&DynEvent) + Send + Sync + 'static) -> Box<dyn std::any::Any>;
    fn on_tracked(&self, f: impl Fn(u64) + Send + Sync + 'static) -> Box<dyn std::any::Any>;
    fn ping(&self, v: u64);
    fn hit(&self, ch: Channel, h: Hit);
    fn publish_dyn(&self, e: &DynEvent);
    fn flush(&self) -> usize;
    fn count(&self, id: u64, ch: Channel) -> usize;
    fn descriptor_by_name(&self, n: &str) -> Option<Arc<EventDescriptor>>;
}

macro_rules! host_impl {
    ($Bus:ty) => {
        impl Host for $Bus {
            fn export(&self) -> RawBus {
                self.export_raw()
            }
            fn on_ping(&self, f: impl Fn(u64) + Send + Sync + 'static) -> Box<dyn std::any::Any> {
                Box::new(self.subscribe(move |p: &Ping| f(p.value)))
            }
            fn on_hit(
                &self,
                ch: Channel,
                f: impl Fn(&Hit) + Send + Sync + 'static,
            ) -> Box<dyn std::any::Any> {
                Box::new(self.subscribe_with(SubscribeOptions::channel(ch), f))
            }
            fn on_hit_dyn(
                &self,
                f: impl Fn(&DynEvent) + Send + Sync + 'static,
            ) -> Box<dyn std::any::Any> {
                Box::new(self.subscribe_dyn(Hit::stable_type_id(), SubscribeOptions::default(), f))
            }
            fn on_tracked(
                &self,
                f: impl Fn(u64) + Send + Sync + 'static,
            ) -> Box<dyn std::any::Any> {
                Box::new(self.subscribe(move |t: &Tracked| f(t.value)))
            }
            fn ping(&self, v: u64) {
                self.publish(Ping { value: v })
            }
            fn hit(&self, ch: Channel, h: Hit) {
                self.publish_to(ch, h)
            }
            fn publish_dyn(&self, e: &DynEvent) {
                <$Bus>::publish_dyn(self, Channel::Global, e).unwrap()
            }
            fn flush(&self) -> usize {
                <$Bus>::flush(self).delivered
            }
            fn count(&self, id: u64, ch: Channel) -> usize {
                self.subscriber_count(id, ch)
            }
            fn descriptor_by_name(&self, n: &str) -> Option<Arc<EventDescriptor>> {
                <$Bus>::descriptor_by_name(self, n)
            }
        }
    };
}
host_impl!(EventBus);
host_impl!(SyncEventBus);

fn hit(target: u64) -> Hit {
    Hit {
        target,
        damage: 1.5,
        critical: true,
    }
}

fn scenario(host: &impl Host) {
    let guard = Plugin::load().lock().unwrap_or_else(|e| e.into_inner());
    let plugin = &*guard;
    for c in 0..6 {
        plugin.take(c);
    }

    assert_eq!(plugin.init(host.export()), 0, "plugin_init failed");

    // The plugin's `Ping` is a different Rust type (different `TypeId`) with
    // the same stable id: delivery below cannot rely on `Any`.
    let host_type_id_hash = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::any::TypeId::of::<Ping>().hash(&mut h);
        h.finish()
    };
    let plugin_type_id_hash = plugin.sym::<extern "C" fn() -> u64>("plugin_ping_type_id_hash")();
    assert_ne!(host_type_id_hash, plugin_type_id_hash);

    // The plugin registered descriptors through the ABI.
    let d = host
        .descriptor_by_name("Hit")
        .expect("plugin registered Hit");
    assert_eq!(
        d.id,
        Hit::stable_type_id(),
        "same schema, same id across libraries"
    );
    let scored = host
        .descriptor_by_name("Plugin.Scored")
        .expect("plugin registered a runtime descriptor");
    assert_eq!(
        *scored,
        EventDescriptor::dynamic("Plugin.Scored", [("points", FieldType::U64)])
    );
    assert_eq!(host.count(Ping::stable_type_id(), Channel::Global), 1);

    let pings = Arc::new(AtomicU64::new(0));
    let hits = Arc::new(Mutex::new(Vec::new()));
    let dyn_hits = Arc::new(Mutex::new(Vec::new()));
    let tracked = Arc::new(AtomicU64::new(0));
    let (p, h, dh, t) = (
        Arc::clone(&pings),
        Arc::clone(&hits),
        Arc::clone(&dyn_hits),
        Arc::clone(&tracked),
    );
    let _subs = [
        host.on_ping(move |v| {
            p.fetch_add(v, Ordering::SeqCst);
        }),
        host.on_hit(Channel::Global, move |x| h.lock().unwrap().push(x.clone())),
        host.on_hit_dyn(move |d| dh.lock().unwrap().push(d.clone())),
        host.on_tracked(move |v| {
            t.fetch_add(v, Ordering::SeqCst);
        }),
    ];

    // 1. Plugin -> host, typed, immediate.
    plugin.publish_ping(5);
    assert_eq!(
        pings.load(Ordering::SeqCst),
        5,
        "host subscriber got the plugin's typed event"
    );
    assert_eq!(plugin.take(PING), 5, "plugin's own subscriber too");

    // 2. Host -> plugin, typed.
    host.ping(3);
    assert_eq!(plugin.take(PING), 3);

    // 3. Channels across the boundary: only Entity(7) reaches the plugin's
    //    typed Hit handler; global hits go to its dynamic handler.
    host.hit(Channel::Entity(7), hit(70));
    host.hit(Channel::Entity(8), hit(80));
    assert_eq!(plugin.take(HIT_TYPED), 70);
    assert_eq!(plugin.take(HIT_DYN), 0);
    host.hit(Channel::Global, hit(11));
    assert_eq!(
        plugin.take(HIT_DYN),
        11,
        "plugin dyn subscriber got a host typed event"
    );
    assert_eq!(plugin.take(HIT_TYPED), 0);
    hits.lock().unwrap().clear();
    dyn_hits.lock().unwrap().clear();

    // 4. Plugin typed Hit -> host typed and dynamic subscribers (the dynamic
    //    form is produced by the plugin's to_dyn, through a byte sink).
    plugin.publish_hit(21, 0);
    assert_eq!(*hits.lock().unwrap(), vec![hit(21)]);
    let expected = DynEvent::new(
        Hit::stable_type_id(),
        vec![DynValue::U64(21), DynValue::F64(1.5), DynValue::Bool(true)],
    );
    assert_eq!(*dyn_hits.lock().unwrap(), vec![expected]);
    plugin.take(HIT_DYN);

    // 5. Plugin dynamic Hit -> host typed subscriber.
    hits.lock().unwrap().clear();
    assert_eq!(plugin.call1("plugin_publish_hit_dyn", 33), 0);
    assert_eq!(*hits.lock().unwrap(), vec![hit(33)]);
    assert_eq!(
        plugin.call_i32("plugin_publish_bad_dyn"),
        -1,
        "host validates plugin dynamic events"
    );
    plugin.take(HIT_DYN);

    // 6. Host dynamic, script-declared event -> plugin dynamic subscriber.
    host.publish_dyn(&DynEvent::new(scored.id, vec![DynValue::U64(9)]));
    assert_eq!(plugin.take(SCRIPT), 9);

    // 7. Deferred plugin event: queued in the host, delivered at flush, then
    //    dropped by the plugin's drop glue.
    assert_eq!(plugin.call1("plugin_publish_tracked_deferred", 42), 0);
    assert_eq!(tracked.load(Ordering::SeqCst), 0);
    assert_eq!(
        plugin.take(TRACKED_DROPS),
        0,
        "ownership moved to the host queue"
    );
    assert_eq!(host.flush(), 1);
    assert_eq!(tracked.load(Ordering::SeqCst), 42);
    assert_eq!(plugin.take(TRACKED_DROPS), 1, "dropped once, in the plugin");

    // 8. Shutdown: the plugin's handlers are dropped by the plugin.
    assert_eq!(plugin.take(HANDLER_DROPS), 0);
    plugin.call0("plugin_shutdown");
    assert_eq!(plugin.take(HANDLER_DROPS), 4);
    assert_eq!(
        host.count(Ping::stable_type_id(), Channel::Global),
        1,
        "only the host's own subscriber is left"
    );
    assert_eq!(host.count(Hit::stable_type_id(), Channel::Entity(7)), 0);
    host.ping(1);
    assert_eq!(plugin.take(PING), 0);
}

#[test]
fn cross_library_with_sync_bus() {
    scenario(&SyncEventBus::new());
}

#[test]
fn cross_library_with_local_bus() {
    scenario(&EventBus::new());
}

#[test]
fn plugin_handlers_dropped_in_plugin_when_bus_is_dropped() {
    let guard = Plugin::load().lock().unwrap_or_else(|e| e.into_inner());
    let host = SyncEventBus::new();
    guard.take(HANDLER_DROPS);
    assert_eq!(guard.init(host.export_raw()), 0);
    // The plugin still holds its bus handle (a strong reference): dropping
    // the host's handle alone keeps the bus alive.
    drop(host);
    assert_eq!(guard.take(HANDLER_DROPS), 0);
    guard.publish_ping(1);
    assert_eq!(guard.take(PING), 1);
    // Releasing the plugin's handle drops the bus and with it the handlers,
    // each through the plugin's own drop function.
    guard.call0("plugin_shutdown");
    assert_eq!(guard.take(HANDLER_DROPS), 4);
}
