//! Event namespaces: isolation between namespaces, the implicit main
//! namespace, and its protection from explicit naming.

use std::cell::Cell;
use std::rc::Rc;

use gamma_core::ffi::ForeignBus;
use gamma_core::{
    Channel, DynEvent, DynValue, Event, EventBus, EventDescriptor, FieldType, RegistryError,
    SubscribeOptions, SyncEventBus,
};
use gamma_derive::pulsar_event;

#[pulsar_event(dynamic, name = "Hit")]
struct MainHit {
    amount: u64,
}

#[pulsar_event(dynamic, name = "Hit", namespace = "engine")]
struct EngineHit {
    amount: u64,
}

#[pulsar_event(dynamic, name = "Hit", namespace = "physics")]
struct PhysicsHit {
    amount: u64,
}

#[pulsar_event(namespace = "engine")]
struct Plain(u32);

#[pulsar_event]
struct PlainMain(u32);

fn counter() -> Rc<Cell<u32>> {
    Rc::new(Cell::new(0))
}

#[test]
fn same_name_different_namespace_is_a_different_event() {
    let ids = [
        MainHit::stable_type_id(),
        EngineHit::stable_type_id(),
        PhysicsHit::stable_type_id(),
    ];
    assert_ne!(ids[0], ids[1]);
    assert_ne!(ids[1], ids[2]);
    assert_ne!(ids[0], ids[2]);
    assert_ne!(Plain::stable_type_id(), PlainMain::stable_type_id());

    let bus = EventBus::new();
    let (main, engine) = (counter(), counter());
    let (m, e) = (main.clone(), engine.clone());
    bus.subscribe(move |_: &MainHit| m.set(m.get() + 1)).detach();
    bus.subscribe(move |_: &EngineHit| e.set(e.get() + 1))
        .detach();

    bus.publish(EngineHit { amount: 1 });
    assert_eq!((main.get(), engine.get()), (0, 1));
    bus.publish(MainHit { amount: 1 });
    assert_eq!((main.get(), engine.get()), (1, 1));
}

#[test]
fn registry_is_per_namespace() {
    let bus = EventBus::new();
    bus.register_event::<MainHit>().unwrap();
    bus.register_event::<EngineHit>().unwrap();
    bus.register_event::<PhysicsHit>().unwrap();

    assert_eq!(bus.descriptor_by_name("Hit").unwrap().namespace, None);
    assert_eq!(
        bus.descriptor_by_name_in("engine", "Hit").unwrap().id,
        EngineHit::stable_type_id()
    );
    assert_eq!(
        bus.descriptor_by_name_in("physics", "Hit").unwrap().id,
        PhysicsHit::stable_type_id()
    );
    assert_eq!(bus.namespaces(), ["engine", "physics"]);
    // Plain listing is main-only; namespaced events don't leak into it.
    assert_eq!(bus.descriptors().len(), 1);
    assert_eq!(bus.descriptors_in("engine").len(), 1);
    assert_eq!(bus.all_descriptors().len(), 3);
}

#[test]
fn main_namespace_is_not_reachable_by_name() {
    let bus = EventBus::new();
    bus.register_event::<MainHit>().unwrap();

    for ns in ["main", ""] {
        assert!(bus.descriptor_by_name_in(ns, "Hit").is_none());
        assert!(bus.descriptors_in(ns).is_empty());
        assert_eq!(
            EventDescriptor::dynamic_in(ns, "Hit", [("amount", FieldType::U64)]),
            Err(RegistryError::ReservedNamespace(ns.into()))
        );
        // Hand-built or decoded descriptors are refused at registration.
        let forged = EventDescriptor::new_in(
            1,
            ns,
            "Forged",
            vec![("amount".into(), FieldType::U64)],
        );
        assert_eq!(
            bus.register_descriptor(forged),
            Err(RegistryError::ReservedNamespace(ns.into()))
        );
    }
    assert_eq!(bus.descriptors().len(), 1);
}

#[test]
fn dynamic_events_are_isolated() {
    let bus = EventBus::new();
    let main = EventDescriptor::dynamic("Spawn", [("n", FieldType::U64)]);
    let engine = EventDescriptor::dynamic_in("engine", "Spawn", [("n", FieldType::U64)]).unwrap();
    let plugin = EventDescriptor::dynamic_in("plugin", "Spawn", [("n", FieldType::U64)]).unwrap();
    // Same name and fields everywhere, three distinct events.
    bus.register_descriptor(main.clone()).unwrap();
    bus.register_descriptor(engine.clone()).unwrap();
    bus.register_descriptor(plugin.clone()).unwrap();
    // Re-registering is idempotent.
    bus.register_descriptor(engine.clone()).unwrap();

    let hits = Rc::new(Cell::new([0u32; 3]));
    for (i, d) in [&main, &engine, &plugin].into_iter().enumerate() {
        let hits = hits.clone();
        bus.subscribe_dyn(d.id, SubscribeOptions::default(), move |_| {
            let mut h = hits.get();
            h[i] += 1;
            hits.set(h);
        })
        .detach();
    }
    bus.publish_dyn(Channel::Global, &DynEvent::new(engine.id, vec![DynValue::U64(1)]))
        .unwrap();
    assert_eq!(hits.get(), [0, 1, 0]);
    bus.publish_dyn(Channel::Global, &DynEvent::new(main.id, vec![DynValue::U64(1)]))
        .unwrap();
    assert_eq!(hits.get(), [1, 1, 0]);
}

#[test]
fn names_are_unique_within_a_namespace() {
    let from_rust = EngineHit::descriptor().unwrap();
    let from_script =
        EventDescriptor::dynamic_in("engine", "Hit", [("amount", FieldType::U64)]).unwrap();
    assert_eq!(from_rust.namespace.as_deref(), Some("engine"));
    let bus = EventBus::new();
    bus.register_descriptor(from_rust).unwrap();
    // Same as in the main namespace: one name, one event.
    assert!(matches!(
        bus.register_descriptor(from_script),
        Err(RegistryError::NameConflict { .. })
    ));
    // The same name in another namespace is fine.
    bus.register_event::<PhysicsHit>().unwrap();
}

#[test]
fn namespaced_descriptors_cross_the_plugin_boundary() {
    let host = SyncEventBus::new();
    let plugin_side = unsafe { ForeignBus::from_raw(host.export_raw()) }.unwrap();
    let d = EventDescriptor::dynamic_in("engine", "Tick", [("dt", FieldType::F64)]).unwrap();
    plugin_side.register_descriptor(&d).unwrap();
    assert_eq!(host.descriptor_by_name_in("engine", "Tick").unwrap().id, d.id);
    assert!(host.descriptor_by_name("Tick").is_none());

    let forged = EventDescriptor::new_in(5, "main", "X", vec![]);
    assert!(plugin_side.register_descriptor(&forged).is_err());
}
