//! Gamma 0.2 behaviour: handles, deferred delivery, dynamic events,
//! channels, ordering and thread safety.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use gamma_core::ffi::ForeignBus;
use gamma_core::{
    Channel, DynEvent, DynEventError, DynValue, Event, EventBus, EventDescriptor, FieldType,
    RegistryError, SubscribeOptions, Subscription, SyncEventBus,
};
use gamma_derive::pulsar_event;

#[pulsar_event]
struct Ping(u32);

#[pulsar_event(dynamic)]
#[derive(Debug, Clone, PartialEq)]
struct Hit {
    target: u64,
    damage: f64,
    critical: bool,
}

#[pulsar_event(dynamic, name = "test.Named")]
#[derive(Debug, Clone, PartialEq)]
struct Named {
    label: String,
    data: Vec<u8>,
    delta: i32,
}

type Log<T> = Rc<RefCell<Vec<T>>>;

fn log<T>() -> Log<T> {
    Rc::new(RefCell::new(Vec::new()))
}

/// Counts drops of a value captured by a handler.
struct DropCounter(Rc<Cell<u32>>);
impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

// ---------------------------------------------------------------------------
// Subscription handles
// ---------------------------------------------------------------------------

#[test]
fn dropping_the_handle_unsubscribes() {
    let bus = EventBus::new();
    let hits = log();
    let h = Rc::clone(&hits);
    let sub = bus.subscribe(move |p: &Ping| h.borrow_mut().push(p.0));
    bus.publish(Ping(1));
    assert!(sub.is_active());
    drop(sub);
    bus.publish(Ping(2));
    assert_eq!(*hits.borrow(), vec![1]);
    assert_eq!(
        bus.subscriber_count(Ping::stable_type_id(), Channel::Global),
        0
    );
}

#[test]
fn explicit_unsubscribe_and_detach() {
    let bus = EventBus::new();
    let n = Rc::new(Cell::new(0));
    let (a, b) = (Rc::clone(&n), Rc::clone(&n));
    let sub = bus.subscribe(move |_: &Ping| a.set(a.get() + 1));
    bus.subscribe(move |_: &Ping| b.set(b.get() + 10)).detach();
    bus.publish(Ping(0));
    sub.unsubscribe();
    bus.publish(Ping(0));
    assert_eq!(n.get(), 1 + 10 + 10);
}

#[test]
fn handler_is_dropped_exactly_once() {
    let bus = EventBus::new();
    let drops = Rc::new(Cell::new(0));
    let guard = DropCounter(Rc::clone(&drops));
    let sub = bus.subscribe(move |_: &Ping| {
        let _ = &guard;
    });
    bus.publish(Ping(0));
    assert_eq!(drops.get(), 0);
    drop(sub);
    assert_eq!(drops.get(), 1);
    drop(bus);
    assert_eq!(drops.get(), 1);
}

#[test]
fn handle_outliving_bus_is_harmless() {
    let bus = EventBus::new();
    let sub = bus.subscribe(|_: &Ping| {});
    drop(bus);
    assert!(!sub.is_active());
    drop(sub);
}

#[test]
fn unsubscribe_self_inside_handler() {
    let bus = EventBus::new();
    let slot: Rc<RefCell<Option<Subscription>>> = Rc::new(RefCell::new(None));
    let calls = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));

    let (s, c, guard) = (
        Rc::clone(&slot),
        Rc::clone(&calls),
        DropCounter(Rc::clone(&drops)),
    );
    let d2 = Rc::clone(&drops);
    let sub = bus.subscribe(move |_: &Ping| {
        let _ = &guard;
        c.set(c.get() + 1);
        // Unsubscribe ourselves mid-dispatch: the closure we are running must
        // not be dropped until the dispatch loop ends.
        if let Some(me) = s.borrow_mut().take() {
            me.unsubscribe();
        }
        assert_eq!(d2.get(), 0, "handler dropped while running");
    });
    *slot.borrow_mut() = Some(sub);

    let after = Rc::new(Cell::new(0));
    let a = Rc::clone(&after);
    let _later = bus.subscribe(move |_: &Ping| a.set(a.get() + 1));

    bus.publish(Ping(0));
    assert_eq!(calls.get(), 1);
    assert_eq!(
        after.get(),
        1,
        "later subscribers still run in the same dispatch"
    );
    assert_eq!(drops.get(), 1, "handler dropped once the dispatch ended");

    bus.publish(Ping(0));
    assert_eq!(calls.get(), 1);
    assert_eq!(after.get(), 2);
}

#[test]
fn unsubscribe_a_later_handler_inside_dispatch_skips_it() {
    let bus = EventBus::new();
    let victim_slot: Rc<RefCell<Option<Subscription>>> = Rc::new(RefCell::new(None));
    let v = Rc::clone(&victim_slot);
    let _killer = bus.subscribe_with(SubscribeOptions::default().priority(10), move |_: &Ping| {
        v.borrow_mut().take();
    });
    let victim_calls = Rc::new(Cell::new(0));
    let vc = Rc::clone(&victim_calls);
    *victim_slot.borrow_mut() = Some(bus.subscribe(move |_: &Ping| vc.set(vc.get() + 1)));

    bus.publish(Ping(0));
    assert_eq!(victim_calls.get(), 0);
}

#[test]
fn subscribing_inside_a_handler_takes_effect_next_publish() {
    let bus = EventBus::new();
    let weak = bus.downgrade();
    let calls = Rc::new(Cell::new(0));
    let subs: Rc<RefCell<Vec<Subscription>>> = Rc::new(RefCell::new(Vec::new()));
    let (c, s) = (Rc::clone(&calls), Rc::clone(&subs));
    let _outer = bus.subscribe(move |_: &Ping| {
        let c = Rc::clone(&c);
        let bus = weak.upgrade().unwrap();
        s.borrow_mut()
            .push(bus.subscribe(move |_: &Ping| c.set(c.get() + 1)));
    });
    bus.publish(Ping(0));
    assert_eq!(calls.get(), 0);
    bus.publish(Ping(0));
    assert_eq!(calls.get(), 1);
}

// ---------------------------------------------------------------------------
// Deferred delivery
// ---------------------------------------------------------------------------

#[test]
fn deferred_events_wait_for_flush_and_keep_fifo_order() {
    let bus = EventBus::new();
    let seen = log();
    let s1 = Rc::clone(&seen);
    let _a = bus.subscribe(move |p: &Ping| s1.borrow_mut().push(format!("ping{}", p.0)));
    let s2 = Rc::clone(&seen);
    let _b = bus.subscribe(move |h: &Hit| s2.borrow_mut().push(format!("hit{}", h.target)));

    bus.publish_deferred(Ping(1));
    bus.publish_deferred(Hit {
        target: 2,
        damage: 0.0,
        critical: false,
    });
    bus.publish_deferred(Ping(3));
    assert!(seen.borrow().is_empty());
    assert_eq!(bus.queued_len(), 3);

    let r = bus.flush();
    assert_eq!(*seen.borrow(), vec!["ping1", "hit2", "ping3"]);
    assert_eq!(
        (r.delivered, r.rounds, r.hit_round_limit, r.remaining),
        (3, 1, false, 0)
    );
    assert_eq!(bus.flush().delivered, 0);
}

#[test]
fn events_queued_during_flush_are_delivered_in_later_rounds() {
    let bus = EventBus::new();
    let weak = bus.downgrade();
    let seen = log();
    let s = Rc::clone(&seen);
    let _chain = bus.subscribe(move |p: &Ping| {
        s.borrow_mut().push(p.0);
        if p.0 % 10 < 2 {
            weak.upgrade().unwrap().publish_deferred(Ping(p.0 + 1));
        }
    });
    bus.publish_deferred(Ping(0));
    bus.publish_deferred(Ping(10));
    let r = bus.flush();
    // Round 1: 0, 10. Round 2: 1, 11. Round 3: 2, 12.
    assert_eq!(*seen.borrow(), vec![0, 10, 1, 11, 2, 12]);
    assert_eq!((r.delivered, r.rounds, r.hit_round_limit), (6, 3, false));
}

#[test]
fn flush_round_limit_is_reported_and_rest_stays_queued() {
    let bus = EventBus::new();
    let weak = bus.downgrade();
    let _forever =
        bus.subscribe(move |p: &Ping| weak.upgrade().unwrap().publish_deferred(Ping(p.0 + 1)));
    bus.publish_deferred(Ping(0));
    let r = bus.flush_with_limit(4);
    assert_eq!(
        (r.delivered, r.rounds, r.hit_round_limit, r.remaining),
        (4, 4, true, 1)
    );
    bus.set_max_flush_rounds(2);
    let r = bus.flush();
    assert_eq!((r.rounds, r.hit_round_limit, r.remaining), (2, true, 1));
}

#[test]
fn nested_flush_is_a_noop() {
    let bus = EventBus::new();
    let weak = bus.downgrade();
    let nested = Rc::new(Cell::new(None));
    let n = Rc::clone(&nested);
    let _s = bus.subscribe(move |_: &Ping| n.set(Some(weak.upgrade().unwrap().flush())));
    bus.publish_deferred(Ping(0));
    bus.publish_deferred(Ping(1));
    let r = bus.flush();
    assert_eq!(r.delivered, 2);
    assert!(nested.get().unwrap().already_flushing);
}

#[test]
fn deferred_event_is_dropped_once_after_delivery() {
    #[pulsar_event]
    struct Owned(DropCounter);
    let bus = EventBus::new();
    let drops = Rc::new(Cell::new(0));
    let seen = Rc::new(Cell::new(false));
    let (d, s) = (Rc::clone(&drops), Rc::clone(&seen));
    let _sub = bus.subscribe(move |_: &Owned| {
        assert_eq!(d.get(), 0);
        s.set(true);
    });
    bus.publish_deferred(Owned(DropCounter(Rc::clone(&drops))));
    bus.flush();
    assert!(seen.get());
    assert_eq!(drops.get(), 1);

    // Undelivered events are dropped with the bus.
    bus.publish_deferred(Owned(DropCounter(Rc::clone(&drops))));
    drop(_sub);
    drop(bus);
    assert_eq!(drops.get(), 2);
}

#[test]
fn over_aligned_and_zero_sized_deferred_events() {
    #[pulsar_event]
    #[repr(align(64))]
    struct Aligned(u64);
    #[pulsar_event]
    struct Zst;
    let bus = EventBus::new();
    let got = Rc::new(Cell::new(0u64));
    let (g1, g2) = (Rc::clone(&got), Rc::clone(&got));
    let _a = bus.subscribe(move |a: &Aligned| {
        assert_eq!((a as *const Aligned as usize) % 64, 0);
        g1.set(g1.get() + a.0)
    });
    let _z = bus.subscribe(move |_: &Zst| g2.set(g2.get() + 1));
    bus.publish_deferred(Aligned(41));
    bus.publish_deferred(Zst);
    bus.flush();
    assert_eq!(got.get(), 42);
}

// ---------------------------------------------------------------------------
// Dynamic events
// ---------------------------------------------------------------------------

fn hit_dyn(target: u64) -> DynEvent {
    DynEvent::new(
        Hit::stable_type_id(),
        vec![
            DynValue::U64(target),
            DynValue::F64(2.5),
            DynValue::Bool(true),
        ],
    )
}

#[test]
fn derived_descriptor_matches_the_type() {
    let d = Hit::descriptor().unwrap();
    assert_eq!(d.id, Hit::stable_type_id());
    assert_eq!(d.name, "Hit");
    assert_eq!(
        d.fields,
        vec![
            ("target".into(), FieldType::U64),
            ("damage".into(), FieldType::F64),
            ("critical".into(), FieldType::Bool)
        ]
    );
    let h = Hit {
        target: 3,
        damage: 1.0,
        critical: false,
    };
    assert_eq!(Hit::from_dyn(&h.to_dyn().unwrap()), Some(h));
    assert_eq!(Named::descriptor().unwrap().name, "test.Named");
    assert!(!Ping::REFLECTED && Ping::descriptor().is_none());
}

#[test]
fn dynamic_subscriber_receives_rust_typed_event() {
    let bus = EventBus::new();
    let got = log();
    let g = Rc::clone(&got);
    let _s = bus.subscribe_dyn(
        Hit::stable_type_id(),
        SubscribeOptions::default(),
        move |d: &DynEvent| g.borrow_mut().push(d.clone()),
    );
    bus.publish(Hit {
        target: 9,
        damage: 2.5,
        critical: true,
    });
    bus.publish_deferred(Hit {
        target: 9,
        damage: 2.5,
        critical: true,
    });
    bus.flush();
    assert_eq!(*got.borrow(), vec![hit_dyn(9), hit_dyn(9)]);
}

#[test]
fn rust_subscriber_receives_matching_dynamic_event() {
    let bus = EventBus::new();
    bus.register_event::<Hit>().unwrap();
    let got = log();
    let g = Rc::clone(&got);
    let _s = bus.subscribe(move |h: &Hit| g.borrow_mut().push(h.clone()));
    bus.publish_dyn(Channel::Global, &hit_dyn(4)).unwrap();
    bus.publish_dyn_deferred(Channel::Global, hit_dyn(5))
        .unwrap();
    bus.flush();
    assert_eq!(
        *got.borrow(),
        vec![
            Hit {
                target: 4,
                damage: 2.5,
                critical: true
            },
            Hit {
                target: 5,
                damage: 2.5,
                critical: true
            }
        ]
    );
}

#[test]
fn strings_bytes_and_narrow_ints_roundtrip() {
    let bus = EventBus::new();
    bus.register_event::<Named>().unwrap();
    let got = log();
    let g = Rc::clone(&got);
    let _s = bus.subscribe(move |n: &Named| g.borrow_mut().push(n.clone()));
    let dynamic = bus.descriptor_by_name("test.Named").unwrap();
    let ev = DynEvent::new(
        dynamic.id,
        vec![
            DynValue::Str("hi".into()),
            DynValue::Bytes(vec![1, 2]),
            DynValue::I64(-3),
        ],
    );
    bus.publish_dyn(Channel::Global, &ev).unwrap();
    // Out of range for i32: rejected by from_dyn, so the typed subscriber is skipped.
    let bad = DynEvent::new(
        dynamic.id,
        vec![
            DynValue::Str("x".into()),
            DynValue::Bytes(vec![]),
            DynValue::I64(i64::MAX),
        ],
    );
    bus.publish_dyn(Channel::Global, &bad).unwrap();
    assert_eq!(
        *got.borrow(),
        vec![Named {
            label: "hi".into(),
            data: vec![1, 2],
            delta: -3
        }]
    );
}

#[test]
fn dynamic_publish_is_validated() {
    let bus = EventBus::new();
    assert_eq!(
        bus.publish_dyn(Channel::Global, &hit_dyn(1)),
        Err(DynEventError::UnknownEvent(Hit::stable_type_id()))
    );
    bus.register_event::<Hit>().unwrap();
    let wrong = DynEvent::new(Hit::stable_type_id(), vec![DynValue::U64(1)]);
    assert!(matches!(
        bus.publish_dyn(Channel::Global, &wrong),
        Err(DynEventError::Arity {
            expected: 3,
            got: 1
        })
    ));
    let wrong = DynEvent::new(
        Hit::stable_type_id(),
        vec![DynValue::I64(1), DynValue::F64(0.0), DynValue::Bool(false)],
    );
    assert!(matches!(
        bus.publish_dyn_deferred(Channel::Global, wrong),
        Err(DynEventError::FieldType { index: 0, .. })
    ));
    assert_eq!(bus.queued_len(), 0);
}

#[test]
fn script_declared_events() {
    let bus = EventBus::new();
    let desc = EventDescriptor::dynamic(
        "Door.Opened",
        [("door", FieldType::U64), ("by", FieldType::Str)],
    );
    let id = bus.register_descriptor(desc.clone()).unwrap();
    assert_eq!(id, desc.id);
    assert_eq!(bus.register_descriptor(desc.clone()), Ok(id), "idempotent");
    assert_eq!(
        bus.register_event::<Ping>(),
        Err(RegistryError::NotReflected)
    );

    let other = EventDescriptor::new(id, "Door.Closed", vec![]);
    assert!(matches!(
        bus.register_descriptor(other),
        Err(RegistryError::IdConflict { .. })
    ));
    let same_name = EventDescriptor::dynamic("Door.Opened", [("door", FieldType::U64)]);
    assert!(matches!(
        bus.register_descriptor(same_name),
        Err(RegistryError::NameConflict { .. })
    ));

    let got = log();
    let g = Rc::clone(&got);
    let _s = bus.subscribe_dyn(
        id,
        SubscribeOptions::channel(Channel::Entity(8)),
        move |d: &DynEvent| g.borrow_mut().push(d.fields[1].clone()),
    );
    let ev = DynEvent::new(id, vec![DynValue::U64(8), DynValue::Str("player".into())]);
    bus.publish_dyn(Channel::Entity(8), &ev).unwrap();
    bus.publish_dyn(Channel::Entity(9), &ev).unwrap();
    assert_eq!(*got.borrow(), vec![DynValue::Str("player".into())]);

    bus.register_event::<Hit>().unwrap();
    let names: Vec<String> = bus.descriptors().iter().map(|d| d.name.clone()).collect();
    assert_eq!(names, vec!["Door.Opened", "Hit"]);
}

#[test]
fn non_reflected_types_do_not_reach_dynamic_subscribers() {
    let bus = EventBus::new();
    let n = Rc::new(Cell::new(0));
    let c = Rc::clone(&n);
    let _s = bus.subscribe_dyn(
        Ping::stable_type_id(),
        SubscribeOptions::default(),
        move |_| c.set(c.get() + 1),
    );
    bus.publish(Ping(1));
    assert_eq!(n.get(), 0);
}

#[test]
fn to_dyn_runs_once_per_publish() {
    use std::sync::atomic::AtomicU32;
    static CONVERSIONS: AtomicU32 = AtomicU32::new(0);
    // Hand-written reflection, to count conversions.
    struct Manual(u64);
    impl Event for Manual {
        fn stable_type_id() -> u64 {
            77
        }
        const REFLECTED: bool = true;
        fn to_dyn(&self) -> Option<DynEvent> {
            CONVERSIONS.fetch_add(1, Ordering::Relaxed);
            Some(DynEvent::new(77, vec![DynValue::U64(self.0)]))
        }
    }
    let bus = EventBus::new();
    let subs: Vec<_> = (0..5)
        .map(|_| bus.subscribe_dyn(77, SubscribeOptions::default(), |_| {}))
        .collect();
    bus.publish(Manual(1));
    assert_eq!(CONVERSIONS.load(Ordering::Relaxed), 1);
    drop(subs);
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

#[test]
fn entity_channel_event_reaches_only_that_entity() {
    let bus = EventBus::new();
    let got = log();
    let mut subs = Vec::new();
    for (tag, ch) in [
        ("global", Channel::Global),
        ("e1", Channel::Entity(1)),
        ("e2", Channel::Entity(2)),
        ("class1", Channel::Class(1)),
    ] {
        let g = Rc::clone(&got);
        subs.push(
            bus.subscribe_with(SubscribeOptions::channel(ch), move |_: &Ping| {
                g.borrow_mut().push(tag)
            }),
        );
    }
    bus.publish_to(Channel::Entity(1), Ping(0));
    assert_eq!(
        *got.borrow(),
        vec!["e1"],
        "no fan-out to Global, other entities or classes"
    );
    got.borrow_mut().clear();

    bus.publish(Ping(0));
    bus.publish_to(Channel::Class(1), Ping(0));
    bus.publish_deferred_to(Channel::Entity(2), Ping(0));
    bus.flush();
    assert_eq!(*got.borrow(), vec!["global", "class1", "e2"]);
}

#[test]
fn clear_channel_removes_an_entitys_subscriptions() {
    let bus = EventBus::new();
    let a = bus.subscribe_with(SubscribeOptions::channel(Channel::Entity(5)), |_: &Ping| {
        panic!("cleared")
    });
    let b = bus.subscribe_dyn(
        Hit::stable_type_id(),
        SubscribeOptions::channel(Channel::Entity(5)),
        |_| panic!("cleared"),
    );
    let c = bus.subscribe_with(SubscribeOptions::channel(Channel::Entity(6)), |_: &Ping| {});
    assert_eq!(bus.clear_channel(Channel::Entity(5)), 2);
    assert!(!a.is_active() && !b.is_active() && c.is_active());
    bus.publish_to(Channel::Entity(5), Ping(0));
    bus.publish_to(
        Channel::Entity(5),
        Hit {
            target: 5,
            damage: 0.0,
            critical: false,
        },
    );
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

#[test]
fn priority_then_subscription_order() {
    let bus = EventBus::new();
    let order = log();
    let mut subs = Vec::new();
    for (name, prio) in [
        ("a0", 0),
        ("b5", 5),
        ("c0", 0),
        ("d-1", -1),
        ("e5", 5),
        ("f10", 10),
    ] {
        let o = Rc::clone(&order);
        subs.push(bus.subscribe_with(
            SubscribeOptions::default().priority(prio),
            move |_: &Hit| o.borrow_mut().push(name),
        ));
    }
    // A dynamic subscriber shares the same ordered list.
    let o = Rc::clone(&order);
    subs.push(bus.subscribe_dyn(
        Hit::stable_type_id(),
        SubscribeOptions::default().priority(5),
        move |_| o.borrow_mut().push("dyn5"),
    ));

    let expected = vec!["f10", "b5", "e5", "dyn5", "a0", "c0", "d-1"];
    for _ in 0..3 {
        order.borrow_mut().clear();
        bus.publish(Hit {
            target: 0,
            damage: 0.0,
            critical: false,
        });
        assert_eq!(*order.borrow(), expected);
    }
    // Removing and re-adding keeps the rule (re-added goes last among equals).
    drop(subs.remove(1)); // b5
    let o = Rc::clone(&order);
    subs.push(
        bus.subscribe_with(SubscribeOptions::default().priority(5), move |_: &Hit| {
            o.borrow_mut().push("b5'")
        }),
    );
    order.borrow_mut().clear();
    bus.publish(Hit {
        target: 0,
        damage: 0.0,
        critical: false,
    });
    assert_eq!(
        *order.borrow(),
        vec!["f10", "e5", "dyn5", "b5'", "a0", "c0", "d-1"]
    );
}

#[test]
fn deferred_order_is_deterministic_across_runs() {
    fn run() -> Vec<String> {
        let bus = EventBus::new();
        let weak = bus.downgrade();
        let out = log();
        let mut subs = Vec::new();
        for (name, prio) in [("lo", -1), ("hi", 1)] {
            let (o, w) = (Rc::clone(&out), weak.clone());
            subs.push(bus.subscribe_with(
                SubscribeOptions::default().priority(prio),
                move |p: &Ping| {
                    o.borrow_mut().push(format!("{name}:{}", p.0));
                    if name == "hi" && p.0 < 3 {
                        w.upgrade().unwrap().publish_deferred(Ping(p.0 + 10));
                    }
                },
            ));
        }
        for i in 0..3 {
            bus.publish_deferred(Ping(i));
        }
        bus.flush();
        out.take()
    }
    let first = run();
    assert_eq!(
        first,
        [
            "hi:0", "lo:0", "hi:1", "lo:1", "hi:2", "lo:2", "hi:10", "lo:10", "hi:11", "lo:11",
            "hi:12", "lo:12"
        ]
    );
    for _ in 0..10 {
        assert_eq!(run(), first);
    }
}

// ---------------------------------------------------------------------------
// SyncEventBus
// ---------------------------------------------------------------------------

#[test]
fn sync_bus_has_the_same_semantics() {
    let bus = SyncEventBus::new();
    bus.register_event::<Hit>().unwrap();
    let got = Arc::new(Mutex::new(Vec::new()));
    let g = Arc::clone(&got);
    let _t = bus.subscribe_with(
        SubscribeOptions::channel(Channel::Entity(1)).priority(1),
        move |h: &Hit| g.lock().unwrap().push(format!("typed{}", h.target)),
    );
    let g = Arc::clone(&got);
    let _d = bus.subscribe_dyn(
        Hit::stable_type_id(),
        SubscribeOptions::channel(Channel::Entity(1)),
        move |d| g.lock().unwrap().push(format!("dyn{:?}", d.fields[0])),
    );

    bus.publish_to(
        Channel::Entity(1),
        Hit {
            target: 1,
            damage: 0.0,
            critical: false,
        },
    );
    bus.publish_dyn_deferred(Channel::Entity(1), hit_dyn(2))
        .unwrap();
    bus.publish_deferred_to(
        Channel::Entity(2),
        Hit {
            target: 3,
            damage: 0.0,
            critical: false,
        },
    );
    assert_eq!(bus.flush().delivered, 2);
    assert_eq!(
        *got.lock().unwrap(),
        vec!["typed1", "dynU64(1)", "typed2", "dynU64(2)"]
    );
}

#[test]
fn sync_bus_concurrent_publish_subscribe_and_flush() {
    let bus = SyncEventBus::new();
    let count = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&count);
    let _s = bus.subscribe(move |_: &Ping| {
        c.fetch_add(1, Ordering::Relaxed);
    });
    std::thread::scope(|s| {
        for _ in 0..4 {
            let bus = bus.clone();
            s.spawn(move || {
                for i in 0..250 {
                    bus.publish(Ping(i));
                    bus.publish_deferred(Ping(i));
                    // Churn subscriptions while others dispatch.
                    let tmp = bus.subscribe(|_: &Ping| {});
                    drop(tmp);
                    if i % 50 == 0 {
                        bus.flush();
                    }
                }
            });
        }
    });
    bus.flush();
    assert_eq!(count.load(Ordering::Relaxed), 2000);
}

#[test]
fn sync_bus_unsubscribe_self_in_handler() {
    let bus = SyncEventBus::new();
    let slot = Arc::new(Mutex::new(None));
    let calls = Arc::new(AtomicUsize::new(0));
    let (s, c) = (Arc::clone(&slot), Arc::clone(&calls));
    *slot.lock().unwrap() = Some(bus.subscribe(move |_: &Ping| {
        c.fetch_add(1, Ordering::Relaxed);
        let me: Option<gamma_core::SyncSubscription> = s.lock().unwrap().take();
        drop(me);
    }));
    bus.publish(Ping(0));
    bus.publish(Ping(0));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn parallel_publish_reaches_every_handler() {
    let bus = SyncEventBus::new();
    let count = Arc::new(AtomicUsize::new(0));
    let subs: Vec<_> = (0..8)
        .map(|_| {
            let c = Arc::clone(&count);
            bus.subscribe(move |p: &Ping| {
                c.fetch_add(p.0 as usize, Ordering::Relaxed);
            })
        })
        .collect();
    bus.parallel_publish(Ping(2));
    assert_eq!(count.load(Ordering::Relaxed), 16);
    drop(subs);
}

// ---------------------------------------------------------------------------
// FFI layer, in-process (the cross-library test uses a real cdylib)
// ---------------------------------------------------------------------------

#[test]
fn foreign_bus_roundtrip_in_process() {
    let host = SyncEventBus::new();
    host.register_event::<Hit>().unwrap();
    // SAFETY: fresh export; single process.
    let plugin = unsafe { ForeignBus::from_raw(host.export_raw()) }.unwrap();
    assert!(plugin.is_thread_safe());

    let got = Arc::new(Mutex::new(Vec::new()));
    let g = Arc::clone(&got);
    let sub = plugin.subscribe_with(
        SubscribeOptions::channel(Channel::Entity(3)),
        move |h: &Hit| g.lock().unwrap().push(h.target),
    );
    assert!(sub.is_valid());
    assert_eq!(
        host.subscriber_count(Hit::stable_type_id(), Channel::Entity(3)),
        1
    );

    host.publish_to(
        Channel::Entity(3),
        Hit {
            target: 1,
            damage: 0.0,
            critical: false,
        },
    );
    host.publish_dyn(Channel::Entity(3), &hit_dyn(2)).unwrap();
    plugin.publish_to(
        Channel::Entity(3),
        Hit {
            target: 3,
            damage: 0.0,
            critical: false,
        },
    );
    plugin.publish_dyn(Channel::Entity(3), &hit_dyn(4)).unwrap();
    plugin
        .publish_deferred_to(
            Channel::Entity(3),
            Hit {
                target: 5,
                damage: 0.0,
                critical: false,
            },
        )
        .unwrap();
    assert!(
        plugin
            .publish_dyn(Channel::Global, &DynEvent::new(1, vec![]))
            .is_err()
    );
    host.flush();
    assert_eq!(*got.lock().unwrap(), vec![1, 2, 3, 4, 5]);

    drop(sub);
    assert_eq!(
        host.subscriber_count(Hit::stable_type_id(), Channel::Entity(3)),
        0
    );

    // The plugin's handle keeps the core alive; a subscription does not.
    let late = plugin.subscribe(|_: &Ping| {});
    drop(host);
    plugin.publish(Ping(1));
    drop(plugin);
    drop(late);
}

#[test]
fn local_bus_export_is_not_thread_safe() {
    let host = EventBus::new();
    let plugin = unsafe { ForeignBus::from_raw(host.export_raw()) }.unwrap();
    assert!(!plugin.is_thread_safe());
    let n = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&n);
    let _s = plugin.subscribe(move |p: &Ping| {
        c.fetch_add(p.0 as usize, Ordering::Relaxed);
    });
    host.publish(Ping(7));
    assert_eq!(n.load(Ordering::Relaxed), 7);
}

#[test]
fn raw_publish_rejects_bad_layouts() {
    use gamma_core::ffi::{
        EVENT_TYPED, RawChannel, RawEventRef, STATUS_BAD_CHANNEL, STATUS_BAD_LAYOUT, STATUS_OK,
    };
    let host = EventBus::new();
    let _s = host.subscribe(|_: &Ping| panic!("must not be delivered"));
    let raw = host.export_raw();
    let value = 5u64;
    let ev = |data: *const u8, align: usize, kind: u32| RawEventRef {
        id: Ping::stable_type_id(),
        channel: RawChannel { kind, value: 0 },
        kind: EVENT_TYPED,
        data,
        len: 4,
        align,
        to_dyn: None,
    };
    let base = (&value as *const u64).cast::<u8>();
    unsafe {
        assert_eq!(
            (raw.publish)(raw.ctx, &ev(std::ptr::null(), 4, 0)),
            STATUS_BAD_LAYOUT
        );
        assert_eq!(
            (raw.publish)(raw.ctx, &ev(base.add(1), 4, 0)),
            STATUS_BAD_LAYOUT
        );
        assert_eq!((raw.publish)(raw.ctx, &ev(base, 3, 0)), STATUS_BAD_LAYOUT);
        assert_eq!((raw.publish)(raw.ctx, &ev(base, 4, 9)), STATUS_BAD_CHANNEL);
        // Wrong size for Ping (u32 is 4 bytes; claim 8): delivered to nobody.
        let mut wrong = ev(base, 8, 0);
        wrong.len = 8;
        assert_eq!((raw.publish)(raw.ctx, &wrong), STATUS_OK);
        (raw.release)(raw.ctx);
    }
}

#[test]
fn dynamic_tuple_and_unit_events() {
    #[pulsar_event(dynamic)]
    #[derive(Debug, PartialEq)]
    struct Pair(u64, bool);
    #[pulsar_event(dynamic)]
    struct Tick;

    let d = Pair::descriptor().unwrap();
    assert_eq!(
        d.fields,
        vec![("0".into(), FieldType::U64), ("1".into(), FieldType::Bool)]
    );
    assert_eq!(
        Pair::from_dyn(&Pair(3, true).to_dyn().unwrap()),
        Some(Pair(3, true))
    );
    assert!(Tick::descriptor().unwrap().fields.is_empty());

    let bus = EventBus::new();
    bus.register_event::<Tick>().unwrap();
    let n = Rc::new(Cell::new(0));
    let c = Rc::clone(&n);
    let _s = bus.subscribe(move |_: &Tick| c.set(c.get() + 1));
    bus.publish_dyn(
        Channel::Global,
        &DynEvent::new(Tick::stable_type_id(), vec![]),
    )
    .unwrap();
    bus.publish(Tick);
    assert_eq!(n.get(), 2);
}

#[test]
fn concurrent_flushes_never_strand_events() {
    // Threads queue and flush concurrently, so most flushes find another one
    // running. Once every flush has returned, nothing may be left queued.
    let bus = SyncEventBus::new();
    let delivered = Arc::new(AtomicUsize::new(0));
    let d = Arc::clone(&delivered);
    let _s = bus.subscribe(move |_: &Ping| {
        d.fetch_add(1, Ordering::SeqCst);
    });
    std::thread::scope(|s| {
        for _ in 0..4 {
            let bus = bus.clone();
            s.spawn(move || {
                for i in 0..2000 {
                    bus.publish_deferred(Ping(i));
                    bus.flush();
                }
            });
        }
    });
    assert_eq!(
        bus.queued_len(),
        0,
        "no event left behind after all flushes returned"
    );
    assert_eq!(delivered.load(Ordering::SeqCst), 8000);
}
