//! Basic typed publish/subscribe behaviour (ported from the 0.1 unit tests).

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use gamma_core::{Event, EventBus};
use gamma_derive::pulsar_event;

#[pulsar_event]
struct EmptyEvent;

#[pulsar_event]
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

impl PrimitiveEvent {
    fn with_c(c: u32) -> Self {
        Self {
            a: 0,
            b: 0,
            c,
            d: 0,
            e: 0,
            f: 0,
            g: 0,
            h: 0,
            i: 0.0,
            j: 0.0,
        }
    }
}

#[pulsar_event]
struct BoolCharEvent {
    flag: bool,
    ch: char,
}

#[pulsar_event]
struct ArrayEvent {
    values: [u64; 8],
}

#[pulsar_event]
struct NestedInner {
    x: f32,
    y: f32,
}

#[pulsar_event]
struct NestedEvent {
    id: u32,
    inner: NestedInner,
}

#[pulsar_event]
struct TupleEvent(u32, f64, bool);

#[pulsar_event]
struct LargeEvent {
    buffer: [u8; 1024],
    checksum: u64,
}

fn cell<T: Copy + 'static>(val: T) -> (Rc<Cell<T>>, Rc<Cell<T>>) {
    let rc = Rc::new(Cell::new(val));
    (Rc::clone(&rc), rc)
}

#[test]
fn subscribe_receives_published_event() {
    let bus = EventBus::new();
    let (sent, received) = cell(0u32);
    let _s = bus.subscribe(move |e: &PrimitiveEvent| sent.set(e.c));
    bus.publish(PrimitiveEvent::with_c(42));
    assert_eq!(received.get(), 42);
}

#[test]
fn publish_with_no_subscribers_is_noop() {
    let bus = EventBus::new();
    bus.publish(EmptyEvent);
    bus.publish(PrimitiveEvent::with_c(0));
}

#[test]
fn event_types_are_independent() {
    let bus = EventBus::new();
    let (flag_a, received_a) = cell(false);
    let (flag_b, received_b) = cell(false);
    let _a = bus.subscribe(move |_: &EmptyEvent| flag_a.set(true));
    let _b = bus.subscribe(move |_: &BoolCharEvent| flag_b.set(true));

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
    let bus = EventBus::new();
    let (a_sent, a_recv) = cell(0u32);
    let (b_sent, b_recv) = cell(0u32);
    let _a = bus.subscribe(move |e: &TupleEvent| a_sent.set(e.0));
    let _b = bus.subscribe(move |e: &TupleEvent| b_sent.set(e.0 * 2));
    bus.publish(TupleEvent(7, 3.5, true));
    assert_eq!(a_recv.get(), 7);
    assert_eq!(b_recv.get(), 14);
}

#[test]
fn subscribers_invoked_in_registration_order() {
    let bus = EventBus::new();
    let log = Rc::new(RefCell::new(Vec::new()));
    for i in 0..5 {
        let log = Rc::clone(&log);
        bus.subscribe(move |_: &EmptyEvent| log.borrow_mut().push(i))
            .detach();
    }
    bus.publish(EmptyEvent);
    assert_eq!(*log.borrow(), (0..5).collect::<Vec<_>>());
}

#[test]
fn complex_field_types() {
    let bus = EventBus::new();
    let (c_sent, c_recv) = cell('\0');
    let arr = Rc::new(RefCell::new([0u64; 8]));
    let (xy_sent, xy_recv) = cell((0u32, 0.0f32, 0.0f32));
    let big = Rc::new(RefCell::new(0u64));

    let _a = bus.subscribe(move |e: &BoolCharEvent| c_sent.set(e.ch));
    let arr2 = Rc::clone(&arr);
    let _b = bus.subscribe(move |e: &ArrayEvent| *arr2.borrow_mut() = e.values);
    let _c = bus.subscribe(move |e: &NestedEvent| xy_sent.set((e.id, e.inner.x, e.inner.y)));
    let big2 = Rc::clone(&big);
    let _d = bus.subscribe(move |e: &LargeEvent| {
        *big2.borrow_mut() = e.buffer.iter().map(|&b| b as u64).sum::<u64>() + e.checksum
    });

    bus.publish(BoolCharEvent {
        flag: true,
        ch: '🚀',
    });
    bus.publish(ArrayEvent {
        values: [10, 20, 30, 40, 50, 60, 70, 80],
    });
    bus.publish(NestedEvent {
        id: 999,
        inner: NestedInner { x: 1.5, y: -3.25 },
    });
    let mut buffer = [0u8; 1024];
    for (i, b) in buffer.iter_mut().enumerate() {
        *b = (i % 256) as u8;
    }
    bus.publish(LargeEvent {
        buffer,
        checksum: 1,
    });

    assert_eq!(c_recv.get(), '🚀');
    assert_eq!(*arr.borrow(), [10, 20, 30, 40, 50, 60, 70, 80]);
    assert_eq!(xy_recv.get(), (999, 1.5, -3.25));
    assert_eq!(*big.borrow(), 4 * (0..256u64).sum::<u64>() + 1);
}

#[test]
fn multiple_publishes_accumulate_in_order() {
    let bus = EventBus::new();
    let log = Rc::new(RefCell::new(Vec::new()));
    let l = Rc::clone(&log);
    let _s = bus.subscribe(move |e: &TupleEvent| l.borrow_mut().push(e.0));
    for i in 1..=3 {
        bus.publish(TupleEvent(i, 0.0, false));
    }
    assert_eq!(*log.borrow(), vec![1, 2, 3]);
}

#[test]
fn publish_then_subscribe_receives_nothing_past() {
    let bus = EventBus::new();
    let (sent, called) = cell(false);
    bus.publish(EmptyEvent);
    let _s = bus.subscribe(move |_: &EmptyEvent| sent.set(true));
    assert!(!called.get());
}

#[test]
fn many_publishes() {
    let bus = EventBus::new();
    let (c, count) = cell(0u32);
    let _s = bus.subscribe(move |_: &EmptyEvent| c.set(c.get() + 1));
    for _ in 0..1000 {
        bus.publish(EmptyEvent);
    }
    assert_eq!(count.get(), 1000);
}

#[test]
fn type_ids_are_unique_and_deterministic() {
    let ids = [
        EmptyEvent::stable_type_id(),
        PrimitiveEvent::stable_type_id(),
        BoolCharEvent::stable_type_id(),
        ArrayEvent::stable_type_id(),
        NestedInner::stable_type_id(),
        NestedEvent::stable_type_id(),
        TupleEvent::stable_type_id(),
        LargeEvent::stable_type_id(),
    ];
    let mut sorted = ids.to_vec();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), ids.len());
    assert_eq!(EmptyEvent::stable_type_id(), EmptyEvent::stable_type_id());
}

#[test]
fn same_layout_different_names_produce_different_ids() {
    #[pulsar_event]
    struct Pos2d {
        x: f32,
        y: f32,
    }
    assert_ne!(NestedInner::stable_type_id(), Pos2d::stable_type_id());
}

#[test]
fn ids_are_unchanged_from_v0_1() {
    // The 0.1 derive hashed exactly name, size and align.
    assert_eq!(
        TupleEvent::stable_type_id(),
        gamma_core::stable_id::type_id(
            "TupleEvent",
            size_of::<TupleEvent>(),
            align_of::<TupleEvent>()
        )
    );
}
