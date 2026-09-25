//! The derive works through the umbrella crate with `crate = gamma`.

use gamma::prelude::*;

#[pulsar_event(dynamic, crate = gamma, name = "umbrella.Scored")]
#[derive(Debug, PartialEq)]
struct Scored {
    player: u64,
    points: i64,
}

#[derive(gamma::Event)]
#[event(crate = "gamma")]
#[repr(C)]
struct Plain(u32);

#[test]
fn umbrella_paths() {
    let bus = EventBus::new();
    bus.register_event::<Scored>().unwrap();
    let got = std::rc::Rc::new(std::cell::Cell::new(0));
    let g = got.clone();
    let _s = bus.subscribe(move |s: &Scored| g.set(s.points));
    let d = bus.descriptor_by_name("umbrella.Scored").unwrap();
    bus.publish_dyn(
        Channel::Global,
        &DynEvent::new(d.id, vec![DynValue::U64(1), DynValue::I64(5)]),
    )
    .unwrap();
    assert_eq!(got.get(), 5);
    assert_ne!(Plain::stable_type_id(), Scored::stable_type_id());
    let _ = Plain(0).0;
}
