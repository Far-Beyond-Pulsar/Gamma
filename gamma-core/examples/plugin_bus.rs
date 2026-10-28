//! A thin namespaced wrapper over Gamma's real API, for engine plugins.

use gamma_core::{
    Channel, DynEvent, DynValue, EventBus, EventDescriptor, FieldType, RegistryError,
    SubscribeOptions, Subscription,
};

#[derive(Debug)]
enum PluginBusError {
    Registry(RegistryError),
    UnknownEvent,
    Publish(gamma_core::DynEventError),
}

impl std::fmt::Display for PluginBusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Registry(error) => write!(f, "registry error: {error}"),
            Self::UnknownEvent => f.write_str("event is not declared in this namespace"),
            Self::Publish(error) => write!(f, "event publish failed: {error}"),
        }
    }
}

struct PluginEventBus {
    bus: EventBus,
}

impl PluginEventBus {
    fn new() -> Self {
        Self {
            bus: EventBus::new(),
        }
    }

    /// Declare an event. `namespace: None` is the main namespace.
    fn declare(
        &self,
        namespace: Option<&str>,
        event: &str,
        fields: &[(&str, FieldType)],
    ) -> Result<(), PluginBusError> {
        let fields = fields.iter().map(|&(n, t)| (n, t));
        let desc = match namespace {
            Some(ns) => EventDescriptor::dynamic_in(ns, event, fields),
            None => Ok(EventDescriptor::dynamic(event, fields)),
        }
        .map_err(PluginBusError::Registry)?;
        self.bus
            .register_descriptor(desc)
            .map(drop)
            .map_err(PluginBusError::Registry)
    }

    fn lookup(&self, namespace: Option<&str>, event: &str) -> Option<u64> {
        let d = match namespace {
            Some(ns) => self.bus.descriptor_by_name_in(ns, event),
            None => self.bus.descriptor_by_name(event),
        };
        d.map(|d| d.id)
    }

    fn publish(
        &self,
        namespace: Option<&str>,
        event: &str,
        values: Vec<DynValue>,
    ) -> Result<(), PluginBusError> {
        let id = self
            .lookup(namespace, event)
            .ok_or(PluginBusError::UnknownEvent)?;
        self.bus
            .publish_dyn(Channel::Global, &DynEvent::new(id, values))
            .map_err(PluginBusError::Publish)
    }

    /// The handler stays registered while the returned handle lives.
    fn subscribe(
        &self,
        namespace: Option<&str>,
        event: &str,
        handler: impl Fn(&DynEvent) + 'static,
    ) -> Result<Subscription, PluginBusError> {
        let id = self
            .lookup(namespace, event)
            .ok_or(PluginBusError::UnknownEvent)?;
        Ok(self
            .bus
            .subscribe_dyn(id, SubscribeOptions::default(), handler))
    }
}

fn main() {
    let bus = PluginEventBus::new();
    bus.declare(Some("engine"), "Tick", &[("dt", FieldType::F64)])
        .unwrap();
    bus.declare(None, "Tick", &[("dt", FieldType::F64)])
        .unwrap(); // main, distinct
    assert!(bus.declare(Some("main"), "Tick", &[]).is_err()); // reserved

    let sub = bus
        .subscribe(Some("engine"), "Tick", |e| {
            println!("engine tick: {:?}", e.fields)
        })
        .unwrap();
    bus.publish(Some("engine"), "Tick", vec![DynValue::F64(0.016)])
        .unwrap(); // printed
    bus.publish(None, "Tick", vec![DynValue::F64(0.016)])
        .unwrap(); // main: not delivered
    sub.unsubscribe(); // unsubscribing is done through the handle
}
