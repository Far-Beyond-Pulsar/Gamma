//! Runtime-described ("dynamic") events.
//!
//! A dynamic event is an [`EventDescriptor`] (id, name and typed fields)
//! plus a [`DynEvent`] payload holding one [`DynValue`] per field. Scripts
//! declare and publish events this way; Rust types opt in with
//! `#[pulsar_event(dynamic)]` so the same event can be received either as a
//! Rust struct or as a [`DynEvent`].
//!
//! Dynamic values cross library boundaries in a small, versioned binary
//! encoding ([`DynEvent::encode`] / [`DynEvent::decode`]) so no Rust-layout
//! type (`String`, `Vec`) is ever shared between separately compiled
//! libraries.

use std::fmt;

use crate::stable_id;

// ---------------------------------------------------------------------------
// FieldType / DynValue
// ---------------------------------------------------------------------------

/// The type of one field of a dynamic event.
///
/// The set is deliberately small: it is what scripts and the binary wire
/// encoding understand. Rust fields map onto it through [`DynField`].
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FieldType {
    /// `bool`.
    Bool = 0,
    /// Signed integer (`i8` to `i64`, `isize`).
    I64 = 1,
    /// Floating point (`f32`, `f64`).
    F64 = 2,
    /// Unsigned integer (`u8` to `u64`, `usize`), also used for entity
    /// handles and ids.
    U64 = 3,
    /// UTF-8 string.
    Str = 4,
    /// Raw bytes.
    Bytes = 5,
}

impl FieldType {
    /// The stable one-byte tag used in hashes and the wire encoding.
    pub const fn tag(self) -> u8 {
        self as u8
    }

    /// Inverse of [`tag`](Self::tag).
    pub const fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            0 => Self::Bool,
            1 => Self::I64,
            2 => Self::F64,
            3 => Self::U64,
            4 => Self::Str,
            5 => Self::Bytes,
            _ => return None,
        })
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Bool => "bool",
            Self::I64 => "i64",
            Self::F64 => "f64",
            Self::U64 => "u64",
            Self::Str => "str",
            Self::Bytes => "bytes",
        })
    }
}

/// One field value of a [`DynEvent`].
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DynValue {
    /// A boolean.
    Bool(bool),
    /// A signed integer.
    I64(i64),
    /// A float.
    F64(f64),
    /// An unsigned integer, entity handle or id.
    U64(u64),
    /// A string.
    Str(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
}

impl DynValue {
    /// The [`FieldType`] of this value.
    pub fn field_type(&self) -> FieldType {
        match self {
            Self::Bool(_) => FieldType::Bool,
            Self::I64(_) => FieldType::I64,
            Self::F64(_) => FieldType::F64,
            Self::U64(_) => FieldType::U64,
            Self::Str(_) => FieldType::Str,
            Self::Bytes(_) => FieldType::Bytes,
        }
    }
}

// ---------------------------------------------------------------------------
// DynField: Rust field <-> DynValue
// ---------------------------------------------------------------------------

/// A Rust type usable as a field of a `#[pulsar_event(dynamic)]` event.
///
/// Implemented for `bool`, all primitive integers, `f32`, `f64`, `String`
/// and `Vec<u8>`. Implement it for your own newtypes (entity handles, say)
/// to use them as fields.
pub trait DynField: Sized {
    /// The dynamic type this field is exposed as.
    const FIELD_TYPE: FieldType;
    /// Convert to a dynamic value.
    fn to_dyn_value(&self) -> DynValue;
    /// Convert from a dynamic value; `None` on a type mismatch or an
    /// out-of-range integer.
    fn from_dyn_value(value: &DynValue) -> Option<Self>;
    /// Write the value into a pooled sink: allocation-free when the sink's
    /// buffers are primed. The default writes [`to_dyn_value`], which for
    /// buffer-backed fields allocates; `String` and `Vec<u8>` override it
    /// to reuse the sink's pooled buffers instead.
    fn to_dyn_into(&self, sink: &mut DynEventSink) {
        sink.push_fixed(self.to_dyn_value());
    }
}

impl DynField for bool {
    const FIELD_TYPE: FieldType = FieldType::Bool;
    fn to_dyn_value(&self) -> DynValue {
        DynValue::Bool(*self)
    }
    fn from_dyn_value(value: &DynValue) -> Option<Self> {
        match value {
            DynValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

macro_rules! dyn_int {
    ($variant:ident, $wide:ty, $($t:ty),*) => {$(
        impl DynField for $t {
            const FIELD_TYPE: FieldType = FieldType::$variant;
            fn to_dyn_value(&self) -> DynValue {
                DynValue::$variant(*self as $wide)
            }
            fn from_dyn_value(value: &DynValue) -> Option<Self> {
                match value {
                    DynValue::$variant(v) => <$t>::try_from(*v).ok(),
                    _ => None,
                }
            }
        }
    )*};
}
dyn_int!(I64, i64, i8, i16, i32, i64, isize);
dyn_int!(U64, u64, u8, u16, u32, u64, usize);

impl DynField for f64 {
    const FIELD_TYPE: FieldType = FieldType::F64;
    fn to_dyn_value(&self) -> DynValue {
        DynValue::F64(*self)
    }
    fn from_dyn_value(value: &DynValue) -> Option<Self> {
        match value {
            DynValue::F64(v) => Some(*v),
            _ => None,
        }
    }
}

impl DynField for f32 {
    const FIELD_TYPE: FieldType = FieldType::F64;
    fn to_dyn_value(&self) -> DynValue {
        DynValue::F64(f64::from(*self))
    }
    fn from_dyn_value(value: &DynValue) -> Option<Self> {
        match value {
            DynValue::F64(v) => Some(*v as f32),
            _ => None,
        }
    }
}

impl DynField for String {
    const FIELD_TYPE: FieldType = FieldType::Str;
    fn to_dyn_value(&self) -> DynValue {
        DynValue::Str(self.clone())
    }
    fn to_dyn_into(&self, sink: &mut DynEventSink) {
        sink.push_pooled_str(self);
    }
    fn from_dyn_value(value: &DynValue) -> Option<Self> {
        match value {
            DynValue::Str(s) => Some(s.clone()),
            _ => None,
        }
    }
}

impl DynField for Vec<u8> {
    const FIELD_TYPE: FieldType = FieldType::Bytes;
    fn to_dyn_value(&self) -> DynValue {
        DynValue::Bytes(self.clone())
    }
    fn to_dyn_into(&self, sink: &mut DynEventSink) {
        sink.push_pooled_bytes(self);
    }
    fn from_dyn_value(value: &DynValue) -> Option<Self> {
        match value {
            DynValue::Bytes(b) => Some(b.clone()),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// EventDescriptor
// ---------------------------------------------------------------------------

/// Describes an event at runtime: its stable id, name and typed fields.
///
/// Rust events declared with `#[pulsar_event(dynamic)]` provide one through
/// [`Event::descriptor`](crate::Event::descriptor); scripts build one with
/// [`EventDescriptor::dynamic`]. Descriptors are registered on a bus with
/// `register_descriptor` / `register_event`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EventDescriptor {
    /// The stable event id (equal to `T::stable_type_id()` for Rust events).
    pub id: u64,
    /// The explicit namespace, or `None` for the main namespace (every
    /// event declared without one). `"main"` and `""` are reserved and are
    /// refused by [`register_descriptor`](crate::EventBus::register_descriptor).
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub namespace: Option<String>,
    /// The event name. Names are unique within a namespace of a bus's registry.
    pub name: String,
    /// Field names and types, in declaration order.
    pub fields: Vec<(String, FieldType)>,
}

impl EventDescriptor {
    /// A descriptor in the main namespace with an explicit id.
    pub fn new(id: u64, name: impl Into<String>, fields: Vec<(String, FieldType)>) -> Self {
        Self {
            id,
            namespace: None,
            name: name.into(),
            fields,
        }
    }

    /// A descriptor in the explicit namespace `namespace` with an explicit id.
    /// A reserved namespace is only refused when the descriptor is registered.
    pub fn new_in(
        id: u64,
        namespace: impl Into<String>,
        name: impl Into<String>,
        fields: Vec<(String, FieldType)>,
    ) -> Self {
        Self {
            id,
            namespace: Some(namespace.into()),
            name: name.into(),
            fields,
        }
    }

    /// A descriptor for a runtime-declared (script) event in the main
    /// namespace. The id is derived from the name and the field list with
    /// [`dynamic_event_id`], so the same declaration always gets the same id.
    pub fn dynamic<N: Into<String>>(
        name: impl Into<String>,
        fields: impl IntoIterator<Item = (N, FieldType)>,
    ) -> Self {
        let name = name.into();
        let fields: Vec<(String, FieldType)> =
            fields.into_iter().map(|(n, t)| (n.into(), t)).collect();
        let id = dynamic_event_id(&name, &fields);
        Self {
            id,
            namespace: None,
            name,
            fields,
        }
    }

    /// Like [`dynamic`](Self::dynamic), in the explicit namespace
    /// `namespace` (the id is derived with [`dynamic_event_id_in`]).
    ///
    /// Fails with [`RegistryError::ReservedNamespace`] for `""` and `"main"`:
    /// the main namespace is only reachable by not naming a namespace.
    pub fn dynamic_in<N: Into<String>>(
        namespace: impl Into<String>,
        name: impl Into<String>,
        fields: impl IntoIterator<Item = (N, FieldType)>,
    ) -> Result<Self, RegistryError> {
        let namespace = namespace.into();
        if stable_id::namespace_is_reserved(&namespace) {
            return Err(RegistryError::ReservedNamespace(namespace));
        }
        let name = name.into();
        let fields: Vec<(String, FieldType)> =
            fields.into_iter().map(|(n, t)| (n.into(), t)).collect();
        let id = dynamic_event_id_in(&namespace, &name, &fields);
        Ok(Self {
            id,
            namespace: Some(namespace),
            name,
            fields,
        })
    }

    /// Index of the field called `name`.
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|(n, _)| n == name)
    }

    /// Check that `event` has this descriptor's id, arity and field types.
    pub fn check(&self, event: &DynEvent) -> Result<(), DynEventError> {
        if event.id != self.id {
            return Err(DynEventError::UnknownEvent(event.id));
        }
        if event.fields.len() != self.fields.len() {
            return Err(DynEventError::Arity {
                expected: self.fields.len(),
                got: event.fields.len(),
            });
        }
        for (index, (value, (_, ty))) in event.fields.iter().zip(&self.fields).enumerate() {
            if value.field_type() != *ty {
                return Err(DynEventError::FieldType {
                    index,
                    expected: *ty,
                    got: value.field_type(),
                });
            }
        }
        Ok(())
    }
}

/// The id [`EventDescriptor::dynamic`] assigns: an FNV-1a hash over a
/// `"dyn:"` prefix, the name and every field's name and type tag.
pub fn dynamic_event_id(name: &str, fields: &[(String, FieldType)]) -> u64 {
    dynamic_event_id_from(stable_id::FNV_OFFSET, name, fields)
}

/// [`dynamic_event_id`] for an event in the explicit namespace `ns`: the
/// hash starts from [`stable_id::namespace_seed`], so it never equals the
/// main-namespace id of the same declaration.
pub fn dynamic_event_id_in(ns: &str, name: &str, fields: &[(String, FieldType)]) -> u64 {
    dynamic_event_id_from(stable_id::namespace_seed(ns), name, fields)
}

fn dynamic_event_id_from(seed: u64, name: &str, fields: &[(String, FieldType)]) -> u64 {
    let mut h = stable_id::fnv_bytes(seed, b"dyn:");
    h = stable_id::fnv_bytes(h, name.as_bytes());
    for (n, t) in fields {
        h = stable_id::fnv_bytes(h, n.as_bytes());
        h = stable_id::fnv_u64(h, t.tag() as u64);
    }
    h
}

// ---------------------------------------------------------------------------
// DynEvent
// ---------------------------------------------------------------------------

/// The payload of a dynamic event: its id and one value per field.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DynEvent {
    /// The event id (matches an [`EventDescriptor::id`]).
    pub id: u64,
    /// Field values in descriptor order.
    pub fields: Vec<DynValue>,
}

const WIRE_VERSION: u8 = 1;
/// Descriptor layout with a namespace string (main-namespace descriptors
/// keep using [`WIRE_VERSION`]).
const WIRE_VERSION_NS: u8 = 2;

// ---------------------------------------------------------------------------
// DynEventSink: building reflective dynamic events from pooled buffers
// ---------------------------------------------------------------------------

/// Builds a [`DynEvent`] field by field, taking its buffers from a pool.
///
/// [`DynField::to_dyn_into`] writes with no allocations once the sink's
/// buffers are primed: fixed-width fields go straight into the fields
/// vector, and `String` / `Vec<u8>` fields are reused (`push_pooled_str`).
/// The buffers are returned to the pool by whoever delivered the event
/// (the core's `Pools`).
///
/// A sink is a one-shot builder: obtain one (usually from a pool), write
/// the fields, then [`finish`](Self::finish) — which also returns what the
/// sink did not use, for the pool.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DynEventSink {
    /// The event's stable id, set by the writer before the fields.
    pub id: u64,
    /// Field values built so far, in descriptor order.
    fields: Vec<DynValue>,
    /// Pooled spare strings, popped one per string field.
    strings: Vec<String>,
    /// Pooled spare byte vectors, popped one per bytes field.
    bytes: Vec<Vec<u8>>,
}

/// Buffers a sink did not use; back into the pool.
#[derive(Debug, Clone, PartialEq)]
pub struct SinkSpares {
    pub(crate) strings: Vec<String>,
    pub(crate) bytes: Vec<Vec<u8>>,
}

impl DynEventSink {
    /// A sink with no pooled buffers (al conversions allocate fresh ones).
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// Add pooled buffers to the sink, done by `Pools` when it creates one.
    pub(crate) fn give_spares(&mut self, strings: Vec<String>, bytes: Vec<Vec<u8>>) {
        self.strings = strings;
        self.bytes = bytes;
    }

    /// Use a pooled fields vector for this event's values.
    pub(crate) fn give_fields(&mut self, fields: Vec<DynValue>) {
        self.fields = fields;
    }

    /// Pooled string in place: clear it and copy `value` in.
    #[inline]
    pub(crate) fn push_pooled_str(&mut self, value: &str) {
        let mut s = self.strings.pop().unwrap_or_default();
        s.clear();
        s.push_str(value);
        self.fields.push(DynValue::Str(s));
    }

    /// Pooled byte vector in place: clear it and copy `value` in.
    #[inline]
    pub(crate) fn push_pooled_bytes(&mut self, value: &[u8]) {
        let mut b = self.bytes.pop().unwrap_or_default();
        b.clear();
        b.extend_from_slice(value);
        self.fields.push(DynValue::Bytes(b));
    }

    /// Fixed-width field push (no allocation).
    #[inline]
    pub(crate) fn push_fixed(&mut self, value: DynValue) {
        self.fields.push(value);
    }

    /// Build the event; returns what the sink did not use for the pool.
    pub(crate) fn finish(mut self) -> (Option<DynEvent>, SinkSpares) {
        let event = Some(DynEvent {
            id: self.id,
            fields: std::mem::take(&mut self.fields),
        });
        (
            event,
            SinkSpares {
                strings: std::mem::take(&mut self.strings),
                bytes: std::mem::take(&mut self.bytes),
            },
        )
    }
}

impl DynEvent {
    /// A new dynamic event.
    pub fn new(id: u64, fields: Vec<DynValue>) -> Self {
        Self { id, fields }
    }

    /// Field `index`.
    pub fn field(&self, index: usize) -> Option<&DynValue> {
        self.fields.get(index)
    }

    /// Serialise into Gamma's ABI-stable wire format (appends to `out`).
    ///
    /// Layout: `version: u8`, `id: u64 LE`, `count: u32 LE`, then per value a
    /// type tag byte and its payload (`bool` as one byte, numbers as 8 bytes
    /// LE, strings and bytes as a `u32 LE` length plus the bytes).
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(WIRE_VERSION);
        out.extend_from_slice(&self.id.to_le_bytes());
        out.extend_from_slice(&(self.fields.len() as u32).to_le_bytes());
        for v in &self.fields {
            out.push(v.field_type().tag());
            match v {
                DynValue::Bool(b) => out.push(*b as u8),
                DynValue::I64(i) => out.extend_from_slice(&i.to_le_bytes()),
                DynValue::F64(f) => out.extend_from_slice(&f.to_le_bytes()),
                DynValue::U64(u) => out.extend_from_slice(&u.to_le_bytes()),
                DynValue::Str(s) => put_bytes(out, s.as_bytes()),
                DynValue::Bytes(b) => put_bytes(out, b),
            }
        }
    }

    /// Convenience wrapper around [`encode`](Self::encode).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode(&mut v);
        v
    }

    /// Parse the wire format produced by [`encode`](Self::encode).
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = Reader(bytes);
        if r.u8()? != WIRE_VERSION {
            return Err(DecodeError);
        }
        let id = r.u64()?;
        let count = r.u32()? as usize;
        // Every value takes at least 2 bytes; cap the preallocation.
        let mut fields = Vec::with_capacity(count.min(r.0.len() / 2));
        for _ in 0..count {
            let tag = FieldType::from_tag(r.u8()?).ok_or(DecodeError)?;
            fields.push(match tag {
                FieldType::Bool => DynValue::Bool(match r.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(DecodeError),
                }),
                FieldType::I64 => DynValue::I64(r.u64()? as i64),
                FieldType::F64 => DynValue::F64(f64::from_bits(r.u64()?)),
                FieldType::U64 => DynValue::U64(r.u64()?),
                FieldType::Str => {
                    DynValue::Str(String::from_utf8(r.bytes()?.to_vec()).map_err(|_| DecodeError)?)
                }
                FieldType::Bytes => DynValue::Bytes(r.bytes()?.to_vec()),
            });
        }
        if !r.0.is_empty() {
            return Err(DecodeError);
        }
        Ok(Self { id, fields })
    }
}

impl EventDescriptor {
    /// Serialise into the ABI-stable wire format used to register
    /// descriptors across a library boundary.
    ///
    /// Main-namespace descriptors use the original (version 1) layout, so
    /// older libraries still understand them. Namespaced descriptors use
    /// version 2, which adds the namespace string after the id.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match &self.namespace {
            None => out.push(WIRE_VERSION),
            Some(_) => out.push(WIRE_VERSION_NS),
        }
        out.extend_from_slice(&self.id.to_le_bytes());
        if let Some(ns) = &self.namespace {
            put_bytes(out, ns.as_bytes());
        }
        put_bytes(out, self.name.as_bytes());
        out.extend_from_slice(&(self.fields.len() as u32).to_le_bytes());
        for (n, t) in &self.fields {
            put_bytes(out, n.as_bytes());
            out.push(t.tag());
        }
    }

    /// Parse the output of [`encode`](Self::encode).
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = Reader(bytes);
        let version = r.u8()?;
        if version != WIRE_VERSION && version != WIRE_VERSION_NS {
            return Err(DecodeError);
        }
        let id = r.u64()?;
        let namespace = if version == WIRE_VERSION_NS {
            Some(r.string()?)
        } else {
            None
        };
        let name = r.string()?;
        let count = r.u32()? as usize;
        let mut fields = Vec::with_capacity(count.min(r.0.len() / 5));
        for _ in 0..count {
            let n = r.string()?;
            let t = FieldType::from_tag(r.u8()?).ok_or(DecodeError)?;
            fields.push((n, t));
        }
        if !r.0.is_empty() {
            return Err(DecodeError);
        }
        Ok(Self {
            id,
            namespace,
            name,
            fields,
        })
    }
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.0.len() < n {
            return Err(DecodeError);
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn string(&mut self) -> Result<String, DecodeError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| DecodeError)
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Malformed wire bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError;

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("malformed gamma wire data")
    }
}
impl std::error::Error for DecodeError {}

/// Why a dynamic event was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DynEventError {
    /// No descriptor is registered for this id.
    UnknownEvent(u64),
    /// Wrong number of fields.
    Arity {
        /// Fields in the descriptor.
        expected: usize,
        /// Fields in the event.
        got: usize,
    },
    /// A field has the wrong type.
    FieldType {
        /// Field index.
        index: usize,
        /// Type in the descriptor.
        expected: FieldType,
        /// Type in the event.
        got: FieldType,
    },
}

impl fmt::Display for DynEventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownEvent(id) => write!(f, "no event descriptor registered for id {id:#018x}"),
            Self::Arity { expected, got } => write!(f, "expected {expected} fields, got {got}"),
            Self::FieldType {
                index,
                expected,
                got,
            } => {
                write!(f, "field {index}: expected {expected}, got {got}")
            }
        }
    }
}
impl std::error::Error for DynEventError {}

/// Why a descriptor could not be registered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryError {
    /// A different descriptor is already registered under this id.
    IdConflict {
        /// The id.
        id: u64,
        /// Name of the descriptor already registered.
        existing: String,
    },
    /// Another id is already registered under this name.
    NameConflict {
        /// The name.
        name: String,
        /// Id already registered under it.
        existing_id: u64,
    },
    /// The Rust type was not declared with `#[pulsar_event(dynamic)]`.
    NotReflected,
    /// The namespace is `""` or `"main"`. Events outside any namespace are
    /// in the main namespace, which cannot be named explicitly.
    ReservedNamespace(String),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdConflict { id, existing } => {
                write!(
                    f,
                    "event id {id:#018x} is already registered with a different descriptor ({existing})"
                )
            }
            Self::NameConflict { name, existing_id } => {
                write!(
                    f,
                    "event name {name:?} is already registered with id {existing_id:#018x}"
                )
            }
            Self::NotReflected => {
                f.write_str("event type is not declared with #[pulsar_event(dynamic)]")
            }
            Self::ReservedNamespace(ns) => {
                write!(
                    f,
                    "namespace {ns:?} is reserved (events without a namespace are in the main namespace)"
                )
            }
        }
    }
}
impl std::error::Error for RegistryError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_roundtrip() {
        let e = DynEvent::new(
            42,
            vec![
                DynValue::Bool(true),
                DynValue::I64(-7),
                DynValue::F64(1.5),
                DynValue::U64(u64::MAX),
                DynValue::Str("héllo".into()),
                DynValue::Bytes(vec![0, 1, 2]),
            ],
        );
        assert_eq!(DynEvent::decode(&e.to_bytes()).unwrap(), e);
        let d = EventDescriptor::dynamic(
            "Hit",
            [("target", FieldType::U64), ("damage", FieldType::F64)],
        );
        let mut b = Vec::new();
        d.encode(&mut b);
        assert_eq!(EventDescriptor::decode(&b).unwrap(), d);
    }

    #[test]
    fn namespaced_descriptor_roundtrip_and_ids() {
        let main = EventDescriptor::dynamic("Hit", [("t", FieldType::U64)]);
        let ns = EventDescriptor::dynamic_in("engine", "Hit", [("t", FieldType::U64)]).unwrap();
        assert_ne!(main.id, ns.id);
        assert_eq!(main.namespace, None);
        let mut b = Vec::new();
        ns.encode(&mut b);
        assert_eq!(b[0], 2);
        assert_eq!(EventDescriptor::decode(&b).unwrap(), ns);
        // Main descriptors keep the original wire layout.
        let mut b = Vec::new();
        main.encode(&mut b);
        assert_eq!(b[0], 1);
        assert_eq!(EventDescriptor::decode(&b).unwrap(), main);
    }

    #[test]
    fn main_namespace_cannot_be_named() {
        for ns in ["main", ""] {
            assert_eq!(
                EventDescriptor::dynamic_in(ns, "E", [("a", FieldType::Bool)]),
                Err(RegistryError::ReservedNamespace(ns.into()))
            );
        }
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(DynEvent::decode(&[]).is_err());
        assert!(DynEvent::decode(&[1, 0, 0]).is_err());
        let mut b = DynEvent::new(1, vec![DynValue::Str("x".into())]).to_bytes();
        b.push(0);
        assert!(DynEvent::decode(&b).is_err());
    }

    #[test]
    fn int_conversion_is_range_checked() {
        assert_eq!(u8::from_dyn_value(&DynValue::U64(300)), None);
        assert_eq!(u8::from_dyn_value(&DynValue::U64(200)), Some(200));
        assert_eq!(i32::from_dyn_value(&DynValue::U64(1)), None);
    }

    #[test]
    fn descriptor_check() {
        let d = EventDescriptor::dynamic("E", [("a", FieldType::Bool)]);
        assert!(
            d.check(&DynEvent::new(d.id, vec![DynValue::Bool(true)]))
                .is_ok()
        );
        assert!(matches!(
            d.check(&DynEvent::new(d.id, vec![])),
            Err(DynEventError::Arity { .. })
        ));
        assert!(matches!(
            d.check(&DynEvent::new(d.id, vec![DynValue::U64(1)])),
            Err(DynEventError::FieldType { index: 0, .. })
        ));
    }
}

#[cfg(all(test, feature = "serde"))]
mod serde_tests {
    fn assert_serde<T: serde::Serialize + serde::de::DeserializeOwned>() {}

    #[test]
    fn dynamic_types_are_serde() {
        assert_serde::<super::DynValue>();
        assert_serde::<super::DynEvent>();
        assert_serde::<super::FieldType>();
        assert_serde::<super::EventDescriptor>();
        assert_serde::<crate::Channel>();
    }
}
