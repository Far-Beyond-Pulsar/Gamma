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
    /// The event name. Names are unique within a bus's registry.
    pub name: String,
    /// Field names and types, in declaration order.
    pub fields: Vec<(String, FieldType)>,
}

impl EventDescriptor {
    /// A descriptor with an explicit id.
    pub fn new(id: u64, name: impl Into<String>, fields: Vec<(String, FieldType)>) -> Self {
        Self {
            id,
            name: name.into(),
            fields,
        }
    }

    /// A descriptor for a runtime-declared (script) event. The id is derived
    /// from the name and the field list with [`dynamic_event_id`], so the
    /// same declaration always gets the same id.
    pub fn dynamic<N: Into<String>>(
        name: impl Into<String>,
        fields: impl IntoIterator<Item = (N, FieldType)>,
    ) -> Self {
        let name = name.into();
        let fields: Vec<(String, FieldType)> =
            fields.into_iter().map(|(n, t)| (n.into(), t)).collect();
        let id = dynamic_event_id(&name, &fields);
        Self { id, name, fields }
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
    let mut h = stable_id::fnv_bytes(stable_id::FNV_OFFSET, b"dyn:");
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
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(WIRE_VERSION);
        out.extend_from_slice(&self.id.to_le_bytes());
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
        if r.u8()? != WIRE_VERSION {
            return Err(DecodeError);
        }
        let id = r.u64()?;
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
        Ok(Self { id, name, fields })
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
