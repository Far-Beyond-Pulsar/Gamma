//! Stable, compiler-independent event ids (FNV-1a).
//!
//! These are `const fn`s so derived ids fold to constants. The algorithm is
//! part of Gamma's ABI: [`type_id`] produces exactly the ids of Gamma 0.1.

use crate::FieldType;

/// FNV-1a 64-bit offset basis.
pub const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
pub const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Mix `bytes` into `hash`.
pub const fn fnv_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    hash
}

/// Mix one integer into `hash` (a single FNV step with `value` as the byte).
pub const fn fnv_u64(hash: u64, value: u64) -> u64 {
    (hash ^ value).wrapping_mul(FNV_PRIME)
}

/// Id of a plain `#[pulsar_event]`: name, size and alignment.
pub const fn type_id(name: &str, size: usize, align: usize) -> u64 {
    let h = fnv_bytes(FNV_OFFSET, name.as_bytes());
    let h = fnv_u64(h, size as u64);
    fnv_u64(h, align as u64)
}

/// Id of a `#[pulsar_event(dynamic)]`: [`type_id`] plus every field's name
/// and [`FieldType`] tag, so two libraries that disagree on the schema get
/// different ids.
pub const fn type_id_with_fields(
    name: &str,
    size: usize,
    align: usize,
    fields: &[(&str, FieldType)],
) -> u64 {
    let mut h = type_id(name, size, align);
    let mut i = 0;
    while i < fields.len() {
        h = fnv_bytes(h, fields[i].0.as_bytes());
        h = fnv_u64(h, fields[i].1.tag() as u64);
        i += 1;
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The v0.1 derive, verbatim, to prove ids did not change.
    fn legacy(name: &str, size: usize, align: usize) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in name.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= size as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        hash ^= align as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        hash
    }

    #[test]
    fn matches_v0_1() {
        assert_eq!(
            type_id("PlayerJumped", 16, 8),
            legacy("PlayerJumped", 16, 8)
        );
        assert_eq!(type_id("", 0, 1), legacy("", 0, 1));
    }

    #[test]
    fn fields_change_the_id() {
        let a = type_id_with_fields("E", 8, 8, &[("x", FieldType::U64)]);
        let b = type_id_with_fields("E", 8, 8, &[("x", FieldType::I64)]);
        let c = type_id_with_fields("E", 8, 8, &[("y", FieldType::U64)]);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, type_id("E", 8, 8));
    }
}
