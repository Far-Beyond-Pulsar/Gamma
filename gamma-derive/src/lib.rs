//! Procedural macros for the **Gamma** event system.
//!
//! | Macro | What it does |
//! |---|---|
//! | [`#[pulsar_event]`](attr.pulsar_event.html) | All-in-one: adds `#[repr(C)]` **and** implements [`Event`]. |
//! | [`#[derive(Event)]`](derive.Event.html) | Just implements [`Event`] (you still need `#[repr(C)]`). |
//!
//! [`Event`]: https://docs.rs/gamma-core/latest/gamma_core/trait.Event.html

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

// ---------------------------------------------------------------------------
// Attribute macro: #[pulsar_event]
// ---------------------------------------------------------------------------

/// Attribute macro that turns a struct into a FFI‑safe event.
///
/// `#[pulsar_event]` is a single‑attribute replacement for the two‑attribute
/// pattern `#[derive(Event)] #[repr(C)]`.  It:
///
/// 1. **Adds `#[repr(C)]`** — guaranteeing stable memory layout across
///    compilation units.
/// 2. **Implements [`Event`]** — generating a deterministic `stable_type_id()`
///    that incorporates the type's name, size, and alignment.
///
/// # Example
///
/// ```rust
/// # use gamma_derive::pulsar_event;
/// use gamma_core::EventBus;
///
/// #[pulsar_event]
/// struct PlayerJumped {
///     height: f32,
///     timestamp: u64,
/// }
///
/// let mut bus = EventBus::new();
/// bus.subscribe(|e: &PlayerJumped| { /* … */ });
/// bus.publish(PlayerJumped { height: 5.0, timestamp: 12345 });
/// ```
///
/// The generated struct is exactly equivalent to:
///
/// ```ignore
/// // Both attributes are required manually here:
/// #[derive(Event)]
/// #[repr(C)]
/// struct PlayerJumped {
///     height: f32,
///     timestamp: u64,
/// }
/// ```
///
/// [`Event`]: https://docs.rs/gamma-core/latest/gamma_core/trait.Event.html
#[proc_macro_attribute]
pub fn pulsar_event(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(item as syn::Item);

    match &mut item {
        syn::Item::Struct(s) => {
            if !has_repr_c(&s.attrs) {
                s.attrs.insert(0, syn::parse_quote!(#[repr(C)]));
            }

            let name = &s.ident;
            let impl_event = generate_event_impl(name);

            let expanded = quote! {
                #item
                #impl_event
            };

            TokenStream::from(expanded)
        }
        _ => {
            let err = syn::Error::new_spanned(
                &item,
                "pulsar_event can only be applied to structs",
            );
            TokenStream::from(err.to_compile_error())
        }
    }
}

/// True when `#[repr(C)]` is already present among the attributes.
fn has_repr_c(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("repr") {
            return false;
        }
        // Parse the attribute meta to inspect the repr arguments.
        if let syn::Meta::List(list) = &attr.meta {
            list.tokens
                .to_string()
                .split(',')
                .any(|s| s.trim() == "C")
        } else {
            false
        }
    })
}

// ---------------------------------------------------------------------------
// Shared helper
// ---------------------------------------------------------------------------

/// Generate the `Event` trait implementation body.
fn generate_event_impl(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote! {
        impl gamma_core::Event for #name {
            fn stable_type_id() -> u64 {
                // FNV-1a offset basis
                let mut hash: u64 = 0xcbf29ce484222325;

                for byte in stringify!(#name).as_bytes() {
                    hash ^= *byte as u64;
                    hash = hash.wrapping_mul(0x100000001b3);
                }

                // Mix in the type's size — catches layout differences.
                hash ^= std::mem::size_of::<Self>() as u64;
                hash = hash.wrapping_mul(0x100000001b3);

                // Mix in the type's alignment — catches padding differences.
                hash ^= std::mem::align_of::<Self>() as u64;
                hash = hash.wrapping_mul(0x100000001b3);

                hash
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Derive macro: #[derive(Event)]
// ---------------------------------------------------------------------------

/// Derives the [`Event`] trait for a struct.
///
/// You **must** also apply `#[repr(C)]` separately.
/// Prefer [`#[pulsar_event]`](attr.pulsar_event.html) unless you need to
/// control the `repr` yourself.
///
/// The generated `stable_type_id()` uses a FNV-1a hash seeded with the
/// struct's name, size, and alignment.
///
/// [`Event`]: https://docs.rs/gamma-core/latest/gamma_core/trait.Event.html
#[proc_macro_derive(Event)]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = generate_event_impl(name);

    TokenStream::from(expanded)
}
