//! Procedural macros for the **Gamma** event system.
//!
//! | Macro | What it does |
//! |---|---|
//! | [`#[pulsar_event]`](attr.pulsar_event.html) | Adds `#[repr(C)]` **and** implements [`Event`]. |
//! | [`#[derive(Event)]`](derive.Event.html) | Just implements [`Event`] (you still need `#[repr(C)]`). |
//!
//! Both accept the same options:
//!
//! | Option | Effect |
//! |---|---|
//! | `dynamic` | Also implement the dynamic side of [`Event`]: `descriptor`, `to_dyn`, `from_dyn` (every field must implement `DynField`). The field schema is mixed into the stable id. |
//! | `name = "..."` | Use this name for the id and descriptor instead of the struct name (for namespacing, e.g. `"physics.Hit"`). |
//! | `crate = path` | Path to `gamma_core` (default `::gamma_core`); use `crate = gamma` when depending only on the umbrella crate. |
//!
//! [`Event`]: https://docs.rs/gamma-core/latest/gamma_core/trait.Event.html

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{DeriveInput, Fields, LitStr, Path, parse_macro_input};

#[derive(Default)]
struct Options {
    dynamic: bool,
    name: Option<LitStr>,
    krate: Option<Path>,
}

impl Options {
    fn parse_meta(&mut self, meta: syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
        if meta.path.is_ident("dynamic") {
            self.dynamic = true;
        } else if meta.path.is_ident("name") {
            self.name = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("crate") {
            let v = meta.value()?;
            self.krate = Some(if v.peek(LitStr) {
                v.parse::<LitStr>()?.parse()?
            } else {
                v.parse()?
            });
        } else {
            return Err(meta.error("expected `dynamic`, `name = \"...\"` or `crate = path`"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Attribute macro: #[pulsar_event]
// ---------------------------------------------------------------------------

/// Turns a struct into an FFI-safe event.
///
/// 1. **Adds `#[repr(C)]`** (unless present), for a stable layout across
///    compilation units.
/// 2. **Implements `Event`** with a deterministic `stable_type_id()` from the
///    name, size and alignment.
///
/// With `#[pulsar_event(dynamic)]` it also implements `descriptor`,
/// `to_dyn` and `from_dyn`, so dynamic (script) subscribers receive the
/// event and typed subscribers receive matching dynamic events.
///
/// ```rust
/// # use gamma_derive::pulsar_event;
/// use gamma_core::{Event, EventBus};
///
/// #[pulsar_event(dynamic, name = "game.PlayerJumped")]
/// struct PlayerJumped {
///     height: f32,
///     player: u64,
/// }
///
/// let d = PlayerJumped::descriptor().unwrap();
/// assert_eq!(d.name, "game.PlayerJumped");
/// assert_eq!(d.id, PlayerJumped::stable_type_id());
///
/// let bus = EventBus::new();
/// let _sub = bus.subscribe(|e: &PlayerJumped| assert_eq!(e.player, 7));
/// bus.publish(PlayerJumped { height: 5.0, player: 7 });
/// ```
#[proc_macro_attribute]
pub fn pulsar_event(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut opts = Options::default();
    let parser = syn::meta::parser(|meta| opts.parse_meta(meta));
    parse_macro_input!(attr with parser);

    let mut item = parse_macro_input!(item as syn::Item);
    let syn::Item::Struct(s) = &mut item else {
        return syn::Error::new_spanned(&item, "pulsar_event can only be applied to structs")
            .to_compile_error()
            .into();
    };
    if !has_repr_c(&s.attrs) {
        s.attrs.insert(0, syn::parse_quote!(#[repr(C)]));
    }
    let imp = match generate_event_impl(&s.ident, &s.generics, &s.fields, &opts) {
        Ok(t) => t,
        Err(e) => return e.to_compile_error().into(),
    };
    quote! { #item #imp }.into()
}

/// True when `#[repr(C)]` is already present among the attributes.
fn has_repr_c(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("repr") {
            return false;
        }
        if let syn::Meta::List(list) = &attr.meta {
            list.tokens.to_string().split(',').any(|s| s.trim() == "C")
        } else {
            false
        }
    })
}

// ---------------------------------------------------------------------------
// Shared generator
// ---------------------------------------------------------------------------

fn generate_event_impl(
    name: &syn::Ident,
    generics: &syn::Generics,
    fields: &Fields,
    opts: &Options,
) -> syn::Result<TokenStream2> {
    if !generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            generics,
            "event types cannot be generic",
        ));
    }
    let krate = opts
        .krate
        .clone()
        .unwrap_or_else(|| syn::parse_quote!(::gamma_core));
    let event_name = opts
        .name
        .as_ref()
        .map(LitStr::value)
        .unwrap_or_else(|| name.to_string());

    if !opts.dynamic {
        return Ok(quote! {
            impl #krate::Event for #name {
                #[inline]
                fn stable_type_id() -> u64 {
                    const ID: u64 = #krate::__private::type_id(
                        #event_name,
                        ::core::mem::size_of::<#name>(),
                        ::core::mem::align_of::<#name>(),
                    );
                    ID
                }
            }
        });
    }

    // (field name for the descriptor, accessor / constructor member, type)
    let members: Vec<(String, syn::Member, &syn::Type)> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| match &f.ident {
            Some(id) => (id.to_string(), syn::Member::Named(id.clone()), &f.ty),
            None => (i.to_string(), syn::Member::Unnamed(i.into()), &f.ty),
        })
        .collect();
    let field_names: Vec<&String> = members.iter().map(|m| &m.0).collect();
    let accessors: Vec<&syn::Member> = members.iter().map(|m| &m.1).collect();
    let types: Vec<&syn::Type> = members.iter().map(|m| m.2).collect();
    let count = members.len();
    let indices = 0..count;
    let vars: Vec<syn::Ident> = (0..count).map(|i| format_ident!("__f{}", i)).collect();

    let construct = match fields {
        Fields::Named(_) => quote! { #name { #( #accessors: #vars ),* } },
        Fields::Unnamed(_) => quote! { #name ( #( #vars ),* ) },
        Fields::Unit => quote! { #name },
    };

    Ok(quote! {
        impl #krate::Event for #name {
            #[inline]
            fn stable_type_id() -> u64 {
                const ID: u64 = #krate::__private::type_id_with_fields(
                    #event_name,
                    ::core::mem::size_of::<#name>(),
                    ::core::mem::align_of::<#name>(),
                    &[ #( (#field_names, <#types as #krate::DynField>::FIELD_TYPE) ),* ],
                );
                ID
            }

            const REFLECTED: bool = true;

            fn descriptor() -> ::core::option::Option<#krate::EventDescriptor> {
                ::core::option::Option::Some(#krate::EventDescriptor::new(
                    <Self as #krate::Event>::stable_type_id(),
                    #event_name,
                    ::std::vec![ #( (::std::string::String::from(#field_names), <#types as #krate::DynField>::FIELD_TYPE) ),* ],
                ))
            }

            fn to_dyn(&self) -> ::core::option::Option<#krate::DynEvent> {
                ::core::option::Option::Some(#krate::DynEvent::new(
                    <Self as #krate::Event>::stable_type_id(),
                    ::std::vec![ #( #krate::DynField::to_dyn_value(&self.#accessors) ),* ],
                ))
            }

            fn from_dyn(event: &#krate::DynEvent) -> ::core::option::Option<Self> {
                if event.id != <Self as #krate::Event>::stable_type_id() || event.fields.len() != #count {
                    return ::core::option::Option::None;
                }
                #( let #vars = <#types as #krate::DynField>::from_dyn_value(&event.fields[#indices])?; )*
                ::core::option::Option::Some(#construct)
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Derive macro: #[derive(Event)]
// ---------------------------------------------------------------------------

/// Derives `Event` for a struct. You **must** also apply `#[repr(C)]`.
/// Prefer [`#[pulsar_event]`](attr.pulsar_event.html).
///
/// Options go in a helper attribute: `#[event(dynamic, name = "...", crate = path)]`.
///
/// ```rust
/// use gamma_derive::Event;
///
/// #[derive(Event)]
/// #[event(dynamic)]
/// #[repr(C)]
/// struct Damage {
///     target: u64,
///     amount: f64,
/// }
/// ```
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let mut opts = Options::default();
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("event")) {
        if let Err(e) = attr.parse_nested_meta(|meta| opts.parse_meta(meta)) {
            return e.to_compile_error().into();
        }
    }
    let fields = match &input.data {
        syn::Data::Struct(s) => &s.fields,
        _ => {
            return syn::Error::new_spanned(&input.ident, "Event can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };
    match generate_event_impl(&input.ident, &input.generics, fields, &opts) {
        Ok(t) => t.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
