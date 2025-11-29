
extern crate proc_macro;
mod atomic_enum;
use proc_macro::TokenStream;

#[proc_macro_derive(AtomicEnum)]
pub fn derive_atomic_enum(input: TokenStream) -> TokenStream {
    atomic_enum::derive_atomic_enum_inner(input)
}
