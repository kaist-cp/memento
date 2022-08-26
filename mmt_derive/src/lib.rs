use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::{self, parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields, Index};

#[proc_macro_derive(Memento)]
pub fn derive_memento(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Construct a representation of Rust code as a syntax tree that we can manipulate
    let input = parse_macro_input!(input as DeriveInput);

    // Used in the quasi-quotation below as `#name`.
    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Build the trait implementation
    let clears = clear_each_fields(&input.data);
    let expanded = quote! {
        // The generated impl.
        impl #impl_generics Memento for #name #ty_generics #where_clause {
            fn clear(&mut self) {
                #clears
            }
        }
    };

    // Hand the output tokens back to the compiler.
    proc_macro::TokenStream::from(expanded)
}

// Generate an expression to sum up the heap size of each field.
fn clear_each_fields(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    // Expands to an expression like
                    //
                    //     self.x.clear(); self.y.clear(); self.z.clear();
                    //
                    // but using fully qualified function call syntax.
                    //
                    // This way if one of the field types does not
                    // implement `Memento` then the compiler's error message
                    // underlines which field it is.
                    let recurse = fields.named.iter().map(|f| {
                        let name = &f.ident;
                        quote_spanned! {f.span()=>
                            Memento::clear(&mut self.#name)
                        }
                    });
                    quote! {
                        #(#recurse; )*
                    }
                }
                Fields::Unnamed(ref fields) => {
                    // Expands to an expression like
                    //
                    //     self.0.clear(); self.1.clear(); self.2.clear();
                    let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                        let index = Index::from(i);
                        quote_spanned! {f.span()=>
                            Memento::clear(&mut self.#index)
                        }
                    });
                    quote! {
                        #(#recurse; )*
                    }
                }
                Fields::Unit => {
                    // Unit structs's clear() is no-op
                    quote!()
                }
            }
        }
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}

#[proc_macro_derive(Collectable)]
pub fn derive_collectable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Construct a representation of Rust code as a syntax tree that we can manipulate
    let input = parse_macro_input!(input as DeriveInput);

    // Used in the quasi-quotation below as `#name`.
    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Build the trait implementation
    let filters = filter_each_fields(&input.data);
    let expanded = quote! {
        // The generated impl.
        impl #impl_generics Collectable for #name #ty_generics #where_clause {
            fn filter(s: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
                #filters
            }
        }
    };

    // Hand the output tokens back to the compiler.
    proc_macro::TokenStream::from(expanded)
}

// Generate an expression to sum up the heap size of each field.
fn filter_each_fields(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    // Expands to an expression like
                    //
                    //     Collectable::filter(&mut s.x, ...); Collectable::filter(&mut s.y, ...);
                    //
                    // but using fully qualified function call syntax.
                    //
                    // This way if one of the field types does not
                    // implement `Memento` then the compiler's error message
                    // underlines which field it is.
                    let recurse = fields.named.iter().map(|f| {
                        let name = &f.ident;
                        quote_spanned! {f.span()=>
                            Collectable::filter(&mut s.#name, tid, gc, pool)
                        }
                    });
                    quote! {
                        #(#recurse; )*
                    }
                }
                Fields::Unnamed(ref fields) => {
                    // Expands to an expression like
                    //
                    //     Collectable::filter(&mut s.0, ...); Collectable::filter(&mut s.1, ...);
                    let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                        let index = Index::from(i);
                        quote_spanned! {f.span()=>
                            Collectable::filter(&mut s.#index, tid, gc, pool)
                        }
                    });
                    quote! {
                        #(#recurse; )*
                    }
                }
                Fields::Unit => {
                    // Unit structs's filter() is no-op
                    quote!()
                }
            }
        }
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}
