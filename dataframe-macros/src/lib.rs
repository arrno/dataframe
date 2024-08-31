extern crate proc_macro2;

use proc_macro::TokenStream;
use quote::quote;
use syn::Data;

#[proc_macro_derive(ToRow)]
pub fn derive_into_hash_map(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::DeriveInput);

    let struct_identifier = &input.ident;

    match &input.data {
        Data::Struct(syn::DataStruct { fields, .. }) => {
            let mut to_row_impl = quote! {
                let mut v = vec![];
            };
            let mut to_label_impl = quote! {
                let mut v = vec![];
            };
            for field in fields {
                let identifier = field.ident.as_ref().unwrap();
                to_row_impl.extend(quote! {
                    v.push(self.#identifier.ref_to_cell());
                });
                to_label_impl.extend(quote! {
                    v.push(stringify!(#identifier).to_string());
                })
            }
            quote! {
                impl ToRow for #struct_identifier {
                    fn to_row(&self) -> Vec<Cell> {
                        #to_row_impl
                        v
                    }
                    fn labels(&self) -> Vec<String> {
                        #to_label_impl
                        v
                    }
                }
            }
        }
        _ => unimplemented!(),
    }
    .into()
}
