use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(FletchSchema)]
pub fn derive_fletch_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;
    let stream_ext = format_ident!("{}StreamExt", ident);

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    ident,
                    "FletchSchema requires a struct with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(ident, "FletchSchema can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    let mut channel_defs = Vec::new();
    let mut method_sigs = Vec::new();
    let mut methods = Vec::new();

    for field in fields {
        let name = field.ident.expect("named field");
        let ty = field.ty;
        let channel_name = name.to_string();
        channel_defs.push(quote! {
            builder = builder.channel::<#ty>(#channel_name)?;
        });
        method_sigs.push(quote! {
            fn #name(&mut self, timestamp_ns: i64, value: #ty) -> fletch::Result<()>;
        });
        methods.push(quote! {
            fn #name(&mut self, timestamp_ns: i64, value: #ty) -> fletch::Result<()> {
                self.write(timestamp_ns, #channel_name, value)
            }
        });
    }

    quote! {
        impl fletch::FletchSchema for #ident {
            fn stream_name() -> &'static str {
                stringify!(#ident)
            }

            fn configure_builder(
                mut builder: fletch::FletchStreamBuilder,
            ) -> fletch::Result<fletch::FletchStreamBuilder> {
                #(#channel_defs)*
                Ok(builder)
            }
        }

        pub trait #stream_ext {
            #(#method_sigs)*
        }

        impl #stream_ext for fletch::Stream<#ident> {
            #(#methods)*
        }
    }
    .into()
}
