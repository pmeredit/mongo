// Dead code analysis doesn't work here.
// The entry point is an exported function called from a .cpp file.
#![allow(dead_code)]

pub mod crust;

use std::ffi::{c_int, c_void};
use std::num::NonZero;

use bson::{to_raw_document_buf, Document, RawDocument, RawDocumentBuf};

use plugin_api_bindgen::{mongodb_aggregation_stage, mongodb_plugin_portal};

pub struct Error {
    pub code: NonZero<i32>,
    pub message: String,
}

pub enum GetNextResult<'a> {
    Advanced(&'a RawDocument),
    PauseExecution,
}

pub struct AggregationSource {
    ptr: *mut c_void,
    get_next_fn: plugin_api_bindgen::mongodb_source_get_next,
}

impl AggregationSource {
    pub fn new(ptr: *mut c_void, get_next_fn: plugin_api_bindgen::mongodb_source_get_next) -> Self {
        Self { ptr, get_next_fn }
    }

    pub fn get_next(&mut self) -> Option<GetNextResult<'_>> {
        let mut result_ptr = std::ptr::null();
        let mut result_len = 0usize;
        let code = unsafe {
            self.get_next_fn.expect("non-null")(self.ptr, &mut result_ptr, &mut result_len)
        };
        // XXX must handle errors.
        assert!(code <= 0);
        match code {
            plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_EOF => None,
            plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_PAUSE_EXECUTION => {
                Some(GetNextResult::PauseExecution)
            }
            plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_ADVANCED => {
                Some(GetNextResult::Advanced(
                    RawDocument::from_bytes(unsafe {
                        std::slice::from_raw_parts(result_ptr, result_len)
                    })
                    .unwrap(),
                ))
            }
            _ => unimplemented!(),
        }
    }
}

/// Trait for a stage in a MongoDB aggregation pipeline.
///
/// A struct that implements this interfaces can be used with [PluginAggregationStage] to register
/// with the C plugin API without writing any additional unsafe code.
pub trait AggregationStage: Sized {
    /// Return the name of this aggregation stage, useful for registration.
    fn name() -> &'static str;

    /// Get the C API bindings for this stage.
    ///
    /// You almost always want to generate this using the `generate_stage_api!` macro, returning
    /// the `API` constant from the generate module.
    fn api() -> mongodb_aggregation_stage;

    /// Create a new stage from a document containing the stage definition.
    /// The stage definition is a document value associated with the stage name, e.g.
    ///   $echo: {<contents of stage_definition}
    fn new(stage_definition: &RawDocument) -> Result<Self, Error>;

    /// Set a source for this stage. Used by intermediate stages to fetch documents to transform.
    fn set_source(&mut self, source: AggregationSource);

    /// Get the next result from this stage.
    /// If this returns `None`, this will return an EOF signal upstream, but IIRC there is no
    /// guarantee that this method will not be called again.
    // TODO: this needs refining. There's no way to an express an error happening during this
    // operation or upstream. Might be better to do Result<GetNextResult<'_>, Error> and add back
    // the EOF enum type.
    fn get_next(&mut self) -> Option<GetNextResult<'_>>;
}

// ALTERNATIVE DESIGN FOR PluginAggregationStage:
// * Make AggregationStage trait object-safe.
// * PluginAggregationStage accepts Box<dyn AggregationStage>
//
// + There's no need to generate extern ABI bindings since there is only one struct shape.
// + No opportunity to mess up the extern ABI bindings.
// - Calls go through double indirection (C function pointer -> vtable -> actual call).
// - Registration becomes a bit more complicated.

/// Wrapper around an [AggregationStage] that binds to the C plugin API.
#[repr(C)]
pub struct PluginAggregationStage<S: AggregationStage> {
    stage_api: mongodb_aggregation_stage,
    stage_impl: S,
}

impl<S: AggregationStage> std::ops::Deref for PluginAggregationStage<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.stage_impl
    }
}

impl<S: AggregationStage> std::ops::DerefMut for PluginAggregationStage<S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.stage_impl
    }
}

/// For an `AggregationStage` used with `PluginAggregationStage` generate ABI compatible functions
/// and a `mongodb_aggregation_stage` constant.
macro_rules! generate_stage_api {
    ($agg_stage:ident, $mod_name:ident) => {
        mod $mod_name {
            use super::$agg_stage;
            use plugin_api_bindgen::{mongodb_aggregation_stage, mongodb_source_get_next};
            use std::ffi::{c_int, c_void};

            pub unsafe extern "C-unwind" fn get_next(
                stage: *mut mongodb_aggregation_stage,
                result: *mut *const u8,
                result_len: *mut usize,
            ) -> c_int {
                crate::crust::get_next::<$agg_stage>(stage, result, result_len)
            }

            pub unsafe extern "C-unwind" fn set_source(
                stage: *mut mongodb_aggregation_stage,
                source_ptr: *mut c_void,
                source_get_next: mongodb_source_get_next,
            ) {
                crate::crust::set_source::<$agg_stage>(stage, source_ptr, source_get_next)
            }

            pub unsafe extern "C-unwind" fn close(stage: *mut mongodb_aggregation_stage) {
                crate::crust::close::<$agg_stage>(stage)
            }

            pub const API: mongodb_aggregation_stage = mongodb_aggregation_stage {
                get_next: Some(get_next),
                set_source: Some(set_source),
                close: Some(close),
            };
        }
    };
}

impl<S: AggregationStage> PluginAggregationStage<S> {
    pub unsafe extern "C-unwind" fn parse_external(
        bson_type: u8,
        bson_value: *const u8,
        bson_value_len: usize,
        stage: *mut *mut mongodb_aggregation_stage,
        error: *mut *const u8,
        error_len: *mut usize,
    ) -> c_int {
        if bson_type != 3 {
            // 3 is embedded document
            // XXX FIXME should include stage name!
            let static_err = "stage argument must be document typed.";
            *error = static_err.as_bytes().as_ptr();
            *error_len = static_err.as_bytes().len();
            return 1;
        }
        let doc_bytes = std::slice::from_raw_parts(bson_value as *const u8, bson_value_len);
        match Self::parse(doc_bytes) {
            Ok(stage_impl) => {
                let plugin_stage = Box::new(Self {
                    stage_api: S::api(),
                    stage_impl,
                });
                *stage = Box::into_raw(plugin_stage) as *mut mongodb_aggregation_stage;
                *error = std::ptr::null();
                *error_len = 0;
                0
            }
            Err(Error { code, message }) => {
                // TODO: fix the leak. We can either provide a way to free this or stash it in
                // thread local memory until the next call.
                *error = message.as_bytes().as_ptr();
                *error_len = message.len();
                std::mem::forget(message);
                code.into()
            }
        }
    }

    /// Parse a generic [AggregationStage] from raw bson document bytes.
    pub fn parse(bson_doc_bytes: &[u8]) -> Result<S, Error> {
        let doc = RawDocument::from_bytes(bson_doc_bytes).map_err(|e| {
            let message = if let Some(key) = e.key() {
                format!(
                    "Error parsing stage definition for {}: {:?} (key={})",
                    S::name(),
                    e.kind,
                    key
                )
            } else {
                format!(
                    "Error parsing stage definition for {}: {:?}",
                    S::name(),
                    e.kind
                )
            };
            Error {
                code: NonZero::new(1).unwrap(),
                message,
            }
        })?;
        S::new(doc)
    }

    pub fn register(plugin_portal: *mut mongodb_plugin_portal) {
        let stage_name = S::name();
        unsafe {
            (*plugin_portal).add_aggregation_stage.expect("add stage")(
                stage_name.as_bytes().as_ptr(),
                stage_name.as_bytes().len(),
                Some(Self::parse_external),
            );
        }
    }
}

struct EchoOxide {
    document: RawDocumentBuf,
    exhausted: bool,
}

impl AggregationStage for EchoOxide {
    fn name() -> &'static str {
        "$echoOxide"
    }

    fn api() -> mongodb_aggregation_stage {
        echo_oxide::API
    }

    fn new(stage_definition: &RawDocument) -> Result<Self, Error> {
        Ok(Self {
            document: stage_definition.to_owned(),
            exhausted: false,
        })
    }

    fn set_source(&mut self, _source: AggregationSource) {
        // Do nothing
    }

    fn get_next(&mut self) -> Option<GetNextResult<'_>> {
        if self.exhausted {
            None
        } else {
            self.exhausted = true;
            Some(GetNextResult::Advanced(&self.document))
        }
    }
}

generate_stage_api!(EchoOxide, echo_oxide);

struct AddSomeCrabs {
    source: Option<AggregationSource>,
    last_document: RawDocumentBuf,
}

impl AggregationStage for AddSomeCrabs {
    fn name() -> &'static str {
        "$addSomeCrabs"
    }

    fn api() -> mongodb_aggregation_stage {
        add_some_crabs::API
    }

    fn new(_stage_definition: &RawDocument) -> Result<Self, Error> {
        Ok(Self {
            source: None,
            last_document: RawDocumentBuf::new(),
        })
    }

    fn set_source(&mut self, source: AggregationSource) {
        self.source = Some(source);
    }

    fn get_next(&mut self) -> Option<GetNextResult<'_>> {
        assert!(self.source.is_some());
        let source = self.source.as_mut()?;
        // XXX handle errors.
        match source.get_next()? {
            GetNextResult::PauseExecution => Some(GetNextResult::PauseExecution),
            GetNextResult::Advanced(input_doc) => {
                let mut doc = Document::try_from(input_doc).unwrap();
                // TODO: you should be able to control the number of crabs.
                doc.insert("some_crabs", "🦀🦀🦀");
                self.last_document = to_raw_document_buf(&doc).unwrap();
                Some(GetNextResult::Advanced(self.last_document.as_ref()))
            }
        }
    }
}

generate_stage_api!(AddSomeCrabs, add_some_crabs);

// #[no_mangle] allows this to be called from C/C++.
#[no_mangle]
unsafe extern "C-unwind" fn initialize_rust_plugins(plugin_portal: *mut mongodb_plugin_portal) {
    PluginAggregationStage::<EchoOxide>::register(plugin_portal);
    PluginAggregationStage::<AddSomeCrabs>::register(plugin_portal);
}
