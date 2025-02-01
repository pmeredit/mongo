// Dead code analysis doesn't work here.
// The entry point is an exported function called from a .cpp file.
#![allow(dead_code)]

use std::num::NonZero;
use std::os::raw::{c_char, c_int};

use bson::raw::{RawDocument, RawDocumentBuf};

use plugin_api_bindgen::{mongodb_aggregation_stage, mongodb_plugin_portal};

pub struct Error {
    pub code: NonZero<i32>,
    pub message: String,
}

pub enum GetNextResult<'a> {
    Advanced(&'a RawDocument),
    PauseExecution,
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
    /// You almost always want to generate this using the `generate_stage_api_impls` and
    /// `generate_stage_api` macros.
    fn api() -> mongodb_aggregation_stage;

    /// Create a new stage from a document containing the stage definition.
    /// The stage definition is a document value associated with the stage name, e.g.
    ///   $echo: {<contents of stage_definition}
    fn new(stage_definition: &RawDocument) -> Result<Self, Error>;

    /// Get the next result from this stage.
    /// If this returns `None`, this will return an EOF signal upstream, but IIRC there is no
    /// guarantee that this method will not be called again.
    // TODO: this needs refining. There's no way to an express an error happening during this
    // operation or upstream. Might be better to do Result<GetNextResult<'_>, Error> and add back
    // the EOF enum type.
    fn get_next(&mut self) -> Option<GetNextResult<'_>>;
}

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

// NB: this works but it is incredibly annoying, and that's without even adding uses outside of
// the crate. At the very least we will probably want to reimport all the symbols into a module
// so that they can be consistently referenced.
macro_rules! generate_stage_api_impls {
    ($agg_stage:ident, $mod_name:ident) => {
        mod $mod_name {
            use super::$agg_stage;
            use crate::*;
            use plugin_api_bindgen::*;
            use std::os::raw::{c_char, c_int};

            pub unsafe extern "C-unwind" fn get_next(
                stage: *mut mongodb_aggregation_stage,
                result: *mut *const c_char,
                result_len: *mut usize,
            ) -> c_int {
                let rust_stage = (stage as *mut PluginAggregationStage<$agg_stage>)
                    .as_mut()
                    .expect("non-null stage pointer");
                match rust_stage.get_next() {
                    None => {
                        *result = std::ptr::null();
                        *result_len = 0;
                        MongoDBAggregationStageGetNextResult_GET_NEXT_EOF
                    }
                    Some(GetNextResult::PauseExecution) => {
                        *result = std::ptr::null();
                        *result_len = 0;
                        MongoDBAggregationStageGetNextResult_GET_NEXT_PAUSE_EXECUTION
                    }
                    Some(GetNextResult::Advanced(doc)) => {
                        *result = doc.as_bytes().as_ptr() as *const c_char;
                        *result_len = doc.as_bytes().len();
                        MongoDBAggregationStageGetNextResult_GET_NEXT_ADVANCED
                    }
                }
            }

            pub unsafe extern "C-unwind" fn close(stage: *mut mongodb_aggregation_stage) {
                let rust_stage = Box::from_raw(stage as *mut PluginAggregationStage<$agg_stage>);
                drop(rust_stage);
            }
        }
    };
}

macro_rules! generate_stage_api {
    ($agg_stage:ident, $mod_name:ident) => {
        ::plugin_api_bindgen::mongodb_aggregation_stage {
            get_next: Some($mod_name::get_next),
            close: Some($mod_name::close),
        }
    };
}

impl<S: AggregationStage> PluginAggregationStage<S> {
    pub unsafe extern "C-unwind" fn parse_external(
        bson_type: i8,
        bson_value: *const c_char,
        bson_value_len: usize,
        stage: *mut *mut mongodb_aggregation_stage,
        error: *mut *const c_char,
        error_len: *mut usize,
    ) -> c_int {
        if bson_type != 3 {
            // 3 is embedded document
            // XXX FIXME should include stage name!
            let static_err = "stage argument must be document typed.";
            *error = static_err.as_bytes().as_ptr() as *const c_char;
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
                *error = message.as_bytes().as_ptr() as *const i8;
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
                stage_name.as_bytes().as_ptr() as *const c_char,
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
        generate_stage_api!(EchoOxide, echo_oxide)
    }

    fn new(stage_definition: &RawDocument) -> Result<Self, Error> {
        Ok(Self {
            document: stage_definition.to_owned(),
            exhausted: false,
        })
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

generate_stage_api_impls!(EchoOxide, echo_oxide);

// #[no_mangle] allows this to be called from C/C++.
#[no_mangle]
unsafe extern "C-unwind" fn initialize_rust_plugins(plugin_portal: *mut mongodb_plugin_portal) {
    PluginAggregationStage::<EchoOxide>::register(plugin_portal);
}
