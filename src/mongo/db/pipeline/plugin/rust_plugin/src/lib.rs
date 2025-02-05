// Dead code analysis doesn't work here.
// The entry point is an exported function called from a .cpp file.
#![allow(dead_code)]

use std::ffi::{c_int, c_void};
use std::num::NonZero;

use bson::{to_raw_document_buf, Document, RawDocument, RawDocumentBuf};

use plugin_api_bindgen::{
    mongodb_aggregation_stage, mongodb_plugin_portal, mongodb_source_get_next,
};

#[derive(Debug)]
pub struct Error {
    pub code: NonZero<i32>,
    pub message: String,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        std::write!(f, "Plugin error code {}: {}", self.code.get(), self.message)
    }
}

// TODO: add a map() fn that only maps over the Advanced state.
#[derive(Debug)]
pub enum GetNextResult<'a> {
    Advanced(&'a RawDocument),
    PauseExecution,
    EOF,
}

/// Represents an input for an aggregation stage.
pub struct AggregationSource {
    ptr: *mut c_void,
    get_next_fn: plugin_api_bindgen::mongodb_source_get_next,
}

impl AggregationSource {
    pub fn new(ptr: *mut c_void, get_next_fn: plugin_api_bindgen::mongodb_source_get_next) -> Self {
        Self { ptr, get_next_fn }
    }

    pub fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        let mut result_ptr = std::ptr::null();
        let mut result_len = 0usize;
        let code = unsafe {
            self.get_next_fn.expect("non-null")(self.ptr, &mut result_ptr, &mut result_len)
        };
        match code {
            plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_EOF => Ok(GetNextResult::EOF),
            plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_PAUSE_EXECUTION => {
                Ok(GetNextResult::PauseExecution)
            }
            plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_ADVANCED => {
                Ok(GetNextResult::Advanced(
                    RawDocument::from_bytes(unsafe {
                        std::slice::from_raw_parts(result_ptr, result_len)
                    })
                    .unwrap(),
                ))
            }
            _ => Err(Error {
                code: NonZero::new(code).unwrap(),
                message: std::str::from_utf8(unsafe {
                    std::slice::from_raw_parts(result_ptr, result_len)
                })
                .unwrap()
                .to_owned(),
            }),
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

    /// Create a new stage from a document containing the stage definition.
    /// The stage definition is a document value associated with the stage name, e.g.
    ///   $echo: {<contents of stage_definition}
    fn new(stage_definition: &RawDocument) -> Result<Self, Error>;

    /// Set a source for this stage. Used by intermediate stages to fetch documents to transform.
    fn set_source(&mut self, source: AggregationSource);

    /// Get the next result from this stage.
    /// This may contain a document or another stream marker, including EOF.
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error>;
}

/// Wrapper around an [AggregationStage] that binds to the C plugin API.
#[repr(C)]
pub struct PluginAggregationStage<S: AggregationStage> {
    stage_api: mongodb_aggregation_stage,
    stage_impl: S,
    // This is a place to put errors or other things that might leak.
    buf: Vec<u8>,
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

impl<S: AggregationStage> PluginAggregationStage<S> {
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

    unsafe extern "C-unwind" fn parse_external(
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
                    stage_api: mongodb_aggregation_stage {
                        get_next: Some(Self::get_next),
                        set_source: Some(Self::set_source),
                        close: Some(Self::close),
                    },
                    stage_impl,
                    buf: vec![],
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
    fn parse(bson_doc_bytes: &[u8]) -> Result<S, Error> {
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

    unsafe extern "C-unwind" fn get_next(
        stage: *mut mongodb_aggregation_stage,
        result: *mut *const u8,
        result_len: *mut usize,
    ) -> c_int {
        let rust_stage = (stage as *mut PluginAggregationStage<S>)
            .as_mut()
            .expect("non-null stage pointer");
        match rust_stage.get_next() {
            Ok(GetNextResult::EOF) => {
                *result = std::ptr::null();
                *result_len = 0;
                plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_EOF
            }
            Ok(GetNextResult::PauseExecution) => {
                *result = std::ptr::null();
                *result_len = 0;
                plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_PAUSE_EXECUTION
            }
            Ok(GetNextResult::Advanced(doc)) => {
                *result = doc.as_bytes().as_ptr();
                *result_len = doc.as_bytes().len();
                plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_ADVANCED
            }
            Err(Error { code, message }) => {
                // XXX we leak memory right here. this memory needs to be owned by the stage.
                rust_stage.buf = message.into_bytes();
                *result = rust_stage.buf.as_ptr();
                *result_len = rust_stage.buf.len();
                code.get()
            }
        }
    }

    unsafe extern "C-unwind" fn set_source(
        stage: *mut mongodb_aggregation_stage,
        source_ptr: *mut c_void,
        source_get_next: mongodb_source_get_next,
    ) {
        let rust_stage = (stage as *mut PluginAggregationStage<S>)
            .as_mut()
            .expect("non-null stage pointer");
        rust_stage.set_source(AggregationSource::new(source_ptr, source_get_next))
    }

    unsafe extern "C-unwind" fn close(stage: *mut mongodb_aggregation_stage) {
        let rust_stage = Box::from_raw(stage as *mut PluginAggregationStage<S>);
        drop(rust_stage);
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

    fn new(stage_definition: &RawDocument) -> Result<Self, Error> {
        Ok(Self {
            document: stage_definition.to_owned(),
            exhausted: false,
        })
    }

    fn set_source(&mut self, _source: AggregationSource) {
        // Do nothing
    }

    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        Ok(if self.exhausted {
            GetNextResult::EOF
        } else {
            self.exhausted = true;
            GetNextResult::Advanced(&self.document)
        })
    }
}

struct AddSomeCrabs {
    source: Option<AggregationSource>,
    last_document: RawDocumentBuf,
}

impl AggregationStage for AddSomeCrabs {
    fn name() -> &'static str {
        "$addSomeCrabs"
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

    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        let source = self
            .source
            .as_mut()
            .expect("intermediate stage must have source");
        let source_result = source.get_next()?;
        match source_result {
            GetNextResult::Advanced(input_doc) => {
                let mut doc = Document::try_from(input_doc).unwrap();
                // TODO: you should be able to control the number of crabs.
                doc.insert("some_crabs", "🦀🦀🦀");
                self.last_document = to_raw_document_buf(&doc).unwrap();
                Ok(GetNextResult::Advanced(self.last_document.as_ref()))
            }
            _ => Ok(source_result),
        }
    }
}

// #[no_mangle] allows this to be called from C/C++.
#[no_mangle]
unsafe extern "C-unwind" fn initialize_rust_plugins(plugin_portal: *mut mongodb_plugin_portal) {
    PluginAggregationStage::<EchoOxide>::register(plugin_portal);
    PluginAggregationStage::<AddSomeCrabs>::register(plugin_portal);
}
