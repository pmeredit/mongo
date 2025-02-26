// Dead code analysis doesn't work here.
// The entry point is an exported function called from a .cpp file.
#![allow(dead_code)]

use std::ffi::{c_int, c_void};
use std::num::NonZero;

use bson::{to_raw_document_buf, Document, RawBsonRef, RawDocument, RawDocumentBuf};

use plugin_api_bindgen::{
    mongodb_aggregation_stage, mongodb_aggregation_stage_vt, mongodb_source_get_next,
    MongoExtensionByteBuf, MongoExtensionByteBufVTable, MongoExtensionByteView,
    MongoExtensionPortal,
};

#[derive(Debug)]
pub struct Error {
    pub code: NonZero<i32>,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + 'static>>,
}

impl Error {
    pub fn new(code: i32, message: String) -> Self {
        Self {
            code: NonZero::new(code).unwrap(),
            message,
            source: None,
        }
    }

    pub fn with_source(
        code: i32,
        message: String,
        source: Box<dyn std::error::Error + 'static>,
    ) -> Self {
        Self {
            code: NonZero::new(code).unwrap(),
            message,
            source: Some(source),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        if let Some(source) = self.source.as_ref() {
            std::write!(
                f,
                "Plugin error code {}: {} (source: {})",
                self.code.get(),
                self.message,
                source
            )
        } else {
            std::write!(f, "Plugin error code {}: {}", self.code.get(), self.message)
        }
    }
}

const VEC_BYTE_BUF_VTABLE: MongoExtensionByteBufVTable = MongoExtensionByteBufVTable {
    drop: Some(VecByteBuf::drop),
    get: Some(VecByteBuf::get),
};

struct VecByteBuf {
    vtable: &'static MongoExtensionByteBufVTable,
    buf: Vec<u8>,
}

impl VecByteBuf {
    pub fn from_string(s: String) -> Box<Self> {
        Box::new(Self {
            vtable: &VEC_BYTE_BUF_VTABLE,
            buf: s.into(),
        })
    }

    pub fn from_vec(v: Vec<u8>) -> Box<Self> {
        Box::new(Self {
            vtable: &VEC_BYTE_BUF_VTABLE,
            buf: v,
        })
    }

    unsafe extern "C-unwind" fn drop(buf: *mut MongoExtensionByteBuf) {
        let _ = Box::from_raw(buf as *mut VecByteBuf);
    }

    unsafe extern "C-unwind" fn get(buf: *const MongoExtensionByteBuf) -> MongoExtensionByteView {
        let bytes = (buf as *const VecByteBuf).as_ref().unwrap().buf.as_slice();
        MongoExtensionByteView {
            data: bytes.as_ptr(),
            len: bytes.len(),
        }
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
            _ => Err(Error::new(
                code,
                std::str::from_utf8(unsafe { std::slice::from_raw_parts(result_ptr, result_len) })
                    .unwrap()
                    .to_owned(),
            )),
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
    /// The stage definition is a bson value associated with the stage name.
    fn new(stage_definition: RawBsonRef<'_>) -> Result<Self, Error>;

    /// Set a source for this stage. Used by intermediate stages to fetch documents to transform.
    fn set_source(&mut self, source: AggregationSource);

    /// Get the next result from this stage.
    /// This may contain a document or another stream marker, including EOF.
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error>;
}

/// Wrapper around an [AggregationStage] that binds to the C plugin API.
#[repr(C)]
pub struct PluginAggregationStage<S: AggregationStage> {
    vtable: &'static mongodb_aggregation_stage_vt,
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
    const VTABLE: mongodb_aggregation_stage_vt = mongodb_aggregation_stage_vt {
        get_next: Some(Self::get_next),
        set_source: Some(Self::set_source),
        close: Some(Self::close),
    };

    pub fn register(portal: *mut MongoExtensionPortal) {
        let stage_name = S::name();
        let stage_view = MongoExtensionByteView {
            data: stage_name.as_bytes().as_ptr(),
            len: stage_name.as_bytes().len(),
        };
        unsafe {
            (*portal).add_aggregation_stage.expect("add stage")(
                stage_view,
                Some(Self::parse_external),
            );
        }
    }

    unsafe extern "C-unwind" fn parse_external(
        stage_bson: MongoExtensionByteView,
        stage: *mut *mut mongodb_aggregation_stage,
        error: *mut *mut MongoExtensionByteBuf,
    ) -> c_int {
        *stage = std::ptr::null_mut();
        *error = std::ptr::null_mut();
        match Self::parse(unsafe { std::slice::from_raw_parts(stage_bson.data, stage_bson.len) }) {
            Ok(stage_impl) => {
                let plugin_stage = Box::new(Self {
                    vtable: &Self::VTABLE,
                    stage_impl,
                    buf: vec![],
                });
                *stage = Box::into_raw(plugin_stage) as *mut mongodb_aggregation_stage;
                0
            }
            Err(Error { code, message, .. }) => {
                *error =
                    Box::into_raw(VecByteBuf::from_string(message)) as *mut MongoExtensionByteBuf;
                code.into()
            }
        }
    }

    /// Parse a generic [AggregationStage] from raw bson document bytes.
    fn parse(bson_doc_bytes: &[u8]) -> Result<S, Error> {
        let doc = RawDocument::from_bytes(bson_doc_bytes).map_err(|e| {
            Error::with_source(
                1,
                format!("Error parsing stage definition for {}", S::name()),
                Box::new(e),
            )
        })?;
        let element = doc
            .iter()
            .next()
            .map(|el| {
                el.map_err(|e| {
                    Error::with_source(
                        1,
                        format!("Error parsing stage definition for {}", S::name()),
                        Box::new(e),
                    )
                })
            })
            .unwrap_or(Err(Error::new(
                1,
                format!(
                    "Error parsing stage defintion for {}: no stage definition element present",
                    S::name()
                ),
            )))?;
        S::new(element.1)
    }

    unsafe extern "C-unwind" fn get_next(
        stage: *mut mongodb_aggregation_stage,
        result: *mut MongoExtensionByteView,
    ) -> c_int {
        let rust_stage = (stage as *mut PluginAggregationStage<S>)
            .as_mut()
            .expect("non-null stage pointer");
        *result = MongoExtensionByteView {
            data: std::ptr::null(),
            len: 0,
        };
        match rust_stage.get_next() {
            Ok(GetNextResult::EOF) => plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_EOF,
            Ok(GetNextResult::PauseExecution) => {
                plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_PAUSE_EXECUTION
            }
            Ok(GetNextResult::Advanced(doc)) => {
                *result = MongoExtensionByteView {
                    data: doc.as_bytes().as_ptr(),
                    len: doc.as_bytes().len(),
                };
                plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_ADVANCED
            }
            Err(Error { code, message, .. }) => {
                rust_stage.buf = message.into_bytes();
                *result = MongoExtensionByteView {
                    data: rust_stage.buf.as_ptr(),
                    len: rust_stage.buf.len(),
                };
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

    fn new(stage_definition: RawBsonRef<'_>) -> Result<Self, Error> {
        let document = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    format!("$echoOxide stage definition must contain a document."),
                ))
            }
        };
        Ok(Self {
            document,
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
    crabs: String,
    source: Option<AggregationSource>,
    last_document: RawDocumentBuf,
}

impl AggregationStage for AddSomeCrabs {
    fn name() -> &'static str {
        "$addSomeCrabs"
    }

    fn new(stage_definition: RawBsonRef<'_>) -> Result<Self, Error> {
        let num_crabs = match stage_definition {
            RawBsonRef::Int32(i) if i > 0 => i as usize,
            RawBsonRef::Int64(i) if i > 0 => i as usize,
            RawBsonRef::Double(i) if i >= 1.0 && i.fract() == 0.0 => i as usize,
            _ => {
                return Err(Error::new(
                    1,
                    "$addSomeCrabs should be followed with a positive integer value.".into(),
                ))
            }
        };
        Ok(Self {
            crabs: String::from_iter(std::iter::repeat('🦀').take(num_crabs)),
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
                doc.insert("someCrabs", self.crabs.clone());
                self.last_document = to_raw_document_buf(&doc).unwrap();
                Ok(GetNextResult::Advanced(self.last_document.as_ref()))
            }
            _ => Ok(source_result),
        }
    }
}

// #[no_mangle] allows this to be called from C/C++.
#[no_mangle]
unsafe extern "C-unwind" fn initialize_rust_plugins(portal: *mut MongoExtensionPortal) {
    PluginAggregationStage::<EchoOxide>::register(portal);
    PluginAggregationStage::<AddSomeCrabs>::register(portal);
}
