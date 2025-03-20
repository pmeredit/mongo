// Dead code analysis doesn't work here.
// The entry point is an exported function called from a .cpp file.
#![allow(dead_code)]

mod command_service;
mod desugar;
mod mongot_client;
mod search;
mod vector;
mod voyage;

use std::ffi::{c_int, c_void};
use std::num::NonZero;

use bson::{to_raw_document_buf, Document, RawBsonRef, RawDocument, RawDocumentBuf, Uuid};
use serde::{Deserialize, Serialize};

use plugin_api_bindgen::{
    mongodb_source_get_next, MongoExtensionAggregationStage, MongoExtensionAggregationStageVTable,
    MongoExtensionByteBuf, MongoExtensionByteBufVTable, MongoExtensionByteView,
    MongoExtensionPortal,
};

use crate::desugar::{EchoWithSomeCrabs, PluginDesugarAggregationStage};
use crate::search::{InternalPluginSearch, PluginSearch};
use crate::vector::{InternalPluginVectorSearch, PluginVectorSearch};
use crate::voyage::VoyageRerank;

#[derive(Debug)]
pub struct Error {
    pub code: NonZero<i32>,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + 'static>>,
}

impl Error {
    pub fn new<S: Into<String>>(code: i32, message: S) -> Self {
        Self {
            code: NonZero::new(code).unwrap(),
            message: message.into(),
            source: None,
        }
    }

    // TODO: source could be <E: Error + 'static> and this method could box it.
    pub fn with_source<S: Into<String>, E: std::error::Error + 'static>(
        code: i32,
        message: S,
        source: E,
    ) -> Self {
        Self {
            code: NonZero::new(code).unwrap(),
            message: message.into(),
            source: Some(Box::new(source)),
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

// TODO: structs like this that are cast to an extension type should have a lint that errors if
// they are not #[repr(C)]. When using the rust ABI they may be reordered(!) which is problematic
// given the C bindings expect vtable access.
#[repr(C)]
pub struct VecByteBuf {
    vtable: &'static MongoExtensionByteBufVTable,
    buf: Vec<u8>,
}

impl VecByteBuf {
    const VTABLE: MongoExtensionByteBufVTable = MongoExtensionByteBufVTable {
        drop: Some(VecByteBuf::drop),
        get: Some(VecByteBuf::get),
    };

    pub fn from_string(s: String) -> Box<Self> {
        Box::new(Self {
            vtable: &Self::VTABLE,
            buf: s.into(),
        })
    }

    pub fn from_vec(v: Vec<u8>) -> Box<Self> {
        Box::new(Self {
            vtable: &Self::VTABLE,
            buf: v,
        })
    }

    pub fn into_byte_buf(self: Box<Self>) -> *mut MongoExtensionByteBuf {
        Box::into_raw(self) as *mut MongoExtensionByteBuf
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

unsafe fn byte_view_as_slice<'a>(buf: MongoExtensionByteView) -> &'a [u8] {
    std::slice::from_raw_parts(buf.data, buf.len)
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

#[derive(Debug, Deserialize, Serialize)]
pub struct AggregationStageContext {
    // NB: this may contain a raw ObjectId so it probably shouldn't be a String.
    // This isn't an issue in the C++ code base because std::string can be an arbitrary byte buffer
    // and doesn't have to contain readable text in any character set.
    #[serde(rename = "$db")]
    pub db: String,
    pub collection: Option<String>,
    #[serde(rename = "collectionUUID")]
    pub collection_uuid: Option<Uuid>,
    #[serde(rename = "inRouter")]
    pub in_router: bool,
}

impl TryFrom<&RawDocument> for AggregationStageContext {
    type Error = Error;

    fn try_from(value: &RawDocument) -> Result<AggregationStageContext, Self::Error> {
        bson::from_slice(value.as_bytes())
            .map_err(|e| Error::with_source(1, "Could not parse AggregationStageContext", e))
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
    /// `context`` is a document containing additional data about this aggregation pipeline. It
    /// is convertible to [AggregationStageContext].
    // TODO: should we always parse Context? It depend on whether or not any non-trival agg stage
    // would need access to the parsed context.
    fn new(stage_definition: RawBsonRef<'_>, context: &RawDocument) -> Result<Self, Error>;

    /// Set a source for this stage. Used by intermediate stages to fetch documents to transform.
    fn set_source(&mut self, source: AggregationSource);

    /// Get the next result from this stage.
    /// This may contain a document or another stream marker, including EOF.
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error>;
}

/// Wrapper around an [AggregationStage] that binds to the C plugin API.
#[repr(C)]
pub struct PluginAggregationStage<S: AggregationStage> {
    vtable: &'static MongoExtensionAggregationStageVTable,
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
    const VTABLE: MongoExtensionAggregationStageVTable = MongoExtensionAggregationStageVTable {
        get_next: Some(Self::get_next),
        set_source: Some(Self::set_source),
        close: Some(Self::close),
    };

    pub fn register(portal: &mut MongoExtensionPortal) {
        let stage_name = S::name();
        let stage_view = MongoExtensionByteView {
            data: stage_name.as_bytes().as_ptr(),
            len: stage_name.as_bytes().len(),
        };
        unsafe {
            portal.add_aggregation_stage.expect("add stage")(
                stage_view,
                Some(Self::parse_external),
            );
        }
    }

    unsafe extern "C-unwind" fn parse_external(
        stage_bson: MongoExtensionByteView,
        context_bson: MongoExtensionByteView,
        stage: *mut *mut MongoExtensionAggregationStage,
        error: *mut *mut MongoExtensionByteBuf,
    ) -> c_int {
        let (stage_slice, context_slice) = unsafe {
            (
                byte_view_as_slice(stage_bson),
                byte_view_as_slice(context_bson),
            )
        };
        *stage = std::ptr::null_mut();
        *error = std::ptr::null_mut();
        match Self::parse(stage_slice, context_slice) {
            Ok(stage_impl) => {
                let plugin_stage = Box::new(Self {
                    vtable: &Self::VTABLE,
                    stage_impl,
                    buf: vec![],
                });
                *stage = Box::into_raw(plugin_stage) as *mut MongoExtensionAggregationStage;
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
    fn parse(stage_doc_bytes: &[u8], context_doc_bytes: &[u8]) -> Result<S, Error> {
        let stage_doc = RawDocument::from_bytes(stage_doc_bytes).map_err(|e| {
            Error::with_source(
                1,
                format!("Error parsing stage definition for {}", S::name()),
                e,
            )
        })?;
        let element = stage_doc
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
        let context_doc = RawDocument::from_bytes(context_doc_bytes).map_err(|e| {
            Error::with_source(
                1,
                format!("Error parsing context for stage {}", S::name()),
                e,
            )
        })?;
        S::new(element.1, context_doc)
    }

    unsafe extern "C-unwind" fn get_next(
        stage: *mut MongoExtensionAggregationStage,
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
        stage: *mut MongoExtensionAggregationStage,
        source_ptr: *mut c_void,
        source_get_next: mongodb_source_get_next,
    ) {
        let rust_stage = (stage as *mut PluginAggregationStage<S>)
            .as_mut()
            .expect("non-null stage pointer");
        rust_stage.set_source(AggregationSource::new(source_ptr, source_get_next))
    }

    unsafe extern "C-unwind" fn close(stage: *mut MongoExtensionAggregationStage) {
        let _ = Box::from_raw(stage as *mut PluginAggregationStage<S>);
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

    fn new(stage_definition: RawBsonRef<'_>, _context: &RawDocument) -> Result<Self, Error> {
        let document = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    "$echoOxide stage definition must contain a document.",
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

    fn new(stage_definition: RawBsonRef<'_>, _context: &RawDocument) -> Result<Self, Error> {
        let num_crabs = match stage_definition {
            RawBsonRef::Int32(i) if i > 0 => i as usize,
            RawBsonRef::Int64(i) if i > 0 => i as usize,
            RawBsonRef::Double(i) if i >= 1.0 && i.fract() == 0.0 => i as usize,
            _ => {
                return Err(Error::new(
                    1,
                    "$addSomeCrabs should be followed with a positive integer value.",
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
unsafe extern "C-unwind" fn initialize_rust_plugins(portal_ptr: *mut MongoExtensionPortal) {
    let portal = portal_ptr
        .as_mut()
        .expect("extension portal pointer may not be null!");
    PluginAggregationStage::<EchoOxide>::register(portal);
    PluginAggregationStage::<AddSomeCrabs>::register(portal);
    PluginDesugarAggregationStage::<EchoWithSomeCrabs>::register(portal);
    PluginAggregationStage::<InternalPluginSearch>::register(portal);
    PluginDesugarAggregationStage::<PluginSearch>::register(portal);
    PluginAggregationStage::<InternalPluginVectorSearch>::register(portal);
    PluginDesugarAggregationStage::<PluginVectorSearch>::register(portal);
    PluginAggregationStage::<VoyageRerank>::register(portal);
}
