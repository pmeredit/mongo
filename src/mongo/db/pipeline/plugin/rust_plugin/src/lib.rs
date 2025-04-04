// Dead code analysis doesn't work here.
// The entry point is an exported function called from a .cpp file.
#![allow(dead_code)]

mod command_service;
mod crabs;
mod custom_sort;
mod echo;
mod mongot_client;
pub mod sdk;
mod search;
mod vector;
mod voyage;

use std::borrow::Cow;
use std::ffi::{c_int, c_void};
use std::num::NonZero;
use std::sync::Arc;

use bson::{doc, to_vec};
use bson::{Bson, Document, RawDocument, Uuid};
use serde::{Deserialize, Serialize};

use plugin_api_bindgen::{
    mongodb_source_get_next, MongoExtensionAggregationStage, MongoExtensionAggregationStageVTable,
    MongoExtensionByteBuf, MongoExtensionByteBufVTable, MongoExtensionByteView,
    MongoExtensionPortal,
};

use crate::crabs::{AddSomeCrabsDescriptor, EchoWithSomeCrabsDescriptor};
use crate::custom_sort::PluginSortDescriptor;
use crate::echo::EchoOxideDescriptor;
use crate::mongot_client::MongotClientState;
use crate::sdk::{AggregationStageDescriptor, ExtensionPortal};
use crate::search::{InternalPluginSearchDescriptor, PluginSearchDescriptor};
use crate::vector::{InternalPluginVectorSearchDescriptor, PluginVectorSearchDescriptor};
use crate::voyage::VoyageRerankDescriptor;

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

#[derive(Debug)]
pub enum GetNextResult<'a> {
    Advanced(Cow<'a, RawDocument>),
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
                    .unwrap()
                    .into(),
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregationStageContext {
    // NB: this may contain a raw ObjectId so it probably shouldn't be a String.
    // This isn't an issue in the C++ code base because std::string can be an arbitrary byte buffer
    // and doesn't have to contain readable text in any character set.
    #[serde(rename = "$db")]
    pub db: String,
    pub collection: Option<String>,
    #[serde(rename = "collectionUUID")]
    pub collection_uuid: Option<Uuid>,
    pub in_router: bool,
    // TODO remove mongotHost in favor of extension-specific config
    pub mongot_host: Option<String>
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

    /// Set a source for this stage. Used by intermediate stages to fetch documents to transform.
    fn set_source(&mut self, source: AggregationSource);

    /// Get the next result from this stage.
    /// This may contain a document or another stream marker, including EOF.
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error>;

    // Get a merging pipeline, represented as a vector of bson documents, that describes logic
    // by which to merge streams during distributed query execution. This is only called if query
    // planning would like to attempt to push the plugin stage to the shards.
    // The default implementation is an empty vector, meaing no merging logic is necessary.
    fn get_merging_stages(&mut self) -> Result<Vec<Document>, Error> {
        Ok(vec![])
    }
}

/// Wrapper around an [AggregationStage] that binds to the C plugin API.
#[repr(C)]
pub struct PluginAggregationStage<S: AggregationStage> {
    vtable: &'static MongoExtensionAggregationStageVTable,
    stage_impl: S,
    // This is a place to put errors or other things that might otherwise leak.
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
        get_merging_stages: Some(Self::get_merging_stages),
        close: Some(Self::close),
    };

    pub fn new(stage: S) -> Self {
        Self {
            vtable: &Self::VTABLE,
            stage_impl: stage,
            buf: vec![],
        }
    }

    pub fn into_raw_interface(self: Box<Self>) -> *mut MongoExtensionAggregationStage {
        Box::into_raw(self) as *mut MongoExtensionAggregationStage
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
                let doc_bytes = match doc {
                    Cow::Owned(rdoc_buf) => {
                        rust_stage.buf = rdoc_buf.into_bytes();
                        rust_stage.buf.as_slice()
                    }
                    Cow::Borrowed(rdoc) => rdoc.as_bytes(),
                };
                *result = MongoExtensionByteView {
                    data: doc_bytes.as_ptr(),
                    len: doc_bytes.len(),
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

    unsafe extern "C-unwind" fn get_merging_stages(
        stage: *mut MongoExtensionAggregationStage,
        result: *mut *mut MongoExtensionByteBuf,
    ) {
        let rust_stage = (stage as *mut PluginAggregationStage<S>)
            .as_mut()
            .expect("non-null stage pointer");
        let merging_stages = rust_stage.get_merging_stages().unwrap();
        let merging_stages_doc = doc! {
            "mergingStages":
            Bson::Array(
                merging_stages
                    .into_iter()
                    .map(Bson::from)
                    .collect(),
            ),
        };
        match to_vec(&merging_stages_doc)
            .map(VecByteBuf::from_vec)
            .map_err(|e| {
                Error::with_source(
                    1,
                    format!("Error parsing merging stages for stage {}", S::name()),
                    e,
                )
            }) {
            Ok(buf) => *result = buf.into_byte_buf(),
            Err(e) => *result = VecByteBuf::from_string(e.to_string()).into_byte_buf(),
        }
    }

    unsafe extern "C-unwind" fn close(stage: *mut MongoExtensionAggregationStage) {
        let _ = Box::from_raw(stage as *mut PluginAggregationStage<S>);
    }
}

// #[no_mangle] allows this to be called from C/C++.
#[no_mangle]
unsafe extern "C-unwind" fn initialize_rust_plugins(portal_ptr: *mut MongoExtensionPortal) {
    let mut sdk_portal =
        ExtensionPortal::from_raw(portal_ptr).expect("extension portal pointer may not be null");
    let mongot_client_state = Arc::new(MongotClientState::new(4));

    sdk_portal.register_source_aggregation_stage(EchoOxideDescriptor);
    sdk_portal.register_transform_aggregation_stage(AddSomeCrabsDescriptor);
    sdk_portal.register_desugar_aggregation_stage(EchoWithSomeCrabsDescriptor);
    sdk_portal.register_transform_aggregation_stage(PluginSortDescriptor);
    sdk_portal.register_desugar_aggregation_stage(PluginSearchDescriptor);
    sdk_portal.register_source_aggregation_stage(InternalPluginSearchDescriptor::new(Arc::clone(
        &mongot_client_state,
    )));
    sdk_portal.register_desugar_aggregation_stage(PluginVectorSearchDescriptor);
    sdk_portal.register_source_aggregation_stage(InternalPluginVectorSearchDescriptor::new(
        Arc::clone(&mongot_client_state),
    ));
    if let Ok(api_key) = std::env::var("VOYAGE_API_KEY") {
        sdk_portal.register_transform_aggregation_stage(VoyageRerankDescriptor::new(4, api_key));
    } else {
        eprintln!(
            "Skipping registration of {}; VOYAGE_API_KEY unset",
            VoyageRerankDescriptor::name()
        );
    }
}
