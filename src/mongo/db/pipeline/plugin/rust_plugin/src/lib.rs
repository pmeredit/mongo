// Dead code analysis doesn't work here.
// The entry point is an exported function called from a .cpp file.
#![allow(dead_code)]

mod command_service;
mod crabs;
mod custom_sort;
mod count_nodes;
mod echo;
mod mongot_client;
pub mod sdk;
mod search;
mod vector;
mod voyage;

use std::borrow::Cow;
use std::ffi::c_int;
use std::sync::Arc;

use bson::doc;
use bson::{RawDocument, Uuid};
use serde::{Deserialize, Serialize};

use plugin_api_bindgen::{
    MongoExtensionAggregationStage, MongoExtensionAggregationStageVTable, MongoExtensionByteBuf,
    MongoExtensionByteBufVTable, MongoExtensionByteView, MongoExtensionError, MongoExtensionPortal,
};

use crate::crabs::{AddSomeCrabsDescriptor, EchoWithSomeCrabsDescriptor};
use crate::custom_sort::PluginSortDescriptor;
use crate::echo::EchoOxideDescriptor;
use crate::count_nodes::CountNodesDescriptor;
use crate::mongot_client::MongotClientState;
use crate::sdk::{AggregationStageDescriptor, Error, ExtensionPortal};
use crate::search::{InternalPluginSearchDescriptor, PluginSearchDescriptor};
use crate::vector::{InternalPluginVectorSearchDescriptor, PluginVectorSearchDescriptor};
use crate::voyage::VoyageRerankDescriptor;

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

// TODO handle unknown codes as another state.
#[derive(Debug)]
pub enum GetNextResult<'a> {
    Advanced(Cow<'a, RawDocument>),
    PauseExecution,
    EOF,
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
    pub mongot_host: Option<String>,
}

impl TryFrom<&RawDocument> for AggregationStageContext {
    type Error = Error;

    fn try_from(value: &RawDocument) -> Result<AggregationStageContext, Self::Error> {
        bson::from_slice(value.as_bytes())
            .map_err(|e| Self::Error::with_source(1, "Could not parse AggregationStageContext", e))
    }
}

/// Trait for a stage in a MongoDB aggregation pipeline.
///
/// A struct that implements this interfaces can be used with [PluginAggregationStage] to register
/// with the C plugin API without writing any additional unsafe code.
pub trait AggregationStage {
    /// Get the next result from this stage.
    /// This may contain a document or another stream marker, including EOF.
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error>;
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
        drop: Some(Self::drop),
        getNext: Some(Self::get_next),
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
        code: *mut c_int,
        doc: *mut MongoExtensionByteView,
    ) -> *mut MongoExtensionError {
        let rust_stage = (stage as *mut PluginAggregationStage<S>)
            .as_mut()
            .expect("non-null stage pointer");
        *doc = MongoExtensionByteView {
            data: std::ptr::null(),
            len: 0,
        };
        match rust_stage.get_next() {
            Ok(GetNextResult::EOF) => {
                *code = plugin_api_bindgen::MongoExtensionGetNextResultCode_kEOF;
                std::ptr::null_mut()
            }
            Ok(GetNextResult::PauseExecution) => {
                *code = plugin_api_bindgen::MongoExtensionGetNextResultCode_kPauseExecution;
                std::ptr::null_mut()
            }
            Ok(GetNextResult::Advanced(raw_doc)) => {
                let doc_bytes = match raw_doc {
                    Cow::Owned(rdoc_buf) => {
                        rust_stage.buf = rdoc_buf.into_bytes();
                        rust_stage.buf.as_slice()
                    }
                    Cow::Borrowed(rdoc) => rdoc.as_bytes(),
                };
                *doc = MongoExtensionByteView {
                    data: doc_bytes.as_ptr(),
                    len: doc_bytes.len(),
                };
                *code = plugin_api_bindgen::MongoExtensionGetNextResultCode_kAdvanced;
                std::ptr::null_mut()
            }
            Err(e) => e.into_raw_interface(),
        }
    }

    unsafe extern "C-unwind" fn drop(stage: *mut MongoExtensionAggregationStage) {
        let _ = Box::from_raw(stage as *mut PluginAggregationStage<S>);
    }
}

/// A lazily initialized [`tokio::runtime::Runtime`].
///
/// The current initialization path in the server invokes plugin registration _before_ it is safe to
/// start new threads, so this wrapper allows us to defer initialization until the threads are
/// actually needed. In the long run this may not be necessary.
pub struct LazyRuntime {
    name: &'static str,
    num_threads: usize,
    runtime: std::sync::OnceLock<tokio::runtime::Runtime>,
}

impl LazyRuntime {
    pub fn new(name: &'static str, num_threads: usize) -> Self {
        Self {
            name,
            num_threads,
            runtime: std::sync::OnceLock::new(),
        }
    }

    pub fn get(&self) -> &tokio::runtime::Runtime {
        self.runtime.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(self.num_threads)
                .thread_name(self.name)
                .on_thread_park(|| {
                    crate::sdk::ExtensionHostServices::begin_idle_thread_block(
                        idle_thread_block_location!(),
                    )
                })
                .on_thread_unpark(crate::sdk::ExtensionHostServices::end_idle_thread_block)
                .enable_io()
                .build()
                .unwrap()
        })
    }
}

// #[no_mangle] allows this to be called from C/C++.
#[no_mangle]
unsafe extern "C-unwind" fn initialize_rust_plugins(portal_ptr: *mut MongoExtensionPortal) {
    let mut sdk_portal =
        ExtensionPortal::from_raw(portal_ptr).expect("extension portal pointer may not be null");
    let mongot_client_state = Arc::new(MongotClientState::new(4));

    sdk_portal.register_source_aggregation_stage(EchoOxideDescriptor);
    sdk_portal.register_source_aggregation_stage(CountNodesDescriptor);
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
