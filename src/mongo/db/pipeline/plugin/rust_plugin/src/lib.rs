//! Extension SDK and sample implementation for MongoDB.
//!
//! This crate contains an [`sdk`] module that wraps the extension header provided by the database
//! and provides a mechanism to define and register new aggregation stages.
//!
//! It also provides several stage implementations, the stages in [`count_nodes`], [`crabs`],
//! [`custom_sort`], and [`echo`] are all toy examples; other modules contain reimplementations of
//! various search-related stages.
//!
//! To navigate through all of the provided abstractions it would be best to either start with the
//! [`sdk`] module or [`ExtensionPortal`]. If you implement a new stage the easiest way to get it
//! working with `mongod` is to register it in [`initialize_rust_plugins`].

// Dead code analysis doesn't work here.
// The entry point is an exported function called from a .cpp file.
#![allow(dead_code)]

pub mod command_service;
pub mod count_nodes;
pub mod crabs;
pub mod conjure;
pub mod custom_sort;
pub mod echo;
pub mod meta;
pub mod mongot_client;
pub mod sdk;
pub mod search;
pub mod vector;
pub mod voyage;

use std::sync::Arc;

use plugin_api_bindgen::MongoExtensionPortal;

use crate::count_nodes::CountNodesDescriptor;
use crate::crabs::{AddSomeCrabsDescriptor, EchoWithSomeCrabsDescriptor, HelloWorldWithFiveCrabsDescriptor};
use crate::conjure::ConjureDescriptor;
use crate::custom_sort::PluginSortDescriptor;
use crate::echo::EchoOxideDescriptor;
use crate::meta::{InternalPluginMetaDescriptor, PluginMetaDescriptor};
use crate::mongot_client::MongotClientState;
use crate::sdk::{AggregationStageDescriptor, ExtensionPortal};
use crate::search::{InternalPluginSearchDescriptor, PluginSearchDescriptor};
use crate::vector::{InternalPluginVectorSearchDescriptor, PluginVectorSearchDescriptor};
use crate::voyage::VoyageRerankDescriptor;

/// A lazily initialized `tokio::runtime::Runtime`.
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
    /// Create a new runtime with `num_threads` and associate thread `name`.
    ///
    /// The underlying runtime will not be created until the first call to [`get`](Self::get).
    pub fn new(name: &'static str, num_threads: usize) -> Self {
        Self {
            name,
            num_threads,
            runtime: std::sync::OnceLock::new(),
        }
    }

    /// Get the runtime, creating it if needed.
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

/// This is the entry point function invoked by the extension host (server) to hook into the server
/// and provide functionality like new aggregation stages.
///
/// This symbol is unmangled which would allow the server to locate it using `dlsym()` after
/// using `dlopen()` to access a shared object. Today it is statically linked.
///
/// At the moment modifying this function is the easiest way to register a new stage.
#[no_mangle]
pub unsafe extern "C-unwind" fn initialize_rust_plugins(portal_ptr: *mut MongoExtensionPortal) {
    let mut sdk_portal =
        ExtensionPortal::from_raw(portal_ptr).expect("extension portal pointer may not be null");
    sdk_portal.install_host_services();
    let mongot_client_state = Arc::new(MongotClientState::new(4));

    sdk_portal.register_source_aggregation_stage(EchoOxideDescriptor);
    sdk_portal.register_source_aggregation_stage(CountNodesDescriptor);
    sdk_portal.register_transform_aggregation_stage(AddSomeCrabsDescriptor);
    sdk_portal.register_desugar_aggregation_stage(ConjureDescriptor);
    sdk_portal.register_desugar_aggregation_stage(EchoWithSomeCrabsDescriptor);
    sdk_portal.register_desugar_aggregation_stage(HelloWorldWithFiveCrabsDescriptor);
    sdk_portal.register_transform_aggregation_stage(PluginSortDescriptor);
    sdk_portal.register_desugar_aggregation_stage(PluginSearchDescriptor);
    sdk_portal.register_source_aggregation_stage(InternalPluginSearchDescriptor::new(Arc::clone(
        &mongot_client_state,
    )));
    sdk_portal.register_desugar_aggregation_stage(PluginMetaDescriptor);
    sdk_portal.register_source_aggregation_stage(InternalPluginMetaDescriptor::new(Arc::clone(
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
