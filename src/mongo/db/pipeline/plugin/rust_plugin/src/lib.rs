// Dead code analysis doesn't work here.
// The entry point is an exported function called from a .cpp file.
#![allow(dead_code)]

mod command_service;
mod count_nodes;
mod crabs;
mod custom_sort;
mod echo;
mod meta;
mod mongot_client;
pub mod sdk;
mod search;
mod vector;
mod voyage;

use std::sync::Arc;

use plugin_api_bindgen::MongoExtensionPortal;

use crate::count_nodes::CountNodesDescriptor;
use crate::crabs::{AddSomeCrabsDescriptor, EchoWithSomeCrabsDescriptor};
use crate::custom_sort::PluginSortDescriptor;
use crate::echo::EchoOxideDescriptor;
use crate::meta::{InternalPluginMetaDescriptor, PluginMetaDescriptor};
use crate::mongot_client::MongotClientState;
use crate::sdk::{AggregationStageDescriptor, ExtensionPortal};
use crate::search::{InternalPluginSearchDescriptor, PluginSearchDescriptor};
use crate::vector::{InternalPluginVectorSearchDescriptor, PluginVectorSearchDescriptor};
use crate::voyage::VoyageRerankDescriptor;

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
    sdk_portal.install_host_services();
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
