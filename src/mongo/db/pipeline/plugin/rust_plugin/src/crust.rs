//! The crust contains functions exposed for C <-> rust plugin bindings.
//!
//! These functions are called by macro generated code that explicitly references an aggregation
//! stage in order to match extern ABI requirements.

use super::{AggregationSource, AggregationStage, GetNextResult, PluginAggregationStage};
use plugin_api_bindgen::*;
use std::ffi::{c_int, c_void};

pub unsafe fn get_next<S: AggregationStage>(
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
            mongodb_get_next_result_GET_NEXT_EOF
        }
        Ok(GetNextResult::PauseExecution) => {
            *result = std::ptr::null();
            *result_len = 0;
            mongodb_get_next_result_GET_NEXT_PAUSE_EXECUTION
        }
        Ok(GetNextResult::Advanced(doc)) => {
            *result = doc.as_bytes().as_ptr();
            *result_len = doc.as_bytes().len();
            mongodb_get_next_result_GET_NEXT_ADVANCED
        }
        Err(super::Error { code, message }) => {
            // XXX we leak memory right here. this memory needs to be owned by the stage.
            rust_stage.buf = message.into_bytes();
            *result = rust_stage.buf.as_ptr();
            *result_len = rust_stage.buf.len();
            code.get()
        }
    }
}

pub unsafe fn set_source<S: AggregationStage>(
    stage: *mut mongodb_aggregation_stage,
    source_ptr: *mut c_void,
    source_get_next: mongodb_source_get_next,
) {
    let rust_stage = (stage as *mut PluginAggregationStage<S>)
        .as_mut()
        .expect("non-null stage pointer");
    rust_stage.set_source(AggregationSource::new(source_ptr, source_get_next))
}

pub unsafe fn close<S: AggregationStage>(stage: *mut mongodb_aggregation_stage) {
    let rust_stage = Box::from_raw(stage as *mut PluginAggregationStage<S>);
    drop(rust_stage);
}
