//! The crust contains functions exposed for C <-> rust plugin bindings.
//!
//! These functions are called by macro generated code that explicitly references an aggregation
//! stage in order to match extern ABI requirements.

use super::{AggregationStage, GetNextResult, PluginAggregationStage};
use plugin_api_bindgen::*;
use std::os::raw::{c_char, c_int};

pub unsafe fn get_next<S: AggregationStage>(
    stage: *mut mongodb_aggregation_stage,
    result: *mut *const c_char,
    result_len: *mut usize,
) -> c_int {
    let rust_stage = (stage as *mut PluginAggregationStage<S>)
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

pub unsafe fn close<S: AggregationStage>(stage: *mut mongodb_aggregation_stage) {
    let rust_stage = Box::from_raw(stage as *mut PluginAggregationStage<S>);
    drop(rust_stage);
}
