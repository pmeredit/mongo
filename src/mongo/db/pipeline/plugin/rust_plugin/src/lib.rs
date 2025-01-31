// Dead code analysis doesn't work here at all.
#[allow(dead_code)]

use std::ffi::CStr;
use std::os::raw::{c_char, c_int};

use plugin_api_bindgen::{mongodb_aggregation_stage,mongodb_plugin_portal};

const STAGE_NAME: &CStr = c"$echoOxide";
const NOT_A_DOCUMENT_ERROR: &CStr = c"$echoOxide argument must be document typed.";

#[repr(C)]
struct EchoOxideAggregationStage {
    stage: mongodb_aggregation_stage,
    document: Vec<u8>,
    exhausted: bool,
}

impl EchoOxideAggregationStage {
    pub fn new(document: Vec<u8>) -> Self {
        Self {
            stage: mongodb_aggregation_stage {
                get_next: Some(echo_oxide_stage_get_next),
                close: Some(echo_oxide_stage_close),
            },
            document,
            exhausted: false,
        }
    }
}

unsafe extern "C-unwind" fn echo_oxide_stage_get_next(
    stage: *mut mongodb_aggregation_stage,
    result: *mut *const c_char,
    result_len: *mut usize) -> c_int {
    let rust_stage = (stage as *mut EchoOxideAggregationStage).as_mut().expect("non-null stage pointer");
    if rust_stage.exhausted {
        plugin_api_bindgen::MongoDBAggregationStageGetNextResult_GET_NEXT_EOF
    } else {
        // Safety: rust_stage.document must live until the next call.
        // TODO: use unsigned char, it's just bytes!
        *result = rust_stage.document.as_ptr() as *const c_char;
        *result_len = rust_stage.document.len();
        rust_stage.exhausted = true;
        plugin_api_bindgen::MongoDBAggregationStageGetNextResult_GET_NEXT_ADVANCED
    }
}

unsafe extern "C-unwind" fn echo_oxide_stage_close(stage: *mut mongodb_aggregation_stage) {
    let rust_stage = Box::from_raw(stage);
    drop(rust_stage);
}

unsafe extern "C-unwind" fn echo_oxide_stage_parse(
    bson_type: i8,
    bson_value: *const c_char,
    bson_value_len: usize,
    stage: *mut *mut mongodb_aggregation_stage,
    error: *mut *const c_char,
    error_len: *mut usize) -> c_int {
    if bson_type != 3 { // 3 is embedded document
        *error = NOT_A_DOCUMENT_ERROR.to_bytes().as_ptr() as *const c_char;
        *error_len = NOT_A_DOCUMENT_ERROR.to_bytes().len();
        return 1;
    }

    *error = std::ptr::null();
    *error_len = 0;
    let document = std::slice::from_raw_parts(bson_value as *const u8, bson_value_len).to_owned();
    let rust_stage = Box::new(EchoOxideAggregationStage::new(document));
    *stage = Box::into_raw(rust_stage) as *mut mongodb_aggregation_stage;
    0
}

// #[no_mangle] allows this to be called from C/C++.
#[no_mangle]
unsafe extern "C-unwind" fn initialize_rust_plugins(plugin_portal: *mut mongodb_plugin_portal) {
    (*plugin_portal).add_aggregation_stage.expect("add stage")(
        STAGE_NAME.to_bytes().as_ptr() as *const c_char,
        STAGE_NAME.to_bytes().len(),
        Some(echo_oxide_stage_parse));
}