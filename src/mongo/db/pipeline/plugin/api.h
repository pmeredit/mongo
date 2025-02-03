#pragma once

#include <stddef.h>

extern "C" {
enum MongoDBPluginVersion {
    MONGODB_PLUGIN_VERSION_0 = 0,
};

enum mongodb_get_next_result {
    GET_NEXT_ADVANCED = 0,
    GET_NEXT_EOF = -1,
    GET_NEXT_PAUSE_EXECUTION = -2,
};

// A function to get data from a source stage.
//
// Return codes <= 0 are a mongodb_get_next_result, codes > 0 are errors.
// On GET_NEXT_ADVANCED (*result, *len) are filled with a BSON document, on error (result, len) may
// be filled with a utf8 error string. On non-zero codes (result, len) may be set to (NULL, 0).
typedef int (*mongodb_source_get_next)(void* source_ptr, const unsigned char** result, size_t* len);

// An aggregation stage provided by the plugin.
//
// To implement an aggregation stage, create a new struct where this is the _first_ member:
//
// ```c
// struct MyAggregationStage {
//   mongodb_aggregation_stage stage;
//   // other state goes here.
// }
// ```
//
// Your aggregation stage parser will heap allocate a `MyAggregationStage` and return it as a
// `mongodb_aggregation_stage*`.
struct mongodb_aggregation_stage {
    // Get the next result from stage and typically filling (result, result_len). Memory pointed to
    // by result is owned by the stage and only valid until the next call on stage.
    //
    // Returns GET_NEXT_ADVANCED (0) on success filling (result, result_len) with a binary coded
    // bson document. Returns GET_NEXT_EOF if exhausted and fills (result, result_len) with (NULL,
    // 0). Returns GET_NEXT_WPAUSE_EXECUTION if there are no results now but there may be in the
    // future. This also fills (result, result_len) with (NULL, 0). If a source stage returns this
    // it must be propagated.
    //
    // Any positive value indicates an error. (result, result_len) will be filled with a utf8 string
    // describing the error.
    int (*get_next)(mongodb_aggregation_stage* stage,
                    const unsigned char** result,
                    size_t* result_len);

    // Set a source pointer and a source function for intermediate stages.
    void (*set_source)(mongodb_aggregation_stage* stage,
                       void* source_ptr,
                       mongodb_source_get_next source_get_next);

    // Close this stage and free any memory associated with it. It is an error to use stage after
    // closing.
    void (*close)(mongodb_aggregation_stage* stage);
};

typedef int (*mongodb_parse_aggregation_stage)(unsigned char bson_type,
                                               const unsigned char* bson_value,
                                               size_t bson_value_len,
                                               mongodb_aggregation_stage** stage,
                                               const unsigned char** error,
                                               size_t* error_len);

// The plugin portal allows plugin functionality to register with the server.
struct mongodb_plugin_portal {
    // Supported version of the plugin API.
    int version;

    // Invoke to add an aggregation stage. The `parser` function is responsible for parsing the
    // stage from a bson value and creating a mongodb_aggregation_stage object.
    void (*add_aggregation_stage)(const unsigned char* name,
                                  size_t name_len,
                                  mongodb_parse_aggregation_stage parser);
};

}  // extern "C"