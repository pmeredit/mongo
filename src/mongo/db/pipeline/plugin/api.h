#pragma once

#include <stddef.h>

extern "C" {

struct MongoExtensionByteBuf;

enum MongoDBPluginVersion {
    MONGODB_PLUGIN_VERSION_0 = 0,
};

enum mongodb_get_next_result {
    GET_NEXT_ADVANCED = 0,
    GET_NEXT_EOF = -1,
    GET_NEXT_PAUSE_EXECUTION = -2,
};

/**
 * A read-only view of a byte array.
 */
struct MongoExtensionByteView {
    const unsigned char* data;
    size_t len;
};

/**
 * Virtual function table for MongoExtensionByteBuf.
 */
struct MongoExtensionByteBufVTable {
    /**
     * Drop `buf` and free any associate resources.
     */
    void (*drop)(MongoExtensionByteBuf* buf);

    /**
     * Get a read-only view of the contents of `buf`.
     */
    MongoExtensionByteView (*get)(const MongoExtensionByteBuf* buf);
};

/**
 * Prototype for an extension byte array buffer.
 *
 * MongoExtensionByteBuf owns the underlying buffer
 */
struct MongoExtensionByteBuf {
    const MongoExtensionByteBufVTable* vtable;
};

// A function to get data from a source stage.
//
// Return codes <= 0 are a mongodb_get_next_result, codes > 0 are errors.
// On GET_NEXT_ADVANCED (*result, *len) are filled with a BSON document, on error (result, len) may
// be filled with a utf8 error string. On non-zero codes (result, len) may be set to (NULL, 0).
typedef int (*mongodb_source_get_next)(void* source_ptr, const unsigned char** result, size_t* len);

struct MongoExtensionAggregationStageVTable;
/// An opaque type to be used with a C++ style polymorphic object.
struct MongoExtensionAggregationStage {
    const MongoExtensionAggregationStageVTable* vtable;
};

// Vtable for an aggregation stage provided by the plugin.
//
// To implement an aggregation stage, create a new struct where a pointer to this is the _first_
// member:
//
// ```c
// struct MyAggregationStage {
//   const MongoExtensionAggregationStageVTable* vtable;
//   // other state goes here.
// }
// ```
//
// Your aggregation stage parser will heap allocate a `MyAggregationStage` and return it as a
// `mongodb_aggregation_stage*`.
struct MongoExtensionAggregationStageVTable {
    // Get the next result from stage and typically filling result. Memory pointed to by result is
    // owned by the stage and only valid until the next call on stage.
    //
    // Returns GET_NEXT_ADVANCED (0) on success fills result with a binary coded bson document.
    // Returns GET_NEXT_EOF if exhausted and fills result an empty view.
    // Returns GET_NEXT_PAUSE_EXECUTION if there are no results now but there may be in the future,
    // and fills result with an empty view. If a source stage returns this it must be propagated.
    //
    // Any positive value indicates an error. result will be filled with a utf8 string
    // describing the error.
    int (*get_next)(MongoExtensionAggregationStage* stage, MongoExtensionByteView* result);

    // Set a source pointer and a source function for intermediate stages.
    void (*set_source)(MongoExtensionAggregationStage* stage,
                       void* source_ptr,
                       mongodb_source_get_next source_get_next);

    // Close this stage and free any memory associated with it. It is an error to use stage after
    // closing.
    // TODO: rename to drop and make it first in the vtable ABI.
    void (*close)(MongoExtensionAggregationStage* stage);
};

// De-sugar a stage into other stages.
//
// stageBson contains a BSON document with a single (stageName, stageDefinition) element tuple.
//
// On a return code of zero *result contains a new BSON document with a single element tuple.
// The tuple is keyed by the input stage name; the value is a stage definition or array of stage
// definitions that will be created.
//
// NB: desugaring should form a DAG of stages -- desugaring a stage $foo should not generate another
// $foo or a stage that may desugar into $foo or demons may fly out of your nose.
typedef int (*MongoExtensionParseDesugarStage)(MongoExtensionByteView stageBson,
                                               MongoExtensionByteBuf** result);

// Create a concrete aggregation stage from stageBson.
//
// stageBson contains a BSON document with a single (stageName, stageDefinition) element tuple.
// contextBson contains a BSON document with additional context:
// - namespace (object)
//   - db ("string"; serialized NamespaceString)
//   - collection (string; optional)
//   - collectionUUID (UUID; optional)
// - inRouter (bool)
//
// On a return code of zero, fills *stage with an object owned by the caller, otherwise fills *error
// with an object owned by the caller.
typedef int (*MongoExtensionParseAggregationStage)(MongoExtensionByteView stageBson,
                                                   MongoExtensionByteView contextBson,
                                                   MongoExtensionAggregationStage** stage,
                                                   MongoExtensionByteBuf** error);

// The portal allows plugin functionality to register with the server.
struct MongoExtensionPortal {
    // Supported version of the plugin API.
    int version;

    // Register a de-sugaring stage.
    //
    // De-sugar stages do not have a concrete implementation and instead expand to one or more other
    // stages based on the stage definition.
    void (*add_desugar_stage)(MongoExtensionByteView name, MongoExtensionParseDesugarStage parser);

    // Invoke to add an aggregation stage. The `parser` function is responsible for parsing the
    // stage from a bson value and creating a mongodb_aggregation_stage object.
    void (*add_aggregation_stage)(MongoExtensionByteView name,
                                  MongoExtensionParseAggregationStage parser);
};

}  // extern "C"