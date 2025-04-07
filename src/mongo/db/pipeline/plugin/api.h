#pragma once

#include <stddef.h>

extern "C" {

struct MongoExtensionByteBufVTable;
struct MongoExtensionAggregationStage;
struct MongoExtensionAggregationStageVTable;
struct MongoExtensionAggregationStageDescriptor;
struct MongoExtensionAggregationStageDescriptorVTable;
struct MongoExtensionBoundAggregationStageDescriptor;
struct MongoExtensionBoundAggregationStageDescriptorVTable;

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
 * Prototype for an extension byte array buffer.
 *
 * MongoExtensionByteBuf owns the underlying buffer
 */
struct MongoExtensionByteBuf {
    const MongoExtensionByteBufVTable* vtable;
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
 * Types of aggregation stages that can be implemented as an extension.
 */
enum MongoExtensionAggregationStageType {
    /**
     * Source stages create documents by reading them from local or remote storage.
     */
    kSource = 0,
    /**
     * Transform stages consume input from another stage and transforms it.
     *
     * The definition of "transform" is quite loose: the transforms need not be 1-to-1, they could
     * summarize the input as a single document, filter, or re-order the input stream.
     */
    kTransform = 1,
    /**
     * Desugaring stages decompose into a pipeline of 1 or more stages.
     *
     * The stages generated from desugaring may reference stages that appear in the public
     * aggregation stage documentation or from other extensions.
     */
    kDesugar = 2,
};

/**
 * An AggregationStageDescriptor describes features of a stage that are not bound to the stage
 * definition. This object functions as a factory to create bound stage definitions.
 *
 * These objects are owned by extensions so no method is provided to free them.
 */
struct MongoExtensionAggregationStageDescriptor {
    const MongoExtensionAggregationStageDescriptorVTable* vtable;
};

/**
 * Virtual function table for MongoExtensionAggregationStageDescriptor.
 */
struct MongoExtensionAggregationStageDescriptorVTable {
    /**
     * Return the type for this stage.
     */
    MongoExtensionAggregationStageType (*type)(const MongoExtensionAggregationStageDescriptor*);

    /**
     * Return properties of this stage as a binary coded BSON document.
     * Properties are static -- a descriptor is expected to return the same pointer on every call.
     * The extension host may cache data structures derived from the properties.
     *
     * This corresponds roughly to the enums that appear in StageConstraints.
     * TODO: add message definition, ideally in the form of an IDL type.
     */
    MongoExtensionByteView (*properties)(const MongoExtensionAggregationStageDescriptor*);

    /**
     * Bind this descriptor to a stage definition and pipeline context.
     *
     * stageBson contains a BSON document with a single (stageName, stageDefinition) element tuple.
     * contextBson contains a BSON document with additional context:
     * - namespace (object)
     *   - db ("string"; serialized NamespaceString)
     *   - collection (string; optional)
     *   - collectionUUID (UUID; optional)
     * - inRouter (bool)
     *
     * If the return code is zero, fills *boundStage, else fills *error. Returned objects are owned
     * by the caller.
     *
     * REQUIRES: type() returns kSource or kTransform.
     */
    int (*bind)(const MongoExtensionAggregationStageDescriptor* descriptor,
                MongoExtensionByteView stageBson,
                MongoExtensionByteView contextBson,
                MongoExtensionBoundAggregationStageDescriptor** boundStage,
                MongoExtensionByteBuf** error);

    /**
     * Desugar this stage into one or more other stages.
     *
     * stageBson contains a BSON document with a single (stageName, stageDefinition) element tuple.
     * contextBson contains a BSON document with additional context:
     * - namespace (object)
     *   - db ("string"; serialized NamespaceString)
     *   - collection (string; optional)
     *   - collectionUUID (UUID; optional)
     * - inRouter (bool)
     *
     * On a return code of zero *result contains a new BSON document with a single element tuple.
     * The tuple is keyed by the input stage name; the value is a stage definition or array of stage
     * definitions that will be created. On a non-zero return code *result contains a string
     * describing the error.
     *
     * REQUIRES: type() returns kDesugar.
     */
    int (*desugar)(const MongoExtensionAggregationStageDescriptor* descriptor,
                   MongoExtensionByteView stageBson,
                   MongoExtensionByteView contextBson,
                   MongoExtensionByteBuf** result);
};

/**
 * A BoundAggregationStageDescriptor describes a stage that has been bound to instance specific
 * context -- the stage definition and other context data from the pipeline. These objects are
 * suitable for pipeline optimization. Once optimization is complete they can be used to generate
 * objects for execution.
 */
struct MongoExtensionBoundAggregationStageDescriptor {
    const MongoExtensionBoundAggregationStageDescriptorVTable* vtable;
};


/**
 * Virtual function table for MongoExtensionBoundAggregationStageDescriptor.
 */
struct MongoExtensionBoundAggregationStageDescriptorVTable {
    /**
     * Drop `descriptor` and free any related resources.
     */
    void (*drop)(MongoExtensionBoundAggregationStageDescriptor* descriptor);

    /**
     * Get a merging pipeline that describes logic by which to merge streams during distributed
     * query execution. This may be called if the host is planning a query over a distributed
     * collection and the planner would like to attempt to push the extension stage to the shards.
     *
     * If the return code is 0, *result will contain a valid BSON document that contains the
     * structure { mergingStages: [<stages>] } which will be parsed as an aggregation.
     * If the return code is non-zero, an error occurred and result contains an error string.
     */
    int (*getMergingStages)(const MongoExtensionBoundAggregationStageDescriptor* stage,
                            MongoExtensionByteBuf** result);

    /**
     * Create an executor for this stage.
     *
     * If return code is 0, *executor will be filled with an execution instance.
     * Otherwise, *error will be returned with an error string.
     *
     * This may only be called once per instance, subsequent calls may return an error.
     */
    // TODO: we may want this method to consume `descriptor` like in a builder pattern.
    // This would be unpleasant for the plugin host to deal with, but logically there's no need for
    // the bound descriptor after the executor is created.
    int (*createExecutor)(MongoExtensionBoundAggregationStageDescriptor* descriptor,
                          MongoExtensionAggregationStage** executor,
                          MongoExtensionByteBuf** error);

    // TODO: a method for querying optimization properties, in particular:
    // * boolean StageConstraints. Many of these are non-static/bound to stage definition.
    // * modified paths for transform stages.
    // Like descriptor properties() this would be returned as a BSON message.
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

// The portal allows plugin functionality to register with the server.
struct MongoExtensionPortal {
    // Supported version of the plugin API.
    int version;

    /**
     * Register an AggregationStageDescriptor.
     *
     * `descriptor`s is expected to have a process lifetime.
     */
    void (*registerStageDescriptor)(MongoExtensionByteView name,
                                    const MongoExtensionAggregationStageDescriptor* descriptor);
};

}  // extern "C"