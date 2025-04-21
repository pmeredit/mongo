#pragma once

#include <stddef.h>

extern "C" {

struct MongoExtensionAggregationStage;
struct MongoExtensionAggregationStageDescriptor;
struct MongoExtensionAggregationStageDescriptorVTable;
struct MongoExtensionAggregationStageVTable;
struct MongoExtensionBoundAggregationStageDescriptor;
struct MongoExtensionBoundAggregationStageDescriptorVTable;
struct MongoExtensionByteBufVTable;
struct MongoExtensionErrorVTable;

enum MongoDBPluginVersion {
    MONGODB_PLUGIN_VERSION_0 = 0,
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
     * Drop `buf` and free all associated resources.
     */
    void (*drop)(MongoExtensionByteBuf* buf);

    /**
     * Get a read-only view of the contents of `buf`.
     */
    MongoExtensionByteView (*get)(const MongoExtensionByteBuf* buf);
};

/**
 * A type for errors that may be passed across the extension boundary.
 *
 * This is typically returned by extension APIs to pass errors to the server side, but may be
 * provided to the extension as well in some cases, like when a transform aggregation stage
 * consumes input from a host provided aggregation stage.
 */
struct MongoExtensionError {
    const MongoExtensionErrorVTable* vtable;
};

/**
 * Virtual function table for MongoExtensionError
 */
struct MongoExtensionErrorVTable {
    /**
     * Drop `error` and free all associated resources.
     */
    void (*drop)(MongoExtensionError* error);

    /**
     * Return a non-zero code associated with `error`.
     */
    int (*code)(const MongoExtensionError* error);

    /**
     * Return a utf-8 string associated with `error`. May be empty.
     */
    MongoExtensionByteView (*reason)(const MongoExtensionError* error);
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
     * If this stage is of kTransform type then source is expected to contain a valid stage pointer,
     * otherwise source should be nullptr. In all cases this call takes ownership of source.
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
                          MongoExtensionAggregationStage* source,
                          MongoExtensionAggregationStage** executor,
                          MongoExtensionByteBuf** error);

    // TODO: a method for querying optimization properties, in particular:
    // * boolean StageConstraints. Many of these are non-static/bound to stage definition.
    // * modified paths for transform stages.
    // Like descriptor properties() this would be returned as a BSON message.
};

/**
 * Code indicating the result of a getNext() call.
 */
enum MongoExtensionGetNextResultCode {
    /**
     * getNext() yielded a document.
     */
    kAdvanced = 0,
    /**
     * getNext() did not yield a document and will never yield another document.
     */
    kEOF = -1,
    /**
     * getNext() did not yield a document, but may yield another document in the future.
     */
    kPauseExecution = -2,
};

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
    /**
     * Drop `stage` and free any related resources.
     */
    void (*drop)(MongoExtensionAggregationStage* stage);

    /**
     * Pull the next result from the stage executor.
     *
     * On success returns a nullptr and fills `code` and `doc` if `code == kAdvanced`. The memory
     * `doc` points to is only valid until the next `getNext()` call.
     *
     * On error the returned pointer belongs to the caller.
     */
    MongoExtensionError* (*getNext)(MongoExtensionAggregationStage* stage,
                                    MongoExtensionGetNextResultCode* code,
                                    MongoExtensionByteView* doc);
};

/**
 * Access to host services.
 *
 * The implementation of each of these functions must be safe for concurrent callers.
 */
struct MongoExtensionHostServices {
    /**
     * Call this method when an extension-owned thread becomes idle. This will cause the thread to
     * be omitted from stack traces.
     *
     * Location should be a valid null-terminated c string.
     */
    void (*beginIdleThreadBlock)(const char* location);

    /**
     * Call this method when an extension-owned thread is activated from idle state.
     */
    void (*endIdleThreadBlock)();
};

// The portal allows plugin functionality to register with the server.
struct MongoExtensionPortal {
    // Supported version of the plugin API.
    int version;

    /**
     * Services provided by the host to extensions.
     */
    const MongoExtensionHostServices* hostServices;

    /**
     * Register an AggregationStageDescriptor.
     *
     * `descriptor`s is expected to have a process lifetime.
     */
    void (*registerStageDescriptor)(MongoExtensionByteView name,
                                    const MongoExtensionAggregationStageDescriptor* descriptor);
};

}  // extern "C"