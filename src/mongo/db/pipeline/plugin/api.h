#include <stddef.h>

namespace mongo {

extern "C" {
    enum MongoDBPluginVersion {
        MONGODB_PLUGIN_VERSION_0 = 0,
    };

    enum MongoDBAggregationStageGetNextResult {
        GET_NEXT_ADVANCED = 0,
        GET_NEXT_EOF = -1,
        GET_NEXT_PAUSE_EXECUTION = -2,
    };

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
    // Your aggregation stage parser will heap allocate a `MyAggregationStage` and return it as a `mongodb_aggregation_stage*`.
    struct mongodb_aggregation_stage {
        // Get the next result from stage and typically filling (result, result_len). Memory pointed to by result is
        // owned by the stage and only valid until the next call on stage.
        //
        // Returns GET_NEXT_ADVANCED (0) on success filling (result, result_len) with a binary coded bson document.
        // Returns GET_NEXT_EOF if exhausted and fills (result, result_len) with (NULL, 0).
        // Returns GET_NEXT_WPAUSE_EXECUTION if there are no results now but there may be in the future. This also
        // fills (result, result_len) with (NULL, 0). If a source stage returns this it must be propagated.
        //
        // Any positive value indicates an error. (result, result_len) will be filled with a utf8 string describing
        // the error.
        int (*get_next)(mongodb_aggregation_stage* stage, char** result, size_t* result_len);

        // Close this stage and free any memory assoicated with it. It is an error to use stage after closing.
        void (*close)(mongodb_aggregation_stage* stage);

        // TODO: some way to get data from another stage.
        // * Could be a function that accepts a function pointer that the stage may store and invoke
        // * Could be a raw function pointer set by the caller that the stage may use to fetch data.
    };

    typedef int (*mongodb_parse_aggregation_stage)(char bson_type, char* bson_value, size_t bson_value_len, mongodb_aggregation_stage** stage, char** error, size_t* error_len);

    // The plugin portal allows plugin functionality to register with the server.
    struct mongodb_plugin_portal {
        // Supported version of the plugin API.
        int version;

        // Invoke to add an aggregation stage.
        //
        // Returns 0 on success. On any other value (error, error_len) should be filled with a useful status message.
        int (*add_aggregation_stage)(const char* name, size_t name_len, mongodb_parse_aggregation_stage parser, char** error, size_t* error_len);
    };

    // Invoked when a plugin is loaded to allow the plugin to register services.
    //
    // Returns 0 on success. On any other value (error, error_len) should be filled with a useful status message.
    int mongodb_initialize_plugin(mongodb_plugin_portal* plugin_portal, char** error, size_t* error_len);
}

}  // namespace mongo