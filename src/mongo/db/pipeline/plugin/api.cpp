#include "mongo/db/pipeline/plugin/api.h"

#include <memory>
#include <string>

namespace mongo {

namespace {
struct c_plugin_echo_aggregation_stage {
    mongodb_aggregation_stage stage;
    std::string document;
    bool exhausted;

    static constexpr char kStageName[] = "$echoC";
};

const char kNotADocument[] = "$echoC argument must be document typed.";

int c_plugin_echo_aggregation_stage_get_next(mongodb_aggregation_stage* stage,
                                             const char** result,
                                             size_t* result_len) {
    auto echo_stage = (c_plugin_echo_aggregation_stage*)stage;
    if (echo_stage->exhausted) {
        return MongoDBAggregationStageGetNextResult::GET_NEXT_EOF;
    }
    *result = echo_stage->document.data();
    *result_len = echo_stage->document.size();
    echo_stage->exhausted = true;
    return MongoDBAggregationStageGetNextResult::GET_NEXT_ADVANCED;
}

void c_plugin_echo_aggregation_stage_close(mongodb_aggregation_stage* stage) {
    // unleak the stage and cast to the real type to ensure proper deletion.
    auto ptr =
        std::unique_ptr<c_plugin_echo_aggregation_stage>((c_plugin_echo_aggregation_stage*)stage);
    ptr = nullptr;
}

int c_plugin_echo_aggregation_stage_parse(char bson_type,
                                          const char* bson_value,
                                          size_t bson_value_len,
                                          mongodb_aggregation_stage** stage,
                                          const char** error,
                                          size_t* error_len) {
    if (bson_type != 3) {  // embedded document
        *error = (char*)kNotADocument;
        *error_len = sizeof(kNotADocument);
        return 1;
    }

    auto echo_stage = std::make_unique<c_plugin_echo_aggregation_stage>();
    echo_stage->stage.get_next = &c_plugin_echo_aggregation_stage_get_next;
    echo_stage->stage.close = &c_plugin_echo_aggregation_stage_close;
    echo_stage->document = std::string(bson_value, bson_value_len);
    echo_stage->exhausted = false;

    // leak the value to the caller as a mongodb_aggregation_stage*.
    *stage = (mongodb_aggregation_stage*)echo_stage.release();
    *error = nullptr;
    *error_len = 0;
    return 0;
}
}  // namespace

extern "C" {

void mongodb_initialize_plugin(mongodb_plugin_portal* plugin_portal) {
    // Register kStageName - 1 bytes to avoid including the null terminator.
    // Note that this is still a valid c string which is often assumed throughout mongod.
    plugin_portal->add_aggregation_stage(c_plugin_echo_aggregation_stage::kStageName,
                                         sizeof(c_plugin_echo_aggregation_stage::kStageName) - 1,
                                         c_plugin_echo_aggregation_stage_parse);
}
}

}  // namespace mongo