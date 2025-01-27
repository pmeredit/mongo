#include "src/mongo/db/pipeline/plugin/api.h"

#include <memory>
#include <string>

#include "src/mongo/db/pipeline/document_source.h"

namespace mongo {

namespace {
struct c_plugin_echo_aggregation_stage {
    mongodb_aggregation_stage stage;
    std::string document;
    bool exhausted;

    static const char kStageName[] = "$cecho";
};

const char kNotADocument[] = "$cecho argument must be document typed.";

int c_plugin_echo_aggregation_stage_get_next(mongodb_aggregation_stage* stage,
                                             char** result,
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
                                          char* bson_value,
                                          size_t bson_value_len,
                                          mongodb_aggregation_stage** stage,
                                          char** error,
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

int mongodb_initialize_plugin(mongodb_plugin_portal* plugin_portal,
                              char** error,
                              size_t* error_len) {
    return plugin_portal->add_aggregation_stage(c_plugin_echo_aggregation_stage::kStageName,
                                                sizeof(c_plugin_echo_aggregation_stage::kStageName),
                                                c_plugin_echo_aggregation_stage_parse,
                                                error,
                                                error_len);
}
}

}  // namespace mongo