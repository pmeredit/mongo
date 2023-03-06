/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/operator_factory.h"
#include "streams/exec/add_fields_operator.h"
#include "streams/exec/match_operator.h"
#include "streams/exec/project_operator.h"
#include "streams/exec/redact_operator.h"
#include "streams/exec/replace_root_operator.h"
#include "streams/exec/set_operator.h"
#include "streams/exec/unwind_operator.h"

namespace streams {

using namespace mongo;
using namespace std;

namespace {
enum class OperatorType { kAddFields, kMatch, kProject, kRedact, kReplaceRoot, kSet, kUnwind };

unordered_map<string, OperatorType> _supportedStages{
    // Non blocking stages. These stages are supported on infinite streams. They can
    // exist in stream pipelines both inside and outside of windows.
    // These stages all convert 1 Document to 0+ Documents.
    {"$addFields", OperatorType::kAddFields},
    {"$match", OperatorType::kMatch},
    {"$project", OperatorType::kProject},
    {"$redact", OperatorType::kRedact},
    {"$replaceRoot", OperatorType::kReplaceRoot},
    {"$replaceWith", OperatorType::kReplaceRoot},
    {"$set", OperatorType::kSet},
    {"$unset", OperatorType::kProject},
    {"$unwind", OperatorType::kUnwind},
};
};  // namespace

void OperatorFactory::validateByName(const std::string& name) {
    bool isStageSupported = _supportedStages.find(name) != _supportedStages.end();
    if (!isStageSupported) {
        uasserted(ErrorCode::kTemporaryUserErrorCode, str::stream() << "Unsupported: " << name);
    }
}

unique_ptr<Operator> OperatorFactory::toOperator(DocumentSource* source) {
    validateByName(source->getSourceName());
    OperatorType type = _supportedStages[source->getSourceName()];
    switch (type) {
        case OperatorType::kAddFields:
            return std::make_unique<AddFieldsOperator>(source);
        case OperatorType::kMatch:
            return std::make_unique<MatchOperator>(source);
        case OperatorType::kProject:
            return std::make_unique<ProjectOperator>(source);
        case OperatorType::kRedact:
            return std::make_unique<RedactOperator>(source);
        case OperatorType::kReplaceRoot:
            return std::make_unique<ReplaceRootOperator>(source);
        case OperatorType::kSet:
            return std::make_unique<SetOperator>(source);
        case OperatorType::kUnwind:
            return std::make_unique<UnwindOperator>(source);
        default:
            MONGO_UNREACHABLE;
    }
}

};  // namespace streams
