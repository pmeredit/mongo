/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/operator_factory.h"
#include "mongo/db/pipeline/document_source_add_fields.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_source_redact.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/document_source_unwind.h"
#include "streams/exec/add_fields_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_partition_consumer_base.h"
#include "streams/exec/log_sink_operator.h"
#include "streams/exec/match_operator.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/project_operator.h"
#include "streams/exec/redact_operator.h"
#include "streams/exec/replace_root_operator.h"
#include "streams/exec/set_operator.h"
#include "streams/exec/unwind_operator.h"

namespace streams {

using namespace mongo;
using namespace std;

namespace {
enum class OperatorType {
    kAddFields,
    kMatch,
    kProject,
    kRedact,
    kReplaceRoot,
    kSet,
    kUnwind,
    kMerge
};

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
    {"$merge", OperatorType::kMerge},
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
        case OperatorType::kAddFields: {
            auto specificSource = dynamic_cast<DocumentSourceSingleDocumentTransformation*>(source);
            dassert(specificSource);
            return std::make_unique<AddFieldsOperator>(specificSource);
        }
        case OperatorType::kSet: {
            auto specificSource = dynamic_cast<DocumentSourceSingleDocumentTransformation*>(source);
            dassert(specificSource);
            return std::make_unique<SetOperator>(specificSource);
        }
        case OperatorType::kMatch: {
            auto specificSource = dynamic_cast<DocumentSourceMatch*>(source);
            dassert(specificSource);
            return std::make_unique<MatchOperator>(specificSource);
        }
        case OperatorType::kProject: {
            auto specificSource = dynamic_cast<DocumentSourceSingleDocumentTransformation*>(source);
            dassert(specificSource);
            return std::make_unique<ProjectOperator>(specificSource);
        }
        case OperatorType::kRedact: {
            auto specificSource = dynamic_cast<DocumentSourceRedact*>(source);
            dassert(specificSource);
            return std::make_unique<RedactOperator>(specificSource);
        }
        case OperatorType::kReplaceRoot: {
            auto specificSource = dynamic_cast<DocumentSourceSingleDocumentTransformation*>(source);
            dassert(specificSource);
            return std::make_unique<ReplaceRootOperator>(specificSource);
        }
        case OperatorType::kUnwind: {
            auto specificSource = dynamic_cast<DocumentSourceUnwind*>(source);
            dassert(specificSource);
            return std::make_unique<UnwindOperator>(specificSource);
        }
        case OperatorType::kMerge: {
            auto specificSource = dynamic_cast<DocumentSourceMerge*>(source);
            dassert(specificSource);
            return std::make_unique<MergeOperator>(specificSource);
        }
        default:
            MONGO_UNREACHABLE;
    }
}

unique_ptr<SourceOperator> OperatorFactory::toSourceOperator(
    KafkaConsumerOperator::Options options) {
    return std::make_unique<KafkaConsumerOperator>(std::move(options));
}

};  // namespace streams
