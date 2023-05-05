/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/operator_factory.h"
#include "mongo/db/matcher/expression_always_boolean.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/pipeline/document_source_add_fields.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_source_redact.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/document_source_unwind.h"
#include "streams/exec/add_fields_operator.h"
#include "streams/exec/document_source_validate_stub.h"
#include "streams/exec/document_source_window_stub.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_partition_consumer_base.h"
#include "streams/exec/log_sink_operator.h"
#include "streams/exec/match_operator.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/project_operator.h"
#include "streams/exec/redact_operator.h"
#include "streams/exec/replace_root_operator.h"
#include "streams/exec/set_operator.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/unwind_operator.h"
#include "streams/exec/validate_operator.h"
#include "streams/exec/window_operator.h"

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
    kMerge,
    kTumblingWindow,
    kValidate
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
    {"$tumblingWindow", OperatorType::kTumblingWindow},
    {"$validate", OperatorType::kValidate},
};

// Constructs WindowOperator::Options.
WindowOperator::Options makeWindowOperatorOptions(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, BSONObj bsonOptions) {
    auto options = TumblingWindowOptions::parse(IDLParserContext("tumblingWindow"), bsonOptions);
    auto interval = options.getInterval();
    const auto& pipeline = options.getPipeline();
    auto size = interval.getSize();
    return {pipeline, expCtx, size, interval.getUnit(), size, interval.getUnit()};
}

// Constructs ValidateOperator::Options.
ValidateOperator::Options makeValidateOperatorOptions(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONObj bsonOptions,
    DeadLetterQueue* dlq) {
    auto options = ValidateOptions::parse(IDLParserContext("validate"), bsonOptions);

    std::unique_ptr<MatchExpression> validator;
    if (!options.getValidator().isEmpty()) {
        auto statusWithMatcher = MatchExpressionParser::parse(options.getValidator(), expCtx);
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "failed to parse validator: " << options.getValidator(),
                statusWithMatcher.isOK());
        validator = std::move(statusWithMatcher.getValue());
    } else {
        validator = std::make_unique<AlwaysTrueMatchExpression>();
    }

    if (options.getValidationAction() == mongo::StreamsValidationActionEnum::Dlq) {
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "DLQ must be specified if validation action is dlq.",
                bool(dlq));
    }

    return {expCtx, std::move(validator), options.getValidationAction(), dlq};
}

};  // namespace

void OperatorFactory::validateByName(const std::string& name) {
    bool isStageSupported = _supportedStages.find(name) != _supportedStages.end();
    if (!isStageSupported) {
        uasserted(ErrorCodes::InvalidOptions, str::stream() << "Unsupported: " << name);
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
        case OperatorType::kTumblingWindow: {
            auto specificSource = dynamic_cast<DocumentSourceWindowStub*>(source);
            dassert(specificSource);
            auto options = makeWindowOperatorOptions(specificSource->getContext(),
                                                     specificSource->bsonOptions());
            return std::make_unique<WindowOperator>(std::move(options));
        }
        case OperatorType::kValidate: {
            auto specificSource = dynamic_cast<DocumentSourceValidateStub*>(source);
            dassert(specificSource);
            auto options = makeValidateOperatorOptions(
                specificSource->getContext(), specificSource->bsonOptions(), _context->dlq.get());
            return std::make_unique<ValidateOperator>(std::move(options));
        }
        case OperatorType::kMerge:
            [[fallthrough]];
        default:
            MONGO_UNREACHABLE;
    }
}

unique_ptr<SourceOperator> OperatorFactory::toSourceOperator(
    KafkaConsumerOperator::Options options) {
    return std::make_unique<KafkaConsumerOperator>(std::move(options));
}

std::unique_ptr<SinkOperator> OperatorFactory::toSinkOperator(mongo::DocumentSource* source) {
    validateByName(source->getSourceName());
    OperatorType type = _supportedStages[source->getSourceName()];
    switch (type) {
        case OperatorType::kMerge: {
            auto specificSource = dynamic_cast<DocumentSourceMerge*>(source);
            dassert(specificSource);
            return std::make_unique<MergeOperator>(specificSource);
        }
        default:
            MONGO_UNREACHABLE;
    }
}

};  // namespace streams
