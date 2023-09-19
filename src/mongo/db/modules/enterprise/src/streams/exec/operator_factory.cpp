/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/operator_factory.h"

#include "mongo/db/matcher/expression_always_boolean.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/pipeline/document_source_add_fields.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_lookup.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_source_redact.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/db/pipeline/document_source_unwind.h"
#include "streams/exec/add_fields_operator.h"
#include "streams/exec/document_source_validate_stub.h"
#include "streams/exec/document_source_window_stub.h"
#include "streams/exec/group_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_emit_operator.h"
#include "streams/exec/kafka_partition_consumer_base.h"
#include "streams/exec/limit_operator.h"
#include "streams/exec/log_sink_operator.h"
#include "streams/exec/match_operator.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/project_operator.h"
#include "streams/exec/redact_operator.h"
#include "streams/exec/replace_root_operator.h"
#include "streams/exec/sample_data_source_operator.h"
#include "streams/exec/set_operator.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/sort_operator.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/unwind_operator.h"
#include "streams/exec/validate_operator.h"
#include "streams/exec/window_operator.h"

namespace streams {

using namespace mongo;
using namespace std;

namespace {

// Constructs WindowOperator::Options.
WindowOperator::Options makeTumblingWindowOperatorOptions(Context* context, BSONObj bsonOptions) {
    auto options = TumblingWindowOptions::parse(IDLParserContext("tumblingWindow"), bsonOptions);
    auto interval = options.getInterval();
    auto offset = options.getOffset();
    auto pipeline = Pipeline::parse(options.getPipeline(), context->expCtx);
    pipeline->optimizePipeline();
    auto size = interval.getSize();
    uassert(ErrorCodes::InvalidOptions, "Window interval size must be greater than 0.", size > 0);
    return {pipeline->serializeToBson(),
            size,
            interval.getUnit(),
            size,
            interval.getUnit(),
            offset ? offset->getOffsetFromUtc() : 0,
            offset ? offset->getUnit() : StreamTimeUnitEnum::Millisecond};
}

// Constructs WindowOperator::Options.
WindowOperator::Options makeHoppingWindowOperatorOptions(Context* context, BSONObj bsonOptions) {
    auto options = HoppingWindowOptions::parse(IDLParserContext("hoppingWindow"), bsonOptions);
    auto windowInterval = options.getInterval();
    auto hopInterval = options.getHopSize();
    auto pipeline = Pipeline::parse(options.getPipeline(), context->expCtx);
    uassert(ErrorCodes::InvalidOptions,
            "Window interval size must be greater than 0.",
            windowInterval.getSize() > 0);
    uassert(ErrorCodes::InvalidOptions,
            "Window hopSize size must be greater than 0.",
            hopInterval.getSize() > 0);
    pipeline->optimizePipeline();
    return {pipeline->serializeToBson(),
            windowInterval.getSize(),
            windowInterval.getUnit(),
            hopInterval.getSize(),
            hopInterval.getUnit()};
}
// Constructs ValidateOperator::Options.
ValidateOperator::Options makeValidateOperatorOptions(Context* context, BSONObj bsonOptions) {
    auto options = ValidateOptions::parse(IDLParserContext("validate"), bsonOptions);

    std::unique_ptr<MatchExpression> validator;
    if (!options.getValidator().isEmpty()) {
        auto statusWithMatcher =
            MatchExpressionParser::parse(options.getValidator(), context->expCtx);
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
                bool(context->dlq));
    }

    return {std::move(validator), options.getValidationAction()};
}

};  // namespace

OperatorFactory::OperatorFactory(Context* context, Options options)
    : _context(context), _options(std::move(options)) {
    _supportedStages = stdx::unordered_map<std::string, StageInfo>{
        {"$addFields", {StageType::kAddFields, true, true}},
        {"$match", {StageType::kMatch, true, true}},
        {"$project", {StageType::kProject, true, true}},
        {"$redact", {StageType::kRedact, true, true}},
        {"$replaceRoot", {StageType::kReplaceRoot, true, true}},
        {"$replaceWith", {StageType::kReplaceRoot, true, true}},
        {"$set", {StageType::kSet, true, true}},
        {"$unset", {StageType::kProject, true, true}},
        {"$unwind", {StageType::kUnwind, true, true}},
        {"$merge", {StageType::kMerge, true, true}},
        {"$tumblingWindow", {StageType::kTumblingWindow, true, false}},
        {"$hoppingWindow", {StageType::kHoppingWindow, true, false}},
        {"$validate", {StageType::kValidate, true, true}},
        {"$lookup", {StageType::kLookUp, true, true}},
        {"$group", {StageType::kGroup, false, true}},
        {"$sort", {StageType::kSort, false, true}},
        {"$limit", {StageType::kLimit, false, true}},
        {"$emit", {StageType::kEmit, true, false}},
    };
}

void OperatorFactory::validateByName(const std::string& name) {
    auto it = _supportedStages.find(name);
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Unsupported stage: " << name,
            it != _supportedStages.end());

    const auto& stageInfo = it->second;
    if (_options.planMainPipeline) {
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << name
                              << " stage is not permitted in the inner pipeline of a window stage",
                stageInfo.allowedInMainPipeline);
    } else {
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << name
                              << " stage is only permitted in the inner pipeline of a window stage",
                stageInfo.allowedInWindowInnerPipeline);
    }
}

unique_ptr<Operator> OperatorFactory::toOperator(DocumentSource* source) {
    validateByName(source->getSourceName());
    const auto& stageInfo = _supportedStages[source->getSourceName()];
    switch (stageInfo.type) {
        case StageType::kAddFields: {
            auto specificSource = dynamic_cast<DocumentSourceSingleDocumentTransformation*>(source);
            dassert(specificSource);
            SingleDocumentTransformationOperator::Options options{.documentSource = specificSource};
            return std::make_unique<AddFieldsOperator>(_context, std::move(options));
        }
        case StageType::kSet: {
            auto specificSource = dynamic_cast<DocumentSourceSingleDocumentTransformation*>(source);
            dassert(specificSource);
            SingleDocumentTransformationOperator::Options options{.documentSource = specificSource};
            return std::make_unique<SetOperator>(_context, std::move(options));
        }
        case StageType::kMatch: {
            auto specificSource = dynamic_cast<DocumentSourceMatch*>(source);
            dassert(specificSource);
            MatchOperator::Options options{.documentSource = specificSource};
            return std::make_unique<MatchOperator>(_context, std::move(options));
        }
        case StageType::kProject: {
            auto specificSource = dynamic_cast<DocumentSourceSingleDocumentTransformation*>(source);
            dassert(specificSource);
            SingleDocumentTransformationOperator::Options options{.documentSource = specificSource};
            return std::make_unique<ProjectOperator>(_context, std::move(options));
        }
        case StageType::kRedact: {
            auto specificSource = dynamic_cast<DocumentSourceRedact*>(source);
            dassert(specificSource);
            RedactOperator::Options options{.documentSource = specificSource};
            return std::make_unique<RedactOperator>(_context, std::move(options));
        }
        case StageType::kReplaceRoot: {
            auto specificSource = dynamic_cast<DocumentSourceSingleDocumentTransformation*>(source);
            dassert(specificSource);
            SingleDocumentTransformationOperator::Options options{.documentSource = specificSource};
            return std::make_unique<ReplaceRootOperator>(_context, std::move(options));
        }
        case StageType::kUnwind: {
            auto specificSource = dynamic_cast<DocumentSourceUnwind*>(source);
            dassert(specificSource);
            UnwindOperator::Options options{.documentSource = specificSource};
            return std::make_unique<UnwindOperator>(_context, std::move(options));
        }
        case StageType::kTumblingWindow: {
            auto specificSource = dynamic_cast<DocumentSourceTumblingWindowStub*>(source);
            dassert(specificSource);
            auto options =
                makeTumblingWindowOperatorOptions(_context, specificSource->bsonOptions());
            return std::make_unique<WindowOperator>(_context, std::move(options));
        }
        case StageType::kHoppingWindow: {
            auto specificSource = dynamic_cast<DocumentSourceHoppingWindowStub*>(source);
            dassert(specificSource);
            auto options =
                makeHoppingWindowOperatorOptions(_context, specificSource->bsonOptions());
            return std::make_unique<WindowOperator>(_context, std::move(options));
        }
        case StageType::kValidate: {
            auto specificSource = dynamic_cast<DocumentSourceValidateStub*>(source);
            dassert(specificSource);
            auto options = makeValidateOperatorOptions(_context, specificSource->bsonOptions());
            return std::make_unique<ValidateOperator>(_context, std::move(options));
        }
        case StageType::kGroup: {
            auto specificSource = dynamic_cast<DocumentSourceGroup*>(source);
            dassert(specificSource);
            GroupOperator::Options options{.documentSource = specificSource};
            return std::make_unique<GroupOperator>(_context, std::move(options));
        }
        case StageType::kSort: {
            auto specificSource = dynamic_cast<DocumentSourceSort*>(source);
            dassert(specificSource);
            SortOperator::Options options{.documentSource = specificSource};
            return std::make_unique<SortOperator>(_context, std::move(options));
        }
        case StageType::kLimit: {
            auto specificSource = dynamic_cast<DocumentSourceLimit*>(source);
            dassert(specificSource);
            return std::make_unique<LimitOperator>(_context, specificSource->getLimit());
        }
        case StageType::kEmit:
        case StageType::kMerge:
            [[fallthrough]];
        default:
            MONGO_UNREACHABLE;
    }
}

std::unique_ptr<Operator> OperatorFactory::toLookUpOperator(LookUpOperator::Options options) {
    return std::make_unique<LookUpOperator>(_context, std::move(options));
}

unique_ptr<SourceOperator> OperatorFactory::toSourceOperator(
    KafkaConsumerOperator::Options options) {
    return std::make_unique<KafkaConsumerOperator>(_context, std::move(options));
}

unique_ptr<SourceOperator> OperatorFactory::toSourceOperator(
    SampleDataSourceOperator::Options options) {
    return std::make_unique<SampleDataSourceOperator>(_context, std::move(options));
}

unique_ptr<SourceOperator> OperatorFactory::toSourceOperator(
    ChangeStreamSourceOperator::Options options) {
    return std::make_unique<ChangeStreamSourceOperator>(_context, std::move(options));
}

unique_ptr<SourceOperator> OperatorFactory::toSourceOperator(
    InMemorySourceOperator::Options options) {
    return std::make_unique<InMemorySourceOperator>(_context, std::move(options));
}

std::unique_ptr<SinkOperator> OperatorFactory::toSinkOperator(MergeOperator::Options options) {
    return std::make_unique<MergeOperator>(_context, std::move(options));
}

std::unique_ptr<SinkOperator> OperatorFactory::toSinkOperator(KafkaEmitOperator::Options options) {
    return std::make_unique<KafkaEmitOperator>(_context, std::move(options));
}

};  // namespace streams
