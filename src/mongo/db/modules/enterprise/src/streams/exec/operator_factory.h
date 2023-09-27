#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/stdx/unordered_map.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_emit_operator.h"
#include "streams/exec/lookup_operator.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/sample_data_source_operator.h"

namespace streams {

class SinkOperator;
class SourceOperator;

/**
 * OperatorFactory is used to create streaming Operators from DocumentSources.
 * See usage in Parser.
 */
class OperatorFactory {
public:
    struct Options {
        // If true, caller is planning outer pipeline.
        // If false, caller is planning window inner pipeline.
        bool planMainPipeline{true};
    };

    OperatorFactory(Context* context, Options options);

    void validateByName(const std::string& name);

    std::unique_ptr<Operator> toOperator(mongo::DocumentSource* source);
    std::unique_ptr<Operator> toLookUpOperator(LookUpOperator::Options options);

    std::unique_ptr<SourceOperator> toSourceOperator(KafkaConsumerOperator::Options options);
    std::unique_ptr<SourceOperator> toSourceOperator(SampleDataSourceOperator::Options options);
    std::unique_ptr<SourceOperator> toSourceOperator(ChangeStreamSourceOperator::Options options);
    std::unique_ptr<SourceOperator> toSourceOperator(InMemorySourceOperator::Options options);

    std::unique_ptr<SinkOperator> toSinkOperator(MergeOperator::Options options);
    std::unique_ptr<SinkOperator> toSinkOperator(KafkaEmitOperator::Options options);

private:
    Context* _context{nullptr};
    Options _options;
};

};  // namespace streams
