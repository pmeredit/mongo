#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/sample_data_source_operator.h"
#include <unordered_map>

namespace streams {

class SinkOperator;
class SourceOperator;

/**
 * OperatorFactory is used to create streaming Operators from DocumentSources.
 * See usage in Parser.
 */
class OperatorFactory {
public:
    OperatorFactory(Context* context) : _context(context) {}

    void validateByName(const std::string& name);
    std::unique_ptr<Operator> toOperator(mongo::DocumentSource* source);
    std::unique_ptr<SourceOperator> toSourceOperator(KafkaConsumerOperator::Options options);
    std::unique_ptr<SourceOperator> toSourceOperator(SampleDataSourceOperator::Options options);
    std::unique_ptr<SinkOperator> toSinkOperator(mongo::DocumentSource* source);

private:
    Context* _context{nullptr};
};

};  // namespace streams
