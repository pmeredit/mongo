#pragma once

#include <deque>
#include <memory>

#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/service_context.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/operator.h"

namespace streams {

class SinkOperator;
class SourceOperator;

/**
 * OperatorDag is an directed, acyclic graph of stream Operators.
 * Use OperatorDagFactory to create an OperatorDag.
 */
class OperatorDag {
public:
    using OperatorContainer = std::deque<std::unique_ptr<Operator>>;

    struct Options {
        mongo::Pipeline::SourceContainer pipeline;
        std::vector<mongo::BSONObj> bsonPipeline;
        std::unique_ptr<DocumentTimestampExtractor> timestampExtractor;
        std::unique_ptr<EventDeserializer> eventDeserializer;
    };

    OperatorDag(Options options, OperatorContainer operators)
        : _options(std::move(options)), _operators(std::move(operators)) {}

    // Start the flow of data through the OperatorDag.
    // TODO: This can throw an exception (e.g. KafkaConsumerOperator::start()), handle the
    // exception.
    void start();

    // Stop the flow of data through the OperatorDag.
    void stop();

    // Returns the first operator in _operators.
    SourceOperator* source();

    // Returns the last operator in _operators.
    SinkOperator* sink();

    const OperatorContainer& operators() {
        return _operators;
    }

    // Adds the given operator at the end of _operators.
    void pushBack(std::unique_ptr<Operator> oper) {
        _operators.push_back(std::move(oper));
    }

    // Adds the given operator at the beginning of _operators.
    void pushFront(std::unique_ptr<Operator> oper) {
        _operators.push_front(std::move(oper));
    }

private:
    Options _options;
    OperatorContainer _operators;
};

};  // namespace streams
