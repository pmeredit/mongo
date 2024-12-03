/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

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
#include "streams/util/metric_manager.h"

namespace streams {

class SinkOperator;
class SourceOperator;

/**
 * OperatorDag is an directed, acyclic graph of stream Operators.
 */
class OperatorDag {
public:
    using OperatorContainer = std::deque<std::unique_ptr<Operator>>;

    struct Options {
        // Tracks DocumentSource objects corresponding to the stages between the source and the
        // sink.
        mongo::Pipeline::SourceContainer pipeline;
        std::vector<mongo::BSONObj> inputPipeline;
        std::vector<mongo::BSONObj> optimizedPipeline;
        std::unique_ptr<DocumentTimestampExtractor> timestampExtractor;
        std::unique_ptr<EventDeserializer> eventDeserializer;
        bool needsWindowReplay{false};
    };

    OperatorDag(Options options, OperatorContainer operators)
        : _options(std::move(options)), _operators(std::move(operators)) {}

    // Starts the flow of data through the OperatorDag.
    // TODO: This can throw an exception (e.g. KafkaConsumerOperator::start()), handle the
    // exception.
    void start();

    // Stops the flow of data through the OperatorDag and destroys the Operator instances.
    void stop();

    // Returns the first operator in _operators.
    SourceOperator* source() const;

    // Returns the last operator in _operators.
    SinkOperator* sink() const;

    const std::vector<mongo::BSONObj>& inputPipeline() const {
        return _options.inputPipeline;
    }

    const std::vector<mongo::BSONObj>& optimizedPipeline() const {
        return _options.optimizedPipeline;
    }

    const OperatorContainer& operators() const {
        return _operators;
    }

    // Adds the given operator at the beginning of _operators.
    void pushFront(std::unique_ptr<Operator> oper) {
        _operators.push_front(std::move(oper));
    }

    mongo::Pipeline::SourceContainer movePipeline() {
        return std::move(_options.pipeline);
    }

    OperatorContainer moveOperators() {
        return std::move(_operators);
    }

    bool needsWindowReplay() {
        return _options.needsWindowReplay;
    }

private:
    friend class OperatorDagTest;
    Options _options;
    // Note that Operator instances get destroyed in stop().
    OperatorContainer _operators;
};

};  // namespace streams
