#pragma once

#include <queue>

#include "mongo/db/pipeline/expression_context.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/context.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"

namespace mongo {
class BSONObjBuilder;
}  // namespace mongo

namespace streams {

/**
 * Represents the computation and results for a single window's "inner pipeline".
 */
class WindowPipeline {
public:
    struct Options {
        int64_t startMs;
        int64_t endMs;
        mongo::Pipeline::SourceContainer pipeline;
        OperatorDag::OperatorContainer operators;
        OperatorId sinkOperatorId;
    };

    WindowPipeline(Context* context, Options options);

    // Process another document.
    // Caller should move the doc argument if it won't be used elsewhere.
    void process(StreamDataMsg dataMsg);

    // Whether this window pipeline has no more output data msgs to emit.
    bool isEof() const;

    // Get the next set of output data msgs for this window pipeline. If `eof
    // is set, then it will continuously sending `eofSignal` to the pipeline
    // operator DAG until a msg is emitted.
    std::queue<StreamDataMsg> getNextOutputDataMsgs(bool eof = false);

    // Stops all the operators in the window pipeline and returns the merged operator
    // stats of all the operators in the pipeline. This should be called after the
    // window pipeline has been exhausted.
    OperatorStats close();

    int64_t getStart() const {
        return _options.startMs;
    }

    int64_t getEnd() const {
        return _options.endMs;
    }

    // Set _error indicating that processing for this window ran into an error.
    void setError(std::string error) {
        _error = std::move(error);
    }

    const boost::optional<std::string>& getError() const {
        return _error;
    }

    // Builds a DLQ message for this window using _error.
    mongo::BSONObjBuilder getDeadLetterQueueMsg() const;

    // Returns the operator stats of the operators in this pipeline
    OperatorStats getStats() const;

private:
    friend class WindowOperatorTest;

    StreamDocument toOutputDocument(StreamDocument streamDoc);

    Context* _context{nullptr};
    Options _options;
    int64_t _minObservedEventTimeMs{std::numeric_limits<int64_t>::max()};
    int64_t _maxObservedEventTimeMs{0};
    int64_t _minObservedProcessingTime{std::numeric_limits<int64_t>::max()};
    boost::optional<mongo::StreamMeta> _streamMetaTemplate;
    boost::optional<std::string> _error;
    bool _reachedEof{false};
};

}  // namespace streams
