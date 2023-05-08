#pragma once

#include "mongo/db/pipeline/expression_context.h"
#include "mongo/util/assert_util.h"

#include "streams/exec/document_source_feeder.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace mongo {
class BSONObjBuilder;
}  // namespace mongo

namespace streams {

/**
 * Represents the computation and results for a single window's "inner pipeline".
 */
class WindowPipeline {
public:
    WindowPipeline(int64_t start,
                   int64_t end,
                   std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> pipeline,
                   boost::intrusive_ptr<mongo::ExpressionContext> expCtx);

    // Process another document.
    // Caller should move the doc argument if it won't be used elsewhere.
    void process(StreamDocument doc);

    // Returns the output of the window.
    std::queue<StreamDataMsg> close();

    int64_t getStart() const {
        return _startMs;
    }

    int64_t getEnd() const {
        return _endMs;
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

private:
    StreamDocument toOutputDocument(mongo::Document doc);

    const int64_t _startMs;
    const int64_t _endMs;
    int64_t _minObservedEventTimeMs{std::numeric_limits<int64_t>::max()};
    int64_t _maxObservedEventTimeMs{0};
    int64_t _minObservedProcessingTime{std::numeric_limits<int64_t>::max()};
    std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> _pipeline;
    std::unique_ptr<DocumentSourceFeeder> _feeder;
    boost::optional<mongo::StreamMeta> _streamMetaTemplate;
    std::vector<mongo::Document> _earlyResults;
    boost::optional<std::string> _error;
};

}  // namespace streams
