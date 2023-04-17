#pragma once

#include "mongo/db/pipeline/expression_context.h"
#include "mongo/util/assert_util.h"

#include "streams/exec/document_source_feeder.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/parser.h"
#include "streams/exec/source_stage_gen.h"

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

    /**
     * Process another document with time=time.
     * Caller should move the doc argument if it won't be used elsewhere.
     */
    void process(StreamDocument doc);

    /**
     * Returns the output of the window.
     */
    std::queue<StreamDataMsg> close();

    int64_t getStart() const {
        return _startMs;
    }

    int64_t getEnd() const {
        return _endMs;
    }

private:
    StreamDocument toOutputDocument(mongo::Document doc);

    const int64_t _startMs;
    const int64_t _endMs;
    int64_t _minObservedEventTimeMs{std::numeric_limits<int64_t>::max()};
    int64_t _maxObservedEventTimeMs{0};
    int64_t _minObservedProcessingTime{std::numeric_limits<int64_t>::max()};
    std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> _pipeline;
    std::unique_ptr<DocumentSourceFeeder> _feeder;
    std::vector<mongo::Document> _earlyResults;
};

}  // namespace streams
