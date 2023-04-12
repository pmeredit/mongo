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
    void process(mongo::Document doc, int64_t time);

    /**
     * Returns the entire output of the window.
     * TODO(STREAMS-220)-PrivatePreview: chunk up the output,
     * right now we're just build one (potentially giant) StreamDataMsg.
     */
    StreamDataMsg close();

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
    std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> _pipeline;
    std::unique_ptr<DocumentSourceFeeder> _feeder;
    std::vector<StreamDocument> _earlyResults;
};

}  // namespace streams
