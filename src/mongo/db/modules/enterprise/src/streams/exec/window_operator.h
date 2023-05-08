#pragma once

#include <chrono>

#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/window_pipeline.h"

namespace streams {

class DeadLetterQueue;

/**
 * The initial implementation of streams $tumblingWindow.
 * Right now the class is built specifically as a time based window that
 * has a "size" and a "slide".
 */
class WindowOperator : public Operator {
public:
    struct Options {
        const std::vector<mongo::BSONObj> pipeline;
        boost::intrusive_ptr<mongo::ExpressionContext> expCtx;
        const int size;
        const mongo::StreamTimeUnitEnum sizeUnit;
        const int slide;
        const mongo::StreamTimeUnitEnum slideUnit;
        DeadLetterQueue* deadLetterQueue{nullptr};
    };

    WindowOperator(Options options);

protected:
    std::string doGetName() const override {
        return "WindowOperator";
    }

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;

    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    friend class WindowOperatorTest;

    bool windowContains(int64_t start, int64_t end, int64_t timestamp);
    std::map<int64_t, WindowPipeline>::iterator addWindow(int64_t start, int64_t end);
    bool shouldCloseWindow(int64_t windowEnd, int64_t watermarkint64_t);
    int64_t toOldestWindowStartTime(int64_t docint64_t);
    bool isTumblingWindow() const {
        return _windowSizeMs == _windowSlideMs;
    }

    // TODO(SERVER-75956): Use unordered map
    std::map<int64_t, WindowPipeline> _openWindows;

    std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> _innerPipelineTemplate;

    const Options _options;
    const int64_t _windowSizeMs;
    const int64_t _windowSlideMs;
};

}  // namespace streams
