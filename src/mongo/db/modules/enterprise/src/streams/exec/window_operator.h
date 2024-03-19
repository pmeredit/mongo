#pragma once

#include <chrono>
#include <deque>
#include <map>

#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/window_pipeline.h"
#include "streams/util/metric_manager.h"
#include "streams/util/metrics.h"

namespace streams {

class DeadLetterQueue;
class Planner;
struct Context;

/**
 * The initial implementation of streams $tumblingWindow.
 * Right now the class is built specifically as a time based window that
 * has a "size" and a "slide".
 */
class WindowOperator : public Operator {
public:
    struct Options {
        int size;
        mongo::StreamTimeUnitEnum sizeUnit;
        int slide;
        mongo::StreamTimeUnitEnum slideUnit;
        int offsetFromUtc{0};
        mongo::StreamTimeUnitEnum offsetUnit{mongo::StreamTimeUnitEnum::Millisecond};
        boost::optional<mongo::StreamTimeUnitEnum> idleTimeoutUnit;
        boost::optional<int> idleTimeoutSize;
        int64_t allowedLatenessMs{0};
        std::vector<mongo::BSONObj> pipeline;
        // Specifies the [min, max] range of OperatorIds to use for the Operators in the inner
        // pipeline.
        boost::optional<std::pair<OperatorId, OperatorId>> minMaxOperatorIds;
    };

    WindowOperator(Context* context, Options options);

    OperatorStats doGetStats() override;
    void registerMetrics(MetricManager* executor) override;

protected:
    std::string doGetName() const override {
        return "WindowOperator";
    }

    void doStart() override;
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;

    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    friend class PlannerTest;
    friend class WindowOperatorBMFixture;
    friend class WindowOperatorTest;

    struct OpenWindow {
        // The pipeline used to compute the results of an open window.
        WindowPipeline pipeline;
        // The max checkpointId that arrived before the window was opened.
        CheckpointId priorCheckpointId{0};
    };

    // Initializes the internal state from a checkpoint.
    void initFromCheckpoint();

    bool windowContains(int64_t start, int64_t end, int64_t timestamp);
    std::map<int64_t, OpenWindow>::iterator addWindow(int64_t start, int64_t end);
    bool shouldCloseWindow(int64_t windowEnd, int64_t watermarkint64_t);
    int64_t toOldestWindowStartTime(int64_t docTime);
    bool isTumblingWindow() const {
        return _windowSizeMs == _windowSlideMs;
    }

    bool processWatermarkMsg(StreamControlMsg controlMsg);
    void sendCheckpointMsg(CheckpointId maxCheckpointIdToSend);
    // If true, fast mode checkpointing is enabled.
    // The basic idea of fast mode checkpointing is that we can recompute an event time window
    // if we rewind far back enough in the $source.
    // In fast mode checkpointing, we may not send along checkpoint messages as we receive them.
    // We only send along checkpoint messages once it's safe to commit them. It's safe to commit
    // a checkpointId when it was received before all open windows.
    bool isCheckpointingEnabled();

    // Sends a DLQ message for the windows this doc missed.
    void sendLateDocDlqMessage(const StreamDocument& doc, int64_t minEligibleStartTime);

    // TODO(SERVER-76722): Use unordered map
    std::map<int64_t, OpenWindow> _openWindows;

    const Options _options;
    const int64_t _windowSizeMs;
    const int64_t _windowSlideMs;
    const int64_t _windowOffsetMs;
    boost::optional<int64_t> _idleTimeoutMs;
    // Exports number of windows currently open.
    std::shared_ptr<Gauge> _numOpenWindowsGauge;

    // Set when a kIdle message is received from the source.
    // Unset whenever a data message or kActive watermark is received.
    // If this is set, the idle timeout occurs if another kIdle message is received
    // when the wall time is greater than _idleStartTime + _idleTimeoutMs + _windowSizeMs
    boost::optional<int64_t> _idleStartTime;
    // Windows before this start time are ignored. This is set in initFromCheckpoint() and
    // updated when windows get closed.
    int64_t _minWindowStartTime{0};
    // checkpointIds received from the input but not yet sent to the output.
    std::deque<CheckpointId> _unsentCheckpointIds;
    // Most recent checkpointId sent to the output.
    CheckpointId _maxSentCheckpointId{0};
    int64_t _maxSentWatermarkMs{0};
    // We closed at least one window after start (or restoring a checkpoint)
    // used solely to avoid sending DLQ messages for windows closed before restart
    // to fix issues with fast checkpoint and replayed messages.
    bool _closedFirstWindowAfterStart{false};
};

}  // namespace streams
