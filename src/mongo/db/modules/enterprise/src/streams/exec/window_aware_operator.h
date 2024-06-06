/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/context.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/window_assigner.h"

namespace streams {

/**
  * This is the abstract base class for window aware implementations of $group, $sort, and $limit.
  * This class manages the map of the open windows. Each open window has some state.
  * The state depends on the derived class (group, sort, or limit).

  * This class also takes a "windowAssigner" option.
  * The windowAssigner is set if this is the first stateful operator in the window's inner pipeline.
  * If windowAssigner is set, the instance will:
  *     1) assign documents to particular windows
  *     2) close windows based on the source watermark
  * If windowAssigner is not set, the instance:
  *     1) expects an upstream operator to set the streamMeta.startWindowTimestamp on all documents
  *     2) expects an upstream operator to send windowCloseSignals control messages

  * An example $tumblingWindow[$group, $project, $sort, $group] will parse into:
  * WindowAwareGroupOperator : WindowAwareOperator(windowAssigner = true)
  * ProjectOperator
  * WindowAwareSortOperator : WindowAwareOperator(windowAssigner = false)
  * WindowAwareGroupOperator : WindowAwareOperator(windowAssigner = false)
 */
class WindowAwareOperator : public Operator {
public:
    struct Options {
        // If set, this instance will assign documents to windows and translate source watermarks
        // into window close events.
        std::unique_ptr<WindowAssigner> windowAssigner;

        // If true, this instance will send window close signals downstream. It is set to true
        // whenever there is another stateful window aware operator downstream (like in the
        // $window[$group, $sort] case).
        bool sendWindowCloseSignal{false};
    };

    WindowAwareOperator(Context* context) : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1) {}

protected:
    // Tracks stats for one window.
    struct PerWindowStats {
        // The number of docs input to the window.
        int32_t numInputDocs{0};
        // We track the memory usage at a per-window level.
        // The doStats implementations sums them all together.
        int64_t memoryUsageBytes{0};
    };

    // The base class for the state of a single open window.
    struct Window {
        virtual ~Window() = default;

        Window(mongo::StreamMeta streamMetaTemplate)
            : streamMetaTemplate(std::move(streamMetaTemplate)) {}

        // The streamMetaTemplate for this window. This streamMeta is applied to all output
        // docs for this window.
        mongo::StreamMeta streamMetaTemplate;
        // The status of this window. Certain errors set this to a non-OK status.
        // Future input to this window is ignored, and a DLQ message is written when the
        // window is closed.
        mongo::Status status{mongo::Status::OK()};
        // Stats for this window.
        PerWindowStats stats;
    };

    void doStart() override;

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;

    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

    // WindowAwareOperator has some special handling for memoryUsageBytes.
    // The memoryUsageBytes of each open window are summed.
    // The other stats (ex. numInputDocs, numDlqDocs) work the same as the other operators.
    OperatorStats doGetStats() override;

    // Called when the stats need to be refreshed for a single window.
    void updateStats(Window* window) {
        doUpdateStats(window);
    }

private:
    friend class WindowAwareOperatorTest;
    friend class WindowOperatorTest;
    friend class WindowOperatorBMFixture;

    // Assigns the docs in the input to windows and processes each.
    void assignWindowsAndProcessDataMsg(StreamDataMsg dataMsg);

    // Process documents for a particular window.
    void processDocsInWindow(int64_t windowStartTime,
                             int64_t windowEndTime,
                             std::vector<StreamDocument> streamDocs,
                             bool projectMetadata);

    // Creates a Window object representing an open window.
    std::unique_ptr<Window> makeWindow(mongo::StreamMeta streamMetaTemplate);

    // Add a new window or get an existing window.
    Window* addOrGetWindow(int64_t windowStartTime,
                           int64_t windowEndTime,
                           boost::optional<mongo::StreamMetaSourceTypeEnum> sourceType);

    // Called when a window is closed. Sends the window output to the next operator.
    void closeWindow(Window* window);

    // Save all the open window state in the specified checkpoint.
    void saveState(CheckpointId checkpointId);

    // Restore all the open window state from the specified checkpoint.
    void restoreState(CheckpointId checkpointId);

    // The derived class should process all of the docs, using the objects in the supplied
    // Window.
    virtual void doProcessDocs(Window* window, std::vector<StreamDocument> docs) = 0;

    // The derived class should create a new Window.
    virtual std::unique_ptr<Window> doMakeWindow(Window baseState) = 0;

    // The derived class should send all results for this window to the next operator.
    // All output should have the specified meta.
    virtual void doCloseWindow(Window* window) = 0;

    // Write the data in the window to the checkpoint storage.
    // All data written should use top level field names specified in the
    // WindowOperatorCheckpointRecord IDL.
    virtual void doSaveWindowState(CheckpointStorage::WriterHandle* writer, Window* window) = 0;

    // Read the record and add its data to the window.
    virtual void doRestoreWindowState(Window* window, mongo::Document record) = 0;

    // The derived class should update the stats' memoryUsageBytes.
    virtual void doUpdateStats(Window* window) = 0;

    virtual const Options& getOptions() const = 0;

    // Sends a DLQ message for the windows this doc missed.
    void sendLateDocDlqMessage(const StreamDocument& doc, int64_t minEligibleStartTime);

    // Process a watermark message, which might close some windows.
    void processWatermarkMsg(StreamControlMsg controlMsg);

    // The map of open windows. The key to the map is the window start time in millis.
    std::map<int64_t, std::unique_ptr<Window>> _windows;
    // The largest watermark this operator has sent.
    int64_t _maxSentWatermarkMs{0};
    // Windows before this start time are already closed.
    int64_t _minWindowStartTime{0};
    // The max watermark received from the input, minus allowedLateness.
    int64_t _maxReceivedWatermarkMs{-1};
    // Set when a kIdle message is received from the source.
    // Unset whenever a data message or kActive watermark is received.
    // If this is set, the idle timeout occurs if another kIdle message is received
    // when the wall time is greater than _idleStartTime + _idleTimeoutMs + _windowSizeMs
    boost::optional<int64_t> _idleStartTime;
};

}  // namespace streams
