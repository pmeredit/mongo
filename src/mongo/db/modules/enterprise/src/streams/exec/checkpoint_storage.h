#pragma once

#include <boost/optional.hpp>

#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/stream_stats.h"
#include "streams/util/metric_manager.h"

namespace streams {

struct Context;
class MetricManager;

// This is the interface that the Operators use to write and read checkpoint data.
class CheckpointStorage {
public:
    // An Operator obtains a WriterHandle for it to add state to a checkpoint.
    class WriterHandle {
    public:
        struct Options {
            CheckpointStorage* storage{nullptr};
            CheckpointId checkpointId;
            OperatorId operatorId;
        };

        ~WriterHandle() {
            _options.storage->closeStateWriter(this);
        }

        CheckpointId getCheckpointId() const {
            return _options.checkpointId;
        }

        CheckpointId getOperatorId() const {
            return _options.operatorId;
        }

    private:
        friend class InMemoryCheckpointStorage;
        friend class LocalDiskCheckpointStorage;

        WriterHandle(Options options) : _options(std::move(options)) {}

        Options _options;
    };

    // An Operator obtains a ReaderHandle for it to read state from a checkpoint.
    class ReaderHandle {
    public:
        struct Options {
            CheckpointStorage* storage{nullptr};
            CheckpointId checkpointId;
            OperatorId operatorId;
        };

        ~ReaderHandle() {
            _options.storage->closeStateReader(this);
        }

        CheckpointId getCheckpointId() const {
            return _options.checkpointId;
        }

        CheckpointId getOperatorId() const {
            return _options.operatorId;
        }

    private:
        friend class InMemoryCheckpointStorage;
        friend class LocalDiskCheckpointStorage;

        ReaderHandle(Options options) : _options(std::move(options)) {}

        Options _options;
    };

    virtual ~CheckpointStorage() = default;

    // Start a new checkpoint.
    CheckpointId startCheckpoint();

    // Commit an existing checkpoint. All state writer objects for this checkpoint must be destroyed
    // before commit is called.
    void commitCheckpoint(CheckpointId id);

    void startCheckpointRestore(CheckpointId chkId) {
        doStartCheckpointRestore(chkId);
    }

    void checkpointRestored(CheckpointId chkId) {
        doMarkCheckpointRestored(chkId);
    }

    // Returns the restore CheckpointId, if there is one.
    virtual boost::optional<CheckpointId> getRestoreCheckpointId() {
        return doGetRestoreCheckpointId();
    }

    // Obtain a writer object for a specific Operator within the checkpoint.
    std::unique_ptr<WriterHandle> createStateWriter(CheckpointId id, OperatorId opId) {
        return doCreateStateWriter(id, opId);
    }

    // Obtain a reader object for a specific Operator within the checkpoint.
    std::unique_ptr<ReaderHandle> createStateReader(CheckpointId id, OperatorId opId) {
        return doCreateStateReader(id, opId);
    }

    // Add a Document of state to an operator's checkpoint state.
    virtual void appendRecord(WriterHandle* writer, mongo::Document record) {
        return doAppendRecord(writer, std::move(record));
    }

    // Read the next Document of state from an operator's checkpoint state.
    // If none is returned, there is no more state for the operator in this checkpoint.
    virtual boost::optional<mongo::Document> getNextRecord(ReaderHandle* reader) {
        return doGetNextRecord(reader);
    }

    // Add operator stats to the checkpoint.
    void addStats(CheckpointId checkpointId, OperatorId operatorId, const OperatorStats& stats) {
        doAddStats(checkpointId, operatorId, stats);
    }

    // Get the CheckpointOperatorInfo from the restore checkpoint, which currently just contains
    // operator stats. It is expected that startCheckpointRestore is called before this method is
    // called.
    std::vector<mongo::CheckpointOperatorInfo> getRestoreCheckpointOperatorInfo() {
        return doGetRestoreCheckpointOperatorInfo();
    }

    // Registers a callback to be executed after a checkpoint is committed. The callback
    // is executed synchronously within `commitCheckpoint()`.
    void registerPostCommitCallback(std::function<void(CheckpointId)> callback) {
        invariant(!_postCommitCallback);
        _postCommitCallback = std::move(callback);
    }

    // This should be called once, any time after CheckpointStorage was constructed
    // and ideally before it starts doing any work.
    void registerMetrics(MetricManager* metricManager);

protected:
    explicit CheckpointStorage(Context* ctxt) : _context{ctxt} {}

    Context* _context{nullptr};
    // Callback thats executed after a checkpoint is committed. Its the responsibility of the
    // implementation to execute this in `doCommitCheckpoint()`.
    boost::optional<std::function<void(CheckpointId)>> _postCommitCallback;
    // A gauge metric to export the number of ongoing checkpoints
    // The MetricManager from which this was obtained maintains a weak_ptr to the gauge.
    // So, when not needed anymore, the gauge can simply be destroyed.
    std::shared_ptr<Gauge> _numOngoingCheckpointsGauge;

private:
    virtual CheckpointId doStartCheckpoint() = 0;
    virtual void doCommitCheckpoint(CheckpointId chkId) = 0;
    virtual void doStartCheckpointRestore(CheckpointId chkId) = 0;
    virtual void doMarkCheckpointRestored(CheckpointId chkId) = 0;
    virtual std::unique_ptr<WriterHandle> doCreateStateWriter(CheckpointId id, OperatorId opId) = 0;
    virtual std::unique_ptr<ReaderHandle> doCreateStateReader(CheckpointId id, OperatorId opId) = 0;
    virtual void doAppendRecord(WriterHandle* writer, mongo::Document record) = 0;
    virtual boost::optional<mongo::Document> doGetNextRecord(ReaderHandle* reader) = 0;
    virtual void doCloseStateReader(ReaderHandle* reader) = 0;
    virtual void doCloseStateWriter(WriterHandle* writer) = 0;
    virtual void doAddStats(CheckpointId checkpointId,
                            OperatorId operatorId,
                            const OperatorStats& stats) = 0;
    virtual std::vector<mongo::CheckpointOperatorInfo> doGetRestoreCheckpointOperatorInfo() = 0;
    virtual boost::optional<CheckpointId> doGetRestoreCheckpointId() = 0;

    void closeStateReader(ReaderHandle* reader) {
        doCloseStateReader(reader);
    }
    void closeStateWriter(WriterHandle* writer) {
        doCloseStateWriter(writer);
    }
};

}  // namespace streams
