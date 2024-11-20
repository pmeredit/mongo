/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>

#include "mongo/util/chunked_memory_aggregator.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/restored_checkpoint_info.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/unflushed_state_container.h"
#include "streams/util/metric_manager.h"
#include "streams/util/metrics.h"

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

    // Start a checkpoint restore for the specified checkpoint ID.
    RestoredCheckpointInfo startCheckpointRestore(CheckpointId chkId);

    // Mark the checkpoint restore as complete. Returns the restore duration in milliseconds.
    // TODO(SERVER-82127): Once Executor kicks off restore, just have Executor compute the restore
    // time.
    mongo::Milliseconds checkpointRestored(CheckpointId chkId);

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

    // This should be called once, any time after CheckpointStorage was constructed
    // and before it starts doing any work. If this is not done, it will lead
    // to an invariant violation
    void registerMetrics(MetricManager* metricManager);

    // Called by the Executor after the streams Agent has flushed a checkpoint to remote storage.
    void onCheckpointFlushed(CheckpointId id);

    // The executor calls this to get the checkpoints that have been flushed to remote storage.
    std::deque<mongo::CheckpointDescription> getFlushedCheckpoints();

    // The byte size of the last checkpoint committed.
    int64_t getLastCheckpointSizeBytes() {
        return _lastCheckpointSizeBytes;
    }

protected:
    explicit CheckpointStorage(Context* ctxt);

    Context* _context{nullptr};
    // The MetricManager responsible for these metrics maintains a weak_ptr to them.
    // So, when not needed anymore, they can simply be destroyed.
    std::shared_ptr<Gauge> _numOngoingCheckpointsGauge;
    // Maximum memory ever used when taking a checkpoint
    std::shared_ptr<Gauge> _maxMemoryUsageBytes;
    std::shared_ptr<Counter> _numCheckpointsTaken;
    std::shared_ptr<CallbackGauge> _durationSinceLastCommit;
    // Time from checkpoint start to commit
    std::shared_ptr<Histogram> _checkpointProduceDurations;
    // Time from checkpoint restore start to finish
    std::shared_ptr<Histogram> _checkpointRestoreDurations;
    // Checkpoint size histogram
    std::shared_ptr<Histogram> _checkpointSizeBytes;
    // Size of last committed checkpoint. This is reported in stats
    int64_t _lastCheckpointSizeBytes{0};
    mongo::Date_t _lastCheckpointStartTs{mongo::Date_t::now()};
    mongo::Date_t _lastCheckpointCommitTs{mongo::Date_t::now()};
    mongo::Date_t _lastCheckpointRestoreStartTs{mongo::Date_t::now()};
    mongo::Date_t _lastCheckpointRestoreDoneTs{mongo::Date_t::now()};
    // Memory usage is reported via this handle. This handle is obtained from
    // the ChunkedMemoryAggregator of the Context
    mongo::MemoryUsageHandle _memoryUsageHandle;

protected:
    // The derived class should call this right before it commits a checkpoint.
    void addUnflushedCheckpoint(CheckpointId checkpointId,
                                mongo::CheckpointDescription description);

private:
    friend class KafkaConsumerOperatorTest;
    friend class StreamManagerTest;

    virtual CheckpointId doStartCheckpoint() = 0;
    virtual void doCommitCheckpoint(CheckpointId chkId) = 0;
    virtual RestoredCheckpointInfo doStartCheckpointRestore(CheckpointId chkId) = 0;
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
    virtual boost::optional<CheckpointId> doGetRestoreCheckpointId() = 0;
    void closeStateReader(ReaderHandle* reader) {
        doCloseStateReader(reader);
    }
    void closeStateWriter(WriterHandle* writer) {
        doCloseStateWriter(writer);
    }
    mongo::Milliseconds durationSinceLastCommit() const;

    // Tracks the checkpoints that have been committed to local disk but not yet flushed to remote
    // storage.
    UnflushedStateContainer _unflushedCheckpoints;
    // Tracks the flushed checkpoints that the Executor has not yet processed.
    std::deque<mongo::CheckpointDescription> _flushedCheckpoints;
};

}  // namespace streams
