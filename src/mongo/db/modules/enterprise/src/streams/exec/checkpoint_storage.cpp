/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/checkpoint_storage.h"

#include <chrono>

#include "mongo/idl/idl_parser.h"
#include "mongo/util/duration.h"
#include "streams/exec/context.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/log_util.h"
#include "streams/util/metric_manager.h"

using namespace mongo;
using namespace std::chrono;
namespace streams {

CheckpointStorage::CheckpointStorage(Context* ctxt)
    : _context{ctxt}, _memoryUsageHandle{_context->memoryAggregator->createUsageHandle()} {}

CheckpointId CheckpointStorage::startCheckpoint() {
    invariant(_numOngoingCheckpointsGauge);
    CheckpointId id = doStartCheckpoint();
    _numOngoingCheckpointsGauge->set(_numOngoingCheckpointsGauge->value() + 1);
    _lastCheckpointStartTs = mongo::Date_t::now();
    return id;
}

void CheckpointStorage::commitCheckpoint(CheckpointId id) {
    invariant(_numOngoingCheckpointsGauge);
    doCommitCheckpoint(id);
    _numOngoingCheckpointsGauge->set(_numOngoingCheckpointsGauge->value() - 1);
    _numCheckpointsTaken->increment(1);
    _lastCheckpointCommitTs = mongo::Date_t::now();
    _checkpointProduceDurations->increment(
        (_lastCheckpointCommitTs - _lastCheckpointStartTs).count());
    _memoryUsageHandle.set(0);
}

std::deque<mongo::CheckpointDescription> CheckpointStorage::getFlushedCheckpoints() {
    std::deque<mongo::CheckpointDescription> result;
    std::swap(result, _flushedCheckpoints);
    return result;
}

void CheckpointStorage::onCheckpointFlushed(CheckpointId checkpointId) {
    auto bson = _unflushedCheckpoints.pop(checkpointId);
    auto description = CheckpointDescription::parseOwned(
        IDLParserContext{"CheckpointStorage::notifyCheckpointFlushed"}, std::move(bson));
    _flushedCheckpoints.push_back(std::move(description));
}

void CheckpointStorage::addUnflushedCheckpoint(CheckpointId checkpointId,
                                               mongo::CheckpointDescription description) {
    _unflushedCheckpoints.add(checkpointId, description.toBSON());
}

mongo::CheckpointDescription CheckpointStorage::startCheckpointRestore(CheckpointId chkId) {
    _lastCheckpointRestoreStartTs = mongo::Date_t::now();
    return doStartCheckpointRestore(chkId);
}

Milliseconds CheckpointStorage::checkpointRestored(CheckpointId chkId) {
    doMarkCheckpointRestored(chkId);
    _lastCheckpointRestoreDoneTs = mongo::Date_t::now();
    Milliseconds duration = _lastCheckpointRestoreDoneTs - _lastCheckpointRestoreStartTs;
    _checkpointRestoreDurations->increment(duration.count());
    return duration;
}

Milliseconds CheckpointStorage::durationSinceLastCommit() const {
    return Date_t::now() - _lastCheckpointCommitTs;
}

void CheckpointStorage::registerMetrics(MetricManager* metricManager) {
    invariant(metricManager);
    auto labelsVec = getDefaultMetricLabels(_context);
    _numOngoingCheckpointsGauge = metricManager->registerGauge(
        "checkpoint_num_ongoing",
        "Number of ongoing checkpoints that have started but not yet committed.",
        labelsVec,
        /* initialValue */ 0);
    _maxMemoryUsageBytes =
        metricManager->registerGauge("checkpoint_max_memory_usage_bytes",
                                     "Maximum memory used while taking a checkpoint",
                                     labelsVec,
                                     0);
    _numCheckpointsTaken = metricManager->registerCounter(
        "num_checkpoints_taken",
        "Total number of checkpoints taken by this CheckpointStorage instance so far",
        labelsVec);
    _durationSinceLastCommit = metricManager->registerCallbackGauge(
        "duration_since_last_checkpoint_ms",
        "Duration in milliseconds since last checkpoint was committed",
        labelsVec,
        [this]() -> double { return durationSinceLastCommit().count(); });
    _checkpointProduceDurations = metricManager->registerHistogram(
        "checkpoint_produce_duration_ms",
        "Duration histograms (in milliseconds) for the checkpoint start to finish durations",
        labelsVec,
        makeExponentialDurationBuckets(256ms, 2, 12));
    _checkpointRestoreDurations = metricManager->registerHistogram(
        "checkpoint_restore_duration_ms",
        "Duration histograms (in milliseconds) for the checkpoint start to finish durations",
        labelsVec,
        makeExponentialDurationBuckets(256ms, 2, 12));
    _checkpointSizeBytes =
        metricManager->registerHistogram("checkpoint_bytes",
                                         "Histograms of checkpoint sizes in bytes.",
                                         labelsVec,
                                         makeExponentialValueBuckets(1024 * 1024, 2, 14));
}

}  // namespace streams
