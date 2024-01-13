/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/checkpoint_storage.h"

#include "streams/exec/log_util.h"
#include "streams/util/metric_manager.h"

namespace streams {

CheckpointId CheckpointStorage::startCheckpoint() {
    CheckpointId id = doStartCheckpoint();
    if (_numOngoingCheckpointsGauge) {
        _numOngoingCheckpointsGauge->set(_numOngoingCheckpointsGauge->value() + 1);
    }
    return id;
}

void CheckpointStorage::commitCheckpoint(CheckpointId id) {
    doCommitCheckpoint(id);
    if (_postCommitCallback) {
        _postCommitCallback.get()(id);
    }
    if (_numOngoingCheckpointsGauge) {
        _numOngoingCheckpointsGauge->set(_numOngoingCheckpointsGauge->value() - 1);
    }
}

void CheckpointStorage::registerMetrics(MetricManager* metricManager) {
    invariant(metricManager);
    _numOngoingCheckpointsGauge = metricManager->registerGauge(
        "checkpoint_num_ongoing",
        "Number of ongoing checkpoints that have started but not yet committed.",
        getDefaultMetricLabels(_context),
        /* initialValue */ 0);
}

}  // namespace streams
