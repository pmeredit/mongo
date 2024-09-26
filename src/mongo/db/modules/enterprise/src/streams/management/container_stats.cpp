/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "mongo/util/assert_util.h"
#include <boost/optional.hpp>
#include <string>

#include "mongo/logv2/log.h"
#include "mongo/util/time_support.h"
#include "mongo/util/timer.h"
#include "streams/management/container_stats.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

ContainerStats::ContainerStats(
    std::unique_ptr<ContainerGroupStatsProvider> containerGroupStatsProvider) {
    if (containerGroupStatsProvider == nullptr) {
        try {
            _containerGroupStatsProvider = std::make_unique<ContainerGroupStatsProvider>();
        } catch (const std::exception& ex) {
            LOGV2_WARNING(
                9481401, "Caught exception in ContainerStats", "exception"_attr = ex.what());
        }
    } else {
        _containerGroupStatsProvider = std::move(containerGroupStatsProvider);
    }
    _metricManager = std::make_unique<MetricManager>();
    _cpuPercentGauge =
        _metricManager->registerGauge("system_cpu_percent", "CPU percent for the system", {});

    _memPercentGauge =
        _metricManager->registerGauge("system_mem_percent", "Memory percent for the system", {});

    _memLimitGauge =
        _metricManager->registerIntGauge("system_mem_limit", "Memory limit for the system", {});
    _containerStatsThread = std::make_unique<mongo::stdx::thread>(&ContainerStats::run, this);
}

void ContainerStats::run() {
    int nCores;

    if (_containerGroupStatsProvider == nullptr) {
        return;
    }
    try {
        auto tuple = _containerGroupStatsProvider->getQuotaPeriod();
        if (tuple) {
            nCores = std::get<0>(*tuple) / std::get<1>(*tuple);
        } else {
            uasserted(9481405, str::stream() << "Cannot get quota/period from cgroup files");
            return;
        }
        boost::optional<int64_t> memLimit = _containerGroupStatsProvider->getMemoryLimitBytes();
        boost::optional<int64_t> prevCpuUsage{};
        boost::optional<int64_t> currentCpuUsage{};
        if (memLimit) {
            _memLimitGauge->set(*memLimit);
        } else {
            uasserted(9481406, str::stream() << "Cannot get memLimit cgroup");
            return;
        }
        while (!_shutdown.load()) {
            currentCpuUsage = _containerGroupStatsProvider->getCpuTimeInMicroseconds();

            if (prevCpuUsage && currentCpuUsage) {
                // cpuUsage in microseconds for cgroup v2 files.
                // the difference is divided by time difference
                // further divided to account for multiple cores.
                _cpuPercentGauge->set(100.0 * (double)(*currentCpuUsage - *prevCpuUsage) /
                                      (_containerGroupStatsProvider->elapsedMicros() * nCores));
            }

            boost::optional<int64_t> currentMemUsage =
                _containerGroupStatsProvider->getMemoryUsageBytes();
            if (memLimit && currentMemUsage) {
                _memPercentGauge->set(100.0 * (double)*currentMemUsage / *memLimit);
            }

            if (currentCpuUsage) {
                _containerGroupStatsProvider->resetTimer();
            }
            prevCpuUsage = currentCpuUsage;
            mongo::sleepmillis(10000);
        }
    } catch (const std::exception& ex) {
        LOGV2_WARNING(9481402, "Caught exception in ContainerStats", "exception"_attr = ex.what());
    }
}

}  // namespace streams
