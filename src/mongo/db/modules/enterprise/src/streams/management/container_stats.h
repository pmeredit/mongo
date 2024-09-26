#pragma once

#include "mongo/platform/atomic.h"
#include "streams/management/container_group_stats_provider.h"
#include "streams/util/metric_manager.h"

namespace streams {

/*
** ContainerStats
** class responsible for getting system cpu/memory related metrics for the mstream container.
** calculates cpuPercent, memPercent, memLimit for the container and updates StreamManager them
*together.
*/
class ContainerStats {
public:
    ContainerStats(
        std::unique_ptr<ContainerGroupStatsProvider> containerGroupStatsProvider = nullptr);
    void shutdown() {
        _shutdown.store(true);
        _containerStatsThread->join();
    }
    MetricManager* getMetricManager() {
        return _metricManager.get();
    }

private:
    void run();

    mongo::Atomic<bool> _shutdown;
    std::unique_ptr<MetricManager> _metricManager;
    std::shared_ptr<Gauge> _cpuPercentGauge;
    std::shared_ptr<Gauge> _memPercentGauge;
    std::shared_ptr<IntGauge> _memLimitGauge;
    std::unique_ptr<mongo::stdx::thread> _containerStatsThread;
    std::unique_ptr<ContainerGroupStatsProvider> _containerGroupStatsProvider;
};

}  // namespace streams
