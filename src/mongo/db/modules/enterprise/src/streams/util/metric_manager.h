#pragma once

#include <boost/optional.hpp>
#include <string>
#include <vector>

#include "mongo/platform/mutex.h"
#include "mongo/util/string_map.h"
#include "streams/util/metrics.h"

namespace streams {

// Manages all the metrics. This class is thread-safe.
class MetricManager {
public:
    using LabelsVec = std::vector<std::pair<std::string, std::string>>;

    // Registers a new Counter.
    std::shared_ptr<Counter> registerCounter(std::string name, LabelsVec labels);

    // Registers a new Gauge.
    std::shared_ptr<Gauge> registerGauge(std::string name, LabelsVec labels);

    // Registers a new CallbackGauge.
    std::shared_ptr<CallbackGauge> registerCallbackGauge(std::string name,
                                                         LabelsVec labels,
                                                         CallbackGauge::CallbackFn fn);

    // Visits all metrics using the provided visitor.
    template <typename Visitor>
    void visitAllMetrics(Visitor* visitor);

private:
    // Encapsulates all the metadata for a metric.
    struct MetricInfo {
        // Unique name of the metric.
        std::string name;
        // Labels associated with this metric.
        LabelsVec labels;
        // Weak pointer to the metric.
        std::weak_ptr<Metric> metric;
    };

    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("MetricManager::mutex");
    // Tracks all registered metrics.
    std::list<MetricInfo> _metrics;
};

template <typename Visitor>
void MetricManager::visitAllMetrics(Visitor* visitor) {
    mongo::stdx::lock_guard<mongo::Latch> lock(_mutex);
    auto it = _metrics.begin();
    while (it != _metrics.end()) {
        auto& metricInfo = *it;
        auto metric = metricInfo.metric.lock();
        if (!metric) {
            it = _metrics.erase(it);
            continue;
        }

        if (auto counter = dynamic_cast<Counter*>(metric.get())) {
            visitor->visit(counter, metricInfo.name, metricInfo.labels);
        } else if (auto gauge = dynamic_cast<Gauge*>(metric.get())) {
            visitor->visit(gauge, metricInfo.name, metricInfo.labels);
        } else {
            auto callbackGauge = dynamic_cast<CallbackGauge*>(metric.get());
            invariant(callbackGauge);
            visitor->visit(callbackGauge, metricInfo.name, metricInfo.labels);
        }
        ++it;
    }
}

}  // namespace streams
