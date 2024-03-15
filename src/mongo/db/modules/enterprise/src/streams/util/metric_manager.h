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
    std::shared_ptr<Counter> registerCounter(std::string name,
                                             std::string description,
                                             LabelsVec labels);

    // Registers a new Gauge.
    std::shared_ptr<Gauge> registerGauge(std::string name,
                                         std::string description,
                                         LabelsVec labels,
                                         double initialValue = 0);

    // Registers a new IntGauge.
    std::shared_ptr<IntGauge> registerIntGauge(std::string name,
                                               std::string description,
                                               LabelsVec labels,
                                               int64_t initialValue = 0);

    // Registers a new CallbackGauge.
    std::shared_ptr<CallbackGauge> registerCallbackGauge(std::string name,
                                                         std::string description,
                                                         LabelsVec labels,
                                                         CallbackGauge::CallbackFn fn);

    // Registers a new Histogram.
    std::shared_ptr<Histogram> registerHistogram(std::string name,
                                                 std::string description,
                                                 LabelsVec labels,
                                                 std::vector<int64_t> buckets);

    // Visits all metrics using the provided visitor.
    template <typename Visitor>
    void visitAllMetrics(Visitor* visitor);

    // Encapsulates all the metadata for a metric.
    struct MetricInfo {
        // Unique name of the metric.
        std::string name;
        // Description of the metric.
        std::string description;
        // Labels associated with this metric.
        LabelsVec labels;
        // Weak pointer to the metric.
        std::weak_ptr<Metric> metric;
    };

    void takeSnapshot();

private:
    std::vector<std::shared_ptr<MetricInfo>> computeMetricsToVisit();
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("MetricManager::mutex");
    // Tracks all registered metrics.
    std::list<std::shared_ptr<MetricInfo>> _metrics;
};

template <typename Visitor>
void MetricManager::visitAllMetrics(Visitor* visitor) {
    // Note: we release the _mutex before visiting each metric.
    for (auto& metricInfo : computeMetricsToVisit()) {
        auto metric = metricInfo->metric.lock();
        if (!metric) {
            continue;
        }
        if (auto counter = dynamic_cast<Counter*>(metric.get())) {
            visitor->visit(counter, metricInfo->name, metricInfo->description, metricInfo->labels);
        } else if (auto gauge = dynamic_cast<Gauge*>(metric.get())) {
            visitor->visit(gauge, metricInfo->name, metricInfo->description, metricInfo->labels);
        } else if (auto intGauge = dynamic_cast<IntGauge*>(metric.get())) {
            visitor->visit(intGauge, metricInfo->name, metricInfo->description, metricInfo->labels);
        } else if (auto callbackGauge = dynamic_cast<CallbackGauge*>(metric.get())) {
            visitor->visit(
                callbackGauge, metricInfo->name, metricInfo->description, metricInfo->labels);
        } else if (auto histogram = dynamic_cast<Histogram*>(metric.get())) {
            visitor->visit(
                histogram, metricInfo->name, metricInfo->description, metricInfo->labels);
        } else {
            MONGO_UNREACHABLE;
        }
    }
}

}  // namespace streams
