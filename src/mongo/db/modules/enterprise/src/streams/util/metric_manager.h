/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/stdx/mutex.h"
#include "mongo/util/string_map.h"
#include "streams/util/metrics.h"

namespace streams {

// Manages all the metrics. This class is thread-safe.
class MetricManager {
public:
    // Registers a new Counter.
    std::shared_ptr<Counter> registerCounter(std::string name,
                                             std::string description,
                                             Metric::LabelsVec labels);

    std::shared_ptr<CounterVec> registerCounterVec(std::string name,
                                                   std::string description,
                                                   Metric::LabelsVec baseLabels,
                                                   CounterVec::LabelNames extraLabelNames);

    // Registers a new Gauge.
    std::shared_ptr<Gauge> registerGauge(std::string name,
                                         std::string description,
                                         Metric::LabelsVec labels,
                                         double initialValue = 0);

    // Registers a new IntGauge.
    std::shared_ptr<IntGauge> registerIntGauge(std::string name,
                                               std::string description,
                                               Metric::LabelsVec labels,
                                               int64_t initialValue = 0);

    // Registers a new CallbackGauge.
    std::shared_ptr<CallbackGauge> registerCallbackGauge(std::string name,
                                                         std::string description,
                                                         Metric::LabelsVec labels,
                                                         CallbackGauge::CallbackFn fn);

    // Registers a new Histogram.
    std::shared_ptr<Histogram> registerHistogram(std::string name,
                                                 std::string description,
                                                 Metric::LabelsVec labels,
                                                 std::vector<int64_t> buckets);

    // Visits all metrics using the provided visitor.
    template <typename Visitor>
    void visitAllMetrics(Visitor* visitor);

    void takeSnapshot();

private:
    std::vector<std::shared_ptr<Collector>> computeMetricsToVisit();
    mutable mongo::stdx::mutex _mutex;
    // Tracks all registered metrics.
    std::list<std::weak_ptr<Collector>> _metrics;
};

template <typename Visitor>
void MetricManager::visitAllMetrics(Visitor* visitor) {
    // Note: we release the _mutex before visiting each metric.
    for (auto& metric : computeMetricsToVisit()) {
        if (auto counter = dynamic_cast<Counter*>(metric.get())) {
            visitor->visit(counter);
        } else if (auto counterVec = dynamic_cast<CounterVec*>(metric.get())) {
            for (const auto& counter : counterVec->getCounters()) {
                visitor->visit(counter.get());
            }
        } else if (auto gauge = dynamic_cast<Gauge*>(metric.get())) {
            visitor->visit(gauge);
        } else if (auto intGauge = dynamic_cast<IntGauge*>(metric.get())) {
            visitor->visit(intGauge);
        } else if (auto callbackGauge = dynamic_cast<CallbackGauge*>(metric.get())) {
            visitor->visit(callbackGauge);
        } else if (auto histogram = dynamic_cast<Histogram*>(metric.get())) {
            visitor->visit(histogram);
        } else {
            MONGO_UNREACHABLE;
        }
    }
}

}  // namespace streams
