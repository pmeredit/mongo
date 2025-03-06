/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/util/metric_manager.h"

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "mongo/stdx/mutex.h"
#include "streams/util/metrics.h"

using namespace mongo;

namespace streams {

std::shared_ptr<Counter> MetricManager::registerCounter(std::string name,
                                                        std::string description,
                                                        Metric::LabelsVec labels) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto counter = std::make_shared<Counter>(name, description, std::move(labels));
    _metrics.push_back(counter);
    return counter;
}

std::shared_ptr<CounterVec> MetricManager::registerCounterVec(
    std::string name,
    std::string description,
    Metric::LabelsVec baseLabels,
    CounterVec::LabelNames extraLabelNames) {
    mongo::stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto counterVec = std::make_shared<CounterVec>(
        std::move(name), std::move(description), std::move(baseLabels), std::move(extraLabelNames));
    _metrics.push_back(counterVec);
    return counterVec;
}

std::shared_ptr<Gauge> MetricManager::registerGauge(std::string name,
                                                    std::string description,
                                                    Metric::LabelsVec labels,
                                                    double initialValue) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto gauge = std::make_shared<Gauge>(name, description, labels);
    gauge->set(initialValue);
    _metrics.push_back(gauge);
    return gauge;
}

std::shared_ptr<IntGauge> MetricManager::registerIntGauge(std::string name,
                                                          std::string description,
                                                          Metric::LabelsVec labels,
                                                          int64_t initialValue) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto gauge = std::make_shared<IntGauge>(name, description, labels);
    gauge->set(initialValue);
    _metrics.push_back(gauge);
    return gauge;
}

std::shared_ptr<CallbackGauge> MetricManager::registerCallbackGauge(std::string name,
                                                                    std::string description,
                                                                    Metric::LabelsVec labels,
                                                                    CallbackGauge::CallbackFn fn) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto gauge = std::make_shared<CallbackGauge>(name, description, labels, std::move(fn));
    _metrics.push_back(gauge);
    return gauge;
}

std::shared_ptr<Histogram> MetricManager::registerHistogram(std::string name,
                                                            std::string description,
                                                            Metric::LabelsVec labels,
                                                            std::vector<int64_t> buckets) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto histogram = std::make_shared<Histogram>(name, description, labels, std::move(buckets));
    _metrics.push_back(std::move(histogram));

    return histogram;
}

void MetricManager::takeSnapshot() {
    for (auto& metric : computeMetricsToVisit()) {
        metric->takeSnapshot();
    }
}

std::vector<std::shared_ptr<Collector>> MetricManager::computeMetricsToVisit() {
    mongo::stdx::lock_guard<mongo::stdx::mutex> lock(_mutex);
    std::vector<std::shared_ptr<Collector>> metricsToVisit;
    auto it = _metrics.begin();
    while (it != _metrics.end()) {
        auto metric = it->lock();
        if (!metric) {
            it = _metrics.erase(it);
            continue;
        }

        metricsToVisit.push_back(std::move(metric));
        ++it;
    }
    return metricsToVisit;
}

}  // namespace streams
