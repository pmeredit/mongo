/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/util/metric_manager.h"

using namespace mongo;

namespace streams {

std::shared_ptr<Counter> MetricManager::registerCounter(std::string name,
                                                        std::string description,
                                                        LabelsVec labels) {
    stdx::lock_guard<Latch> lock(_mutex);
    auto counter = std::make_shared<Counter>();
    auto metricInfo = std::make_shared<MetricInfo>();
    metricInfo->name = std::move(name);
    metricInfo->description = std::move(description);
    metricInfo->labels = std::move(labels);
    metricInfo->metric = counter;
    _metrics.push_back(std::move(metricInfo));
    return counter;
}

std::shared_ptr<Gauge> MetricManager::registerGauge(std::string name,
                                                    std::string description,
                                                    LabelsVec labels,
                                                    double initialValue) {
    stdx::lock_guard<Latch> lock(_mutex);
    auto gauge = std::make_shared<Gauge>();
    gauge->set(initialValue);
    auto metricInfo = std::make_shared<MetricInfo>();
    metricInfo->name = std::move(name);
    metricInfo->description = std::move(description);
    metricInfo->labels = std::move(labels);
    metricInfo->metric = gauge;
    _metrics.push_back(std::move(metricInfo));
    return gauge;
}

std::shared_ptr<IntGauge> MetricManager::registerIntGauge(std::string name,
                                                          std::string description,
                                                          LabelsVec labels,
                                                          int64_t initialValue) {
    stdx::lock_guard<Latch> lock(_mutex);
    auto gauge = std::make_shared<IntGauge>();
    gauge->set(initialValue);
    auto metricInfo = std::make_shared<MetricInfo>();
    metricInfo->name = std::move(name);
    metricInfo->description = std::move(description);
    metricInfo->labels = std::move(labels);
    metricInfo->metric = gauge;
    _metrics.push_back(std::move(metricInfo));
    return gauge;
}

std::shared_ptr<CallbackGauge> MetricManager::registerCallbackGauge(std::string name,
                                                                    std::string description,
                                                                    LabelsVec labels,
                                                                    CallbackGauge::CallbackFn fn) {
    stdx::lock_guard<Latch> lock(_mutex);
    auto gauge = std::make_shared<CallbackGauge>(std::move(fn));
    auto metricInfo = std::make_shared<MetricInfo>();
    metricInfo->name = std::move(name);
    metricInfo->description = std::move(description);
    metricInfo->labels = std::move(labels);
    metricInfo->metric = gauge;
    _metrics.push_back(std::move(metricInfo));
    return gauge;
}

std::shared_ptr<Histogram> MetricManager::registerHistogram(std::string name,
                                                            std::string description,
                                                            LabelsVec labels,
                                                            std::vector<int64_t> buckets) {
    stdx::lock_guard<Latch> lock(_mutex);
    auto histogram = std::make_shared<Histogram>(std::move(buckets));
    auto metricInfo = std::make_shared<MetricInfo>();
    metricInfo->name = std::move(name);
    metricInfo->description = std::move(description);
    metricInfo->labels = std::move(labels);
    metricInfo->metric = histogram;
    _metrics.push_back(std::move(metricInfo));

    return histogram;
}

void MetricManager::takeSnapshot() {
    for (auto& metricInfo : computeMetricsToVisit()) {
        auto metric = metricInfo->metric.lock();
        if (!metric) {
            continue;
        }
        metric->takeSnapshot();
    }
}

std::vector<std::shared_ptr<MetricManager::MetricInfo>> MetricManager::computeMetricsToVisit() {
    mongo::stdx::lock_guard<mongo::Latch> lock(_mutex);
    std::vector<std::shared_ptr<MetricInfo>> metricsToVisit;
    metricsToVisit.reserve(_metrics.size());
    auto it = _metrics.begin();
    while (it != _metrics.end()) {
        auto& metricInfo = *it;
        auto metric = metricInfo->metric.lock();
        if (metric) {
            metricsToVisit.push_back(*it);
            ++it;
        } else {
            it = _metrics.erase(it);
        }
    }
    return metricsToVisit;
}

}  // namespace streams
