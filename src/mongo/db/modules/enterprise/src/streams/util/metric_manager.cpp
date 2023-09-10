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
    MetricInfo metricInfo;
    metricInfo.name = std::move(name);
    metricInfo.description = std::move(description);
    metricInfo.labels = std::move(labels);
    metricInfo.metric = counter;
    _metrics.push_back(std::move(metricInfo));
    return counter;
}

std::shared_ptr<Gauge> MetricManager::registerGauge(std::string name,
                                                    std::string description,
                                                    LabelsVec labels) {
    stdx::lock_guard<Latch> lock(_mutex);
    auto gauge = std::make_shared<Gauge>();
    MetricInfo metricInfo;
    metricInfo.name = std::move(name);
    metricInfo.description = std::move(description);
    metricInfo.labels = std::move(labels);
    metricInfo.metric = gauge;
    _metrics.push_back(std::move(metricInfo));
    return gauge;
}

std::shared_ptr<CallbackGauge> MetricManager::registerCallbackGauge(std::string name,
                                                                    std::string description,
                                                                    LabelsVec labels,
                                                                    CallbackGauge::CallbackFn fn) {
    stdx::lock_guard<Latch> lock(_mutex);
    auto gauge = std::make_shared<CallbackGauge>(std::move(fn));
    MetricInfo metricInfo;
    metricInfo.name = std::move(name);
    metricInfo.description = std::move(description);
    metricInfo.labels = std::move(labels);
    metricInfo.metric = gauge;
    _metrics.push_back(std::move(metricInfo));
    return gauge;
}

}  // namespace streams
