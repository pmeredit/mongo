/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/stdx/unordered_map.h"
#include "mongo/unittest/assert.h"
#include "mongo/util/time_support.h"
#include <boost/optional.hpp>
#include <tuple>

#include "streams/management/container_group_stats_provider.h"
#include "streams/management/container_stats.h"
#include "streams/util/metric_manager.h"

using namespace mongo;

namespace streams {

const int64_t kMemoryLimit = 10000000;
const int64_t kTimerValue = 10 * 1000 * 1000;  // 10 microseconds

class ContainerGroupMockProvider : public ContainerGroupStatsProvider {

public:
    ContainerGroupMockProvider(int cpuPercent, int memPercent) {
        _lastCpuValue = 1000 * 1000 * 1000;  // 1000 micro seconds
        _cpuPercent = cpuPercent;
        _memPercent = memPercent;
    }
    ~ContainerGroupMockProvider() override {}

    boost::optional<std::tuple<int64_t, int64_t>> getQuotaPeriod() override {
        return {{10000, 5000}};
    }
    boost::optional<int64_t> getCpuTimeInMicroseconds() override {
        _lastCpuValue = _lastCpuValue + kTimerValue * _cpuPercent * 2 / 100;
        return {_lastCpuValue};
    }
    boost::optional<int64_t> getMemoryLimitBytes() override {
        return {kMemoryLimit};
    }
    boost::optional<int64_t> getMemoryUsageBytes() override {
        return {kMemoryLimit * (_memPercent) / 100};
    }
    void resetTimer() override {}
    int64_t elapsedMicros() override {
        return kTimerValue;
    }


private:
    int _memPercent;
    int _cpuPercent;
    int64_t _lastCpuValue;
};

// A visitor class that can be used with MetricManager::visitAllMetrics().
class TestMetricsVisitor {
public:
    const auto& counters() {
        return _counters;
    }

    const auto& gauges() {
        return _gauges;
    }

    const auto& intGauges() {
        return _intGauges;
    }

    const auto& callbackGauges() {
        return _callbackGauges;
    }

    const auto& histograms() {
        return _histograms;
    }

    void visit(Counter* counter,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _counters[name] = counter;
    }

    void visit(Gauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _gauges[name] = gauge;
    }

    void visit(IntGauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _intGauges[name] = gauge;
    }

    void visit(CallbackGauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _callbackGauges[name] = gauge;
    }

    void visit(Histogram* histogram,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _histograms[name] = histogram;
    }

private:
    stdx::unordered_map<std::string, Counter*> _counters;
    stdx::unordered_map<std::string, Gauge*> _gauges;
    stdx::unordered_map<std::string, IntGauge*> _intGauges;
    stdx::unordered_map<std::string, CallbackGauge*> _callbackGauges;
    stdx::unordered_map<std::string, Histogram*> _histograms;
};

TEST(ContainerStatsTest, ProviderTest) {
    std::unique_ptr<ContainerGroupStatsProvider> provider =
        std::make_unique<ContainerGroupMockProvider>(10, 10);
    ContainerStats* stats = new ContainerStats(std::move(provider));
    mongo::sleepmillis(16000);
    TestMetricsVisitor visitor;
    stats->getMetricManager()->visitAllMetrics(&visitor);

    const auto& gauges = visitor.gauges();
    ASSERT_FALSE(gauges.empty());
    auto gauge = gauges.find("system_cpu_percent");
    ASSERT_TRUE(gauge != gauges.end());
    ASSERT_EQ(gauge->second->value(), 10.0);
    gauge = gauges.find("system_mem_percent");
    ASSERT_TRUE(gauge != gauges.end());
    ASSERT_EQ(gauge->second->value(), 10.0);
    auto intGauges = visitor.intGauges();
    auto intGauuge = intGauges.find("system_mem_limit");
    ASSERT_TRUE(intGauuge != intGauges.end());
    ASSERT_EQ(intGauuge->second->value(), kMemoryLimit);
}

}  // namespace streams
