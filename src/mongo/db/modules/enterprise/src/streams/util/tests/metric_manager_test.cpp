/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/stdx/unordered_map.h"
#include "mongo/unittest/unittest.h"
#include "streams/util/metric_manager.h"

namespace streams {
namespace {

using namespace mongo;

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

TEST(MetricManagerTest, Counter) {
    MetricManager manager;
    auto counter1 = manager.registerCounter("counter1", "description1", {{"tenant_id", "tenant1"}});
    auto counter2 = manager.registerCounter("counter2", "description2", {{"tenant_id", "tenant1"}});
    auto counter3 = manager.registerCounter("counter3", "description3", {{"tenant_id", "tenant2"}});

    counter1->increment(1);
    counter2->increment(2);
    counter3->increment(3);
    ASSERT_EQUALS(1, counter1->value());
    ASSERT_EQUALS(2, counter2->value());
    ASSERT_EQUALS(3, counter3->value());

    {
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& counters = visitor.counters();
        ASSERT_EQUALS(3, counters.size());
        ASSERT_EQUALS(counter1.get(), counters.at("counter1"));
        ASSERT_EQUALS(counter2.get(), counters.at("counter2"));
        ASSERT_EQUALS(counter3.get(), counters.at("counter3"));
    }
    {
        counter1.reset();
        counter2.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& counters = visitor.counters();
        ASSERT_EQUALS(1, counters.size());
        ASSERT_EQUALS(counter3.get(), counters.at("counter3"));
    }
    {
        counter3.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& counters = visitor.counters();
        ASSERT_TRUE(counters.empty());
    }
}

TEST(MetricManagerTest, Gauge) {
    MetricManager manager;
    auto gauge1 = manager.registerGauge("gauge1", "description1", {{"tenant_id", "tenant1"}});
    auto gauge2 = manager.registerGauge("gauge2", "description2", {{"tenant_id", "tenant1"}});
    auto gauge3 = manager.registerGauge("gauge3", "description3", {{"tenant_id", "tenant2"}});

    gauge1->set(1);
    gauge2->set(2);
    gauge3->set(3);
    ASSERT_EQUALS(1, gauge1->value());
    ASSERT_EQUALS(2, gauge2->value());
    ASSERT_EQUALS(3, gauge3->value());

    {
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.gauges();
        ASSERT_EQUALS(3, gauges.size());
        ASSERT_EQUALS(gauge1.get(), gauges.at("gauge1"));
        ASSERT_EQUALS(gauge2.get(), gauges.at("gauge2"));
        ASSERT_EQUALS(gauge3.get(), gauges.at("gauge3"));
        ASSERT_TRUE(visitor.callbackGauges().empty());
    }
    {
        gauge1.reset();
        gauge2.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.gauges();
        ASSERT_EQUALS(1, gauges.size());
        ASSERT_EQUALS(gauge3.get(), gauges.at("gauge3"));
    }
    {
        gauge3.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.gauges();
        ASSERT_TRUE(gauges.empty());
    }
}

TEST(MetricManagerTest, CallbackGauge) {
    MetricManager manager;
    auto gauge1 = manager.registerCallbackGauge(
        "gauge1", "description1", {{"tenant_id", "tenant1"}}, []() { return 1; });
    auto gauge2 = manager.registerCallbackGauge(
        "gauge2", "description2", {{"tenant_id", "tenant1"}}, []() { return 2; });
    auto gauge3 = manager.registerCallbackGauge(
        "gauge3", "description3", {{"tenant_id", "tenant2"}}, []() { return 3; });

    ASSERT_EQUALS(1, gauge1->value());
    ASSERT_EQUALS(2, gauge2->value());
    ASSERT_EQUALS(3, gauge3->value());

    {
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.callbackGauges();
        ASSERT_EQUALS(3, gauges.size());
        ASSERT_EQUALS(gauge1.get(), gauges.at("gauge1"));
        ASSERT_EQUALS(gauge2.get(), gauges.at("gauge2"));
        ASSERT_EQUALS(gauge3.get(), gauges.at("gauge3"));
        ASSERT_TRUE(visitor.gauges().empty());
    }
    {
        gauge1.reset();
        gauge2.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.callbackGauges();
        ASSERT_EQUALS(1, gauges.size());
        ASSERT_EQUALS(gauge3.get(), gauges.at("gauge3"));
    }
    {
        gauge3.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.callbackGauges();
        ASSERT_TRUE(gauges.empty());
    }
}

TEST(MetricManagerTest, Histogram) {
    MetricManager manager;

    // Linear buckets [5, 10, 15]
    auto histogram1 =
        manager.registerHistogram("histogram1",
                                  "description1",
                                  {{"tenant_id", "tenant1"}},
                                  makeLinearDurationBuckets(stdx::chrono::milliseconds(5), 5, 3));

    // Exponential buckets [5, 10, 20]
    auto histogram2 = manager.registerHistogram(
        "histogram2",
        "description2",
        {{"tenant_id", "tenant1"}},
        makeExponentialDurationBuckets(stdx::chrono::milliseconds(5), 2, 3));

    histogram1->increment(1);   // 5 bucket
    histogram1->increment(10);  // 10 bucket

    histogram2->increment(11);  // 20 bucket
    histogram2->increment(12);  // 20 bucket
    histogram2->increment(50);  // Infinity bucket

    histogram1->takeSnapshot();
    histogram2->takeSnapshot();

    auto assertBucketsEqual = [&](const auto& expect, const auto& actual) {
        ASSERT_EQUALS(expect.size(), actual.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            auto [bucket, count] = expect[i];
            ASSERT_EQUALS(bucket, actual[i].upper);
            ASSERT_EQUALS(count, actual[i].count);
        }
    };

    std::vector<std::pair<boost::optional<int64_t>, int64_t>> expectedHistogram1{
        {5, 1}, {10, 1}, {15, 0}, {boost::none, 0}};
    assertBucketsEqual(expectedHistogram1, histogram1->snapshotValue());

    std::vector<std::pair<boost::optional<int64_t>, int64_t>> expectedHistogram2{
        {5, 0}, {10, 0}, {20, 2}, {boost::none, 1}};
    assertBucketsEqual(expectedHistogram2, histogram2->snapshotValue());

    {
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& histograms = visitor.histograms();
        ASSERT_EQUALS(2, histograms.size());
        ASSERT_EQUALS(histogram1.get(), histograms.at("histogram1"));
        ASSERT_EQUALS(histogram2.get(), histograms.at("histogram2"));
    }
}

TEST(MetricManagerTest, DuplicateNamesAllowed) {
    MetricManager manager;
    auto counter1 = manager.registerCounter("counter1", "description1", {{"tenant_id", "tenant1"}});
    auto counter2 = manager.registerCounter("counter1", "description1", {{"tenant_id", "tenant2"}});

    counter1->increment(1);
    counter2->increment(2);
    ASSERT_EQUALS(1, counter1->value());
    ASSERT_EQUALS(2, counter2->value());
}

}  // namespace
}  // namespace streams
