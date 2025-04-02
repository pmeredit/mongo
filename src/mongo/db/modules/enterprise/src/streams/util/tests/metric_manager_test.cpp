/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/util/metric_manager.h"

#include <memory>
#include <string>
#include <vector>

#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metrics.h"

namespace streams {
namespace {

using namespace mongo;

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

    auto assertCounter = [&](auto counters, Counter* counter) {
        ASSERT_TRUE(counters.contains(""));
        ASSERT_TRUE(counters.at("").contains(counter->getName()));
        ASSERT_TRUE(counters.at("").at(counter->getName()).at(""));
        ASSERT_EQUALS(counters.at("").at(counter->getName()).at(""), counter);
    };

    {
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& counters = visitor.counters();

        assertCounter(counters, counter1.get());
        assertCounter(counters, counter2.get());
        assertCounter(counters, counter3.get());

        ASSERT_EQUALS(counters.at("").size(), 3);
    }
    {
        counter1.reset();
        counter2.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& counters = visitor.counters();
        assertCounter(counters, counter3.get());

        ASSERT_EQUALS(1, counters.at("").size());
    }
    {
        counter3.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& counters = visitor.counters();
        ASSERT_TRUE(counters.empty());
    }
}

TEST(MetricManagerTest, CounterVec) {
    MetricManager manager;

    const std::string counterVec1Name{"counters1"};
    const std::string counterVec1Desc{"countervec1 description"};
    const std::vector<std::string> counterVec1ExtraLabelNames{"label1", "label2"};
    auto counterVec = manager.registerCounterVec(
        counterVec1Name, counterVec1Desc, {}, counterVec1ExtraLabelNames);

    auto assertCounter = [&](std::vector<std::string> labelValues, int expectedCount) {
        auto expectedCounter = counterVec->withLabels(std::move(labelValues));
        expectedCounter->increment();

        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& counters = visitor.counters();

        ASSERT_TRUE(counters.contains(""));
        ASSERT_TRUE(counters.at("").contains(expectedCounter->getName()));

        auto labels = expectedCounter->getLabels();
        auto labelStr = fmt::format("label1={},label2={},", labels[0].second, labels[1].second);
        ASSERT_TRUE(counters.at("").at(expectedCounter->getName()).contains(labelStr));

        auto counter = counters.at("").at(expectedCounter->getName()).at(labelStr);
        ASSERT_EQUALS(counter, expectedCounter.get());
    };

    assertCounter(std::vector<std::string>{"0", "0"}, 1);
    assertCounter(std::vector<std::string>{"0", "1"}, 1);
    assertCounter(std::vector<std::string>{"1", "0"}, 1);
    assertCounter(std::vector<std::string>{"1", "1"}, 1);

    {
        counterVec.reset();

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

    auto assertGauge = [&](auto gauges, GaugeBase<double>* gauge) {
        ASSERT_TRUE(gauges.contains(""));
        ASSERT_TRUE(gauges.at("").contains(gauge->getName()));
        ASSERT_TRUE(gauges.at("").at(gauge->getName()).at(""));
        ASSERT_EQUALS(gauges.at("").at(gauge->getName()).at(""), gauge);
    };

    {
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.gauges();

        assertGauge(gauges, gauge1.get());
        assertGauge(gauges, gauge2.get());
        assertGauge(gauges, gauge3.get());
        ASSERT_EQUALS(3, gauges.at("").size());
        ASSERT_TRUE(visitor.callbackGauges().empty());
    }
    {
        gauge1.reset();
        gauge2.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.gauges();
        assertGauge(gauges, gauge3.get());
        ASSERT_EQUALS(1, gauges.at("").size());
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

    auto assertGauge = [&](auto gauges, CallbackGauge* gauge) {
        ASSERT_TRUE(gauges.contains(""));
        ASSERT_TRUE(gauges.at("").contains(gauge->getName()));
        ASSERT_TRUE(gauges.at("").at(gauge->getName()).at(""));
        ASSERT_EQUALS(gauges.at("").at(gauge->getName()).at(""), gauge);
    };

    {
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.callbackGauges();
        assertGauge(gauges, gauge1.get());
        assertGauge(gauges, gauge2.get());
        assertGauge(gauges, gauge3.get());
        ASSERT_EQUALS(3, gauges.at("").size());
        ASSERT_TRUE(visitor.gauges().empty());
    }
    {
        gauge1.reset();
        gauge2.reset();
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& gauges = visitor.callbackGauges();
        assertGauge(gauges, gauge3.get());
        ASSERT_EQUALS(1, gauges.at("").size());
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

    auto assertHistogram = [&](auto histograms, Histogram* histogram) {
        ASSERT_TRUE(histograms.contains(""));
        ASSERT_TRUE(histograms.at("").contains(histogram->getName()));
        ASSERT_TRUE(histograms.at("").at(histogram->getName()).at(""));
        ASSERT_EQUALS(histograms.at("").at(histogram->getName()).at(""), histogram);
    };

    {
        TestMetricsVisitor visitor;
        manager.visitAllMetrics(&visitor);
        const auto& histograms = visitor.histograms();
        assertHistogram(histograms, histogram1.get());
        assertHistogram(histograms, histogram2.get());
        ASSERT_EQUALS(2, histograms.at("").size());
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
