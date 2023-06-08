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

    const auto& callbackGauges() {
        return _callbackGauges;
    }

    void visit(Counter* counter, const std::string& name, const MetricManager::LabelsVec& labels) {
        _counters[name] = counter;
    }

    void visit(Gauge* gauge, const std::string& name, const MetricManager::LabelsVec& labels) {
        _gauges[name] = gauge;
    }

    void visit(CallbackGauge* gauge,
               const std::string& name,
               const MetricManager::LabelsVec& labels) {
        _callbackGauges[name] = gauge;
    }

private:
    stdx::unordered_map<std::string, Counter*> _counters;
    stdx::unordered_map<std::string, Gauge*> _gauges;
    stdx::unordered_map<std::string, CallbackGauge*> _callbackGauges;
};

TEST(MetricManagerTest, Counter) {
    MetricManager manager;
    auto counter1 = manager.registerCounter("counter1", {{"tenant_id", "tenant1"}});
    auto counter2 = manager.registerCounter("counter2", {{"tenant_id", "tenant1"}});
    auto counter3 = manager.registerCounter("counter3", {{"tenant_id", "tenant2"}});

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
    auto gauge1 = manager.registerGauge("gauge1", {{"tenant_id", "tenant1"}});
    auto gauge2 = manager.registerGauge("gauge2", {{"tenant_id", "tenant1"}});
    auto gauge3 = manager.registerGauge("gauge3", {{"tenant_id", "tenant2"}});

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
    auto gauge1 =
        manager.registerCallbackGauge("gauge1", {{"tenant_id", "tenant1"}}, []() { return 1; });
    auto gauge2 =
        manager.registerCallbackGauge("gauge2", {{"tenant_id", "tenant1"}}, []() { return 2; });
    auto gauge3 =
        manager.registerCallbackGauge("gauge3", {{"tenant_id", "tenant2"}}, []() { return 3; });

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

TEST(MetricManagerTest, DuplicateNamesAllowed) {
    MetricManager manager;
    auto counter1 = manager.registerCounter("counter1", {{"tenant_id", "tenant1"}});
    auto counter2 = manager.registerCounter("counter1", {{"tenant_id", "tenant2"}});

    counter1->increment(1);
    counter2->increment(2);
    ASSERT_EQUALS(1, counter1->value());
    ASSERT_EQUALS(2, counter2->value());
}

}  // namespace
}  // namespace streams
