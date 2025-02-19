/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/external_function_operator.h"

#include <aws/lambda/model/InvokeRequest.h>
#include <bsoncxx/exception/error_code.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <cstdint>
#include <deque>
#include <fmt/core.h>
#include <fmt/format.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/assert.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/tick_source_mock.h"
#include "mongo/util/timer.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/planner.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/tests/test_utils.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

class ExternalFunctionOperatorTest : public AggregationContextFixture {
public:
    ExternalFunctionOperatorTest() : AggregationContextFixture() {
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
    }

    void setupDag(ExternalFunctionOperator::Options opts) {
        _oper = std::make_unique<ExternalFunctionOperator>(_context.get(), std::move(opts));
        _oper->registerMetrics(_executor->getMetricManager());
        _sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);

        // Build DAG.
        _oper->addOutput(_sink.get(), 0);
        _oper->start();
        _sink->start();
    }

    void testAgainstDocs(std::vector<StreamDocument> inputDocs,
                         std::function<void(const std::deque<StreamMsgUnion>&)> msgsAssert,
                         std::function<void(OperatorStats)> statsAssert = nullptr) {
        // Send data message to operator and let process + flow to sink.
        _oper->onDataMsg(0, StreamDataMsg{.docs = inputDocs, .creationTimer = mongo::Timer{}});
        auto messages = _sink->getMessages();
        msgsAssert(messages);
        if (statsAssert != nullptr) {
            statsAssert(_oper->getStats());
        }
    }

    void stopDag() {
        _oper->stop();
        _sink->stop();
    }

    void updateRateLimitPerSecond(int64_t rate) {
        auto featureFlags = _context->featureFlags->testOnlyGetFeatureFlags();
        featureFlags[FeatureFlags::kExternalFunctionRateLimitPerSecond.name] =
            mongo::Value::createIntOrLong(rate);
        _context->featureFlags->updateFeatureFlags(
            StreamProcessorFeatureFlags{featureFlags,
                                        std::chrono::time_point<std::chrono::system_clock>{
                                            std::chrono::system_clock::now().time_since_epoch()}});
    }

    void assertStreamMetaExternalFunction(
        StreamMetaExternalFunction expectedStreamMetaExternalFunction,
        BSONObj actualStreamMetaExternalFunctionBson) {
        auto functionName = actualStreamMetaExternalFunctionBson["functionName"];
        ASSERT_TRUE(functionName.ok());
        ASSERT_STRING_CONTAINS(functionName.String(),
                               expectedStreamMetaExternalFunction.getFunctionName());

        auto executedVersion = actualStreamMetaExternalFunctionBson["executedVersion"];
        ASSERT_TRUE(executedVersion.ok());
        ASSERT_EQ(executedVersion.String(),
                  expectedStreamMetaExternalFunction.getExecutedVersion());

        auto statusCode = actualStreamMetaExternalFunctionBson["statusCode"];
        ASSERT_TRUE(statusCode.ok());
        ASSERT_EQ(statusCode.Int(), expectedStreamMetaExternalFunction.getStatusCode());

        auto responseTimeMs = actualStreamMetaExternalFunctionBson["responseTimeMs"];
        ASSERT_TRUE(responseTimeMs.ok());
        auto responseTimeMsDiff =
            expectedStreamMetaExternalFunction.getResponseTimeMs() - responseTimeMs.Int();
        if (responseTimeMsDiff < 0) {
            responseTimeMsDiff *= -1;
        }
        ASSERT_LESS_THAN_OR_EQUALS(responseTimeMsDiff, 50);
    }

    void tryLog(int id, std::function<void(int logID)> logFn) {
        _oper->tryLog(id, logFn);
    }

protected:
    std::unique_ptr<Context> _context;
    std::unique_ptr<Executor> _executor;

    std::unique_ptr<ExternalFunctionOperator> _oper;
    std::unique_ptr<InMemorySinkOperator> _sink;
};

int computeNumDlqBytes(std::queue<mongo::BSONObj> dlqMsgs) {
    int numBytes{};
    for (auto dlqMsgsCopy = std::move(dlqMsgs); !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
        numBytes += dlqMsgsCopy.front().objsize();
    }

    return numBytes;
}

class MockLambdaClient : public LambdaClient {
public:
    struct ComparableInvokeRequest {
        std::string functionName;
        Aws::Lambda::Model::InvocationType invocationType;
        std::string payloadBody;

        template <typename H>
        friend H AbslHashValue(H h, const ComparableInvokeRequest& cpr) {
            return H::combine(std::move(h), cpr.functionName, cpr.invocationType, cpr.payloadBody);
        }
        friend bool operator==(const ComparableInvokeRequest& my,
                               const ComparableInvokeRequest& other) {
            return my.functionName == other.functionName &&
                my.invocationType == other.invocationType && my.payloadBody == other.payloadBody;
        }
    };

    MockLambdaClient(){};

    Aws::Lambda::Model::InvokeOutcome Invoke(
        const Aws::Lambda::Model::InvokeRequest& request) const override {
        std::stringstream tempStream;
        tempStream << request.GetBody()->rdbuf();
        auto body = tempStream.str();
        auto it = _expectations.find(
            ComparableInvokeRequest{.functionName = request.GetFunctionName(),
                                    .invocationType = request.GetInvocationType(),
                                    .payloadBody = body});
        uassert(ErrorCodes::OperationFailed,
                fmt::format("Unexpected request submitted to mock lambda client {} {} {}",
                            request.GetFunctionName(),
                            request.GetInvocationType(),
                            body),
                it != _expectations.end());
        auto outcome = std::move(it->second);
        _expectations.erase(it);

        return outcome;
    }

    void expect(Aws::Lambda::Model::InvokeRequest& request,
                Aws::Lambda::Model::InvokeOutcome outcome) {
        std::stringstream tempStream;
        tempStream << request.GetBody()->rdbuf();
        auto body = tempStream.str();
        _expectations.emplace(ComparableInvokeRequest{.functionName = request.GetFunctionName(),
                                                      .invocationType = request.GetInvocationType(),
                                                      .payloadBody = body},
                              std::move(outcome));
    }

private:
    mutable stdx::unordered_map<ComparableInvokeRequest, Aws::Lambda::Model::InvokeOutcome>
        _expectations;
};

struct ExternalFunctionOpereratorTestCase {
    const std::string description;
    const std::function<ExternalFunctionOperator::Options()> optionsFn;
    const std::vector<StreamDocument> inputDocs;
    const std::function<void(const std::deque<StreamMsgUnion>&)> msgsAssertFn;
    const std::function<void(OperatorStats)> statsAssertFn;
};

TEST_F(ExternalFunctionOperatorTest, ExternalFunctionOperatorTestCases) {
    ExternalFunctionOpereratorTestCase tests[] = {
        {
            "should make a sync invoke request",
            [&] {
                auto mockClient = std::make_unique<MockLambdaClient>();

                Aws::Lambda::Model::InvokeRequest req;
                req.SetFunctionName("foo");
                req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

                std::shared_ptr<Aws::IOStream> body =
                    std::make_shared<Aws::StringStream>(R"({"data":"foo"})");
                req.SetBody(body);

                Aws::Lambda::Model::InvokeResult result;
                result.SetStatusCode(200);
                result.SetExecutedVersion("foo123");
                auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
                *responseBody << R"({"ack":"ok"})";
                result.ReplaceBody(responseBody);
                mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));

                return ExternalFunctionOperator::Options{
                    .lambdaClient = std::move(mockClient),
                    .functionName = "foo",
                    .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                    .as = boost::optional<std::string>("response"),
                };
            },
            std::vector<StreamDocument>{
                Document{fromjson(R"({"data":"foo"})")},
            },
            [this](std::deque<StreamMsgUnion> messages) {
                ASSERT_EQ(messages.size(), 1);
                auto msg = messages.at(0);

                ASSERT(msg.dataMsg);
                ASSERT(!msg.controlMsg);
                ASSERT_EQ(msg.dataMsg->docs.size(), 1);

                for (const auto& streamDoc : msg.dataMsg->docs) {
                    auto docBSON = streamDoc.doc.toBson();
                    auto response = docBSON["response"].Obj();
                    ASSERT_TRUE(!response.isEmpty());
                    auto ack = response["ack"];
                    ASSERT_TRUE(ack.ok());
                    ASSERT_EQ(ack.String(), "ok");

                    StreamMetaExternalFunction expectedStreamMetaExternalFunction;
                    expectedStreamMetaExternalFunction.setFunctionName("foo");
                    expectedStreamMetaExternalFunction.setExecutedVersion("foo123");
                    expectedStreamMetaExternalFunction.setStatusCode(200);
                    assertStreamMetaExternalFunction(
                        expectedStreamMetaExternalFunction,
                        docBSON[*_context->streamMetaFieldName].Obj()["externalFunction"].Obj());
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 12);
                ASSERT_EQ(stats.numOutputBytes, 14);
                ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
            },
        },
        {
            "should make an async invoke request",
            [&] {
                auto mockClient = std::make_unique<MockLambdaClient>();

                Aws::Lambda::Model::InvokeRequest req;
                req.SetFunctionName("foo");
                req.SetInvocationType(Aws::Lambda::Model::InvocationType::Event);

                std::shared_ptr<Aws::IOStream> body =
                    std::make_shared<Aws::StringStream>(R"({"data":"foo"})");
                req.SetBody(body);

                Aws::Lambda::Model::InvokeResult result;
                result.SetStatusCode(202);
                result.SetExecutedVersion("foo123");
                auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
                *responseBody << R"({"ack":"ok"})";
                result.ReplaceBody(responseBody);
                mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));

                return ExternalFunctionOperator::Options{
                    .lambdaClient = std::move(mockClient),
                    .functionName = "foo",
                    .execution = mongo::ExternalFunctionExecutionEnum::Async,
                };
            },
            std::vector<StreamDocument>{
                Document{fromjson(R"({"data":"foo"})")},
            },
            [this](std::deque<StreamMsgUnion> messages) {
                ASSERT_EQ(messages.size(), 1);
                auto msg = messages.at(0);

                ASSERT(msg.dataMsg);
                ASSERT(!msg.controlMsg);
                ASSERT_EQ(msg.dataMsg->docs.size(), 1);

                for (const auto& streamDoc : msg.dataMsg->docs) {
                    auto docBSON = streamDoc.doc.toBson();
                    auto response = docBSON["response"];
                    ASSERT_FALSE(response.ok());

                    StreamMetaExternalFunction expectedStreamMetaExternalFunction;
                    expectedStreamMetaExternalFunction.setFunctionName("foo");
                    expectedStreamMetaExternalFunction.setExecutedVersion("foo123");
                    expectedStreamMetaExternalFunction.setStatusCode(202);
                    assertStreamMetaExternalFunction(
                        expectedStreamMetaExternalFunction,
                        docBSON[*_context->streamMetaFieldName].Obj()["externalFunction"].Obj());
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 0);
                ASSERT_EQ(stats.numOutputBytes, 14);
                ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
            },
        },
        {
            "should make a sync invoke request with payload pipeline",
            [&] {
                auto rawPipeline = std::vector<mongo::BSONObj>{
                    fromjson(R"({ $replaceRoot: { newRoot: "$fullDocument.payload" }})"),
                    fromjson(R"({ $project: { include: 1 }})")};
                auto pipeline = Pipeline::parse(rawPipeline, _context->expCtx);
                pipeline->optimizePipeline();

                auto mockClient = std::make_unique<MockLambdaClient>();

                auto functionName = "foo";
                auto responsePayload = R"({"ack":"ok"})";
                auto statusCode = 200;

                Aws::Lambda::Model::InvokeRequest req;
                req.SetFunctionName(functionName);
                req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

                auto expectedDoc = R"({"include":"feefie"})";
                std::shared_ptr<Aws::IOStream> body =
                    std::make_shared<Aws::StringStream>(expectedDoc);
                req.SetBody(body);

                Aws::Lambda::Model::InvokeResult result;
                result.SetStatusCode(statusCode);
                result.SetExecutedVersion("foo123");
                auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
                *responseBody << responsePayload;
                result.ReplaceBody(responseBody);
                mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));

                return ExternalFunctionOperator::Options{
                    .lambdaClient = std::move(mockClient),
                    .functionName = functionName,
                    .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                    .as = boost::optional<std::string>("response"),
                    .payloadPipeline = FeedablePipeline{std::move(pipeline)},
                };
            },
            std::vector<StreamDocument>{
                Document{fromjson(
                    "{'fullDocument':{ 'payload': {'include': 'feefie', 'exclude': 'fohfum'}}}")},
            },
            [](std::deque<StreamMsgUnion> messages) {
                ASSERT_EQ(messages.size(), 1);
                auto msg = messages.at(0);

                ASSERT(msg.dataMsg);
                ASSERT(!msg.controlMsg);
                ASSERT_EQ(msg.dataMsg->docs.size(), 1);

                for (const auto& streamDoc : msg.dataMsg->docs) {
                    auto doc = streamDoc.doc.toBson();
                    auto response = doc["response"].Obj();
                    ASSERT_TRUE(!response.isEmpty());

                    auto ack = response["ack"];
                    ASSERT_TRUE(ack.ok());
                    ASSERT_EQ(ack.String(), "ok");
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 12);
                ASSERT_EQ(stats.numOutputBytes, 20);
                ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
            },
        },
        {
            "should handle being returned a text/plain response",
            [&] {
                auto mockClient = std::make_unique<MockLambdaClient>();

                Aws::Lambda::Model::InvokeRequest req;
                req.SetFunctionName("foo");
                req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

                std::shared_ptr<Aws::IOStream> body =
                    std::make_shared<Aws::StringStream>(R"({"data":"foo"})");
                req.SetBody(body);

                Aws::Lambda::Model::InvokeResult result;
                result.SetStatusCode(200);
                result.SetExecutedVersion("foo123");
                auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
                *responseBody << "feefie-fohfum";
                result.ReplaceBody(responseBody);
                mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));

                return ExternalFunctionOperator::Options{
                    .lambdaClient = std::move(mockClient),
                    .functionName = "foo",
                    .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                    .as = boost::optional<std::string>("response"),
                };
            },
            std::vector<StreamDocument>{
                Document{fromjson(R"({"data":"foo"})")},
            },
            [this](std::deque<StreamMsgUnion> messages) {
                ASSERT_EQ(messages.size(), 1);
                auto msg = messages.at(0);

                ASSERT(msg.dataMsg);
                ASSERT(!msg.controlMsg);
                ASSERT_EQ(msg.dataMsg->docs.size(), 1);

                for (const auto& streamDoc : msg.dataMsg->docs) {
                    auto docBSON = streamDoc.doc.toBson();
                    auto response = docBSON["response"];
                    ASSERT_EQ(response.String(), "feefie-fohfum");

                    StreamMetaExternalFunction expectedStreamMetaExternalFunction;
                    expectedStreamMetaExternalFunction.setFunctionName("foo");
                    expectedStreamMetaExternalFunction.setExecutedVersion("foo123");
                    expectedStreamMetaExternalFunction.setStatusCode(200);
                    assertStreamMetaExternalFunction(
                        expectedStreamMetaExternalFunction,
                        docBSON[*_context->streamMetaFieldName].Obj()["externalFunction"].Obj());
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 13);
                ASSERT_EQ(stats.numOutputBytes, 14);
                ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
            },
        },
    };

    for (const auto& tc : tests) {
        LOGV2_DEBUG(9929409, 1, "Running test case", "description"_attr = tc.description);
        setupDag(tc.optionsFn());
        testAgainstDocs(tc.inputDocs, tc.msgsAssertFn, tc.statsAssertFn);
        stopDag();
    }
}

TEST_F(ExternalFunctionOperatorTest, IsThrottledWithDefaultThrottleFnAndTimer) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto functionName = "foo";

    const auto docsToInsert = 5;
    const auto minTestDuration = Seconds(4);
    std::vector<StreamDocument> docs;
    docs.reserve(docsToInsert);

    for (int i = 0; i < docsToInsert; i++) {
        Aws::Lambda::Model::InvokeRequest req;
        req.SetFunctionName(functionName);
        req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

        auto expectedDoc = fmt::format(R"({{"data":{}}})", i);
        std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(expectedDoc);
        req.SetBody(body);

        Aws::Lambda::Model::InvokeResult result;
        result.SetStatusCode(200);
        result.SetExecutedVersion("foo123");
        auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
        *responseBody << R"({"ack":"ok"})";
        result.ReplaceBody(responseBody);

        mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));
        docs.emplace_back(StreamDocument{Document{fromjson(fmt::format(R"({{"data":{}}})", i))}});
    }

    updateRateLimitPerSecond(1);

    ExternalFunctionOperator::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
    };

    setupDag(std::move(options));

    Timer timer{};
    auto start = timer.elapsed();

    testAgainstDocs(
        docs,
        [&](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 1);
            auto msg = messages.at(0);

            ASSERT(msg.dataMsg);
            ASSERT(!msg.controlMsg);
            ASSERT_EQ(msg.dataMsg->docs.size(), docsToInsert);
        },
        [](OperatorStats stats) { ASSERT_GREATER_THAN(stats.timeSpent.count(), 4); });

    auto elapsedTime = timer.elapsed() - start;
    ASSERT_GREATER_THAN_OR_EQUALS(elapsedTime, minTestDuration);
    TestMetricsVisitor metrics;
    _executor->getMetricManager()->visitAllMetrics(&metrics);
    const auto& operatorCounters = metrics.counters().find(_context->streamProcessorId);
    long metricThrottleDuration = -1;
    if (operatorCounters != metrics.counters().end()) {
        auto it = operatorCounters->second.find(
            std::string{"external_function_operator_throttle_duration_micros"});
        if (it != operatorCounters->second.end()) {
            metricThrottleDuration = it->second->value();
        }
    }
    ASSERT_GREATER_THAN_OR_EQUALS(Microseconds(metricThrottleDuration),
                                  minTestDuration -
                                      Milliseconds(5));  // metric is a hair flakey based on host
}

TEST_F(ExternalFunctionOperatorTest, IsThrottledWithOverridenThrottleFnAndTimer) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto rawMockClient = mockClient.get();

    auto functionName = "foo";
    auto executedVersion = "foo123";
    auto payload = R"({"data":"foo_bar"})";
    auto statusCode = 200;

    TickSourceMock<Microseconds> tickSource{};
    Timer timer{&tickSource};

    ExternalFunctionOperator::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),

        .throttleFn =
            [&tickSource](Microseconds throttleDelay) { tickSource.advance(throttleDelay); },
        .timer = timer,
    };

    auto addMockClientExpectation =
        [rawMockClient, functionName, executedVersion, payload, statusCode]() {
            Aws::Lambda::Model::InvokeRequest req;
            req.SetFunctionName(functionName);
            req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

            std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(payload);
            req.SetBody(body);

            Aws::Lambda::Model::InvokeResult result;
            result.SetStatusCode(statusCode);
            result.SetExecutedVersion(executedVersion);
            auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
            *responseBody << R"({"ack":"ok"})";
            result.ReplaceBody(responseBody);

            rawMockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));
        };

    auto testAssert =
        [this, functionName, executedVersion, statusCode](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 1);
            auto msg = messages.at(0);

            ASSERT(msg.dataMsg);
            ASSERT(!msg.controlMsg);
            ASSERT_EQ(msg.dataMsg->docs.size(), 1);

            auto streamDoc = msg.dataMsg->docs[0];
            auto doc = streamDoc.doc.toBson();
            auto response = doc["response"].Obj();
            ASSERT_TRUE(!response.isEmpty());

            auto ack = response["ack"];
            ASSERT_TRUE(ack.ok());
            ASSERT_EQ(ack.String(), "ok");

            StreamMetaExternalFunction expectedStreamMetaExternalFunction;
            expectedStreamMetaExternalFunction.setFunctionName(functionName);
            expectedStreamMetaExternalFunction.setExecutedVersion(executedVersion);
            expectedStreamMetaExternalFunction.setStatusCode(statusCode);
            assertStreamMetaExternalFunction(
                expectedStreamMetaExternalFunction,
                doc[*_context->streamMetaFieldName].Obj()["externalFunction"].Obj());
        };

    setupDag(std::move(options));

    auto now = timer.elapsed();

    updateRateLimitPerSecond(1);
    addMockClientExpectation();
    testAgainstDocs(std::vector<StreamDocument>{Document{fromjson(payload)}}, testAssert);
    ASSERT_EQ(timer.elapsed() - now, Seconds(0));

    addMockClientExpectation();
    testAgainstDocs(std::vector<StreamDocument>{Document{fromjson(payload)}}, testAssert);
    ASSERT_EQ(timer.elapsed() - now, Seconds(1));
}

TEST_F(ExternalFunctionOperatorTest, RateLimitingParametersUpdate) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto rawMockClient = mockClient.get();

    auto functionName = "foo";
    auto executedVersion = "foo123";
    auto payload = R"({"data":"foo_bar"})";
    auto statusCode = 200;

    TickSourceMock<Microseconds> tickSource{};
    Timer timer{&tickSource};

    ExternalFunctionOperator::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
        .throttleFn =
            [&tickSource](Microseconds throttleDelay) { tickSource.advance(throttleDelay); },
        .timer = timer,
    };

    auto addMockClientExpectation =
        [rawMockClient, functionName, executedVersion, payload, statusCode]() {
            Aws::Lambda::Model::InvokeRequest req;
            req.SetFunctionName(functionName);
            req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

            std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(payload);
            req.SetBody(body);

            Aws::Lambda::Model::InvokeResult result;
            result.SetStatusCode(statusCode);
            result.SetExecutedVersion(executedVersion);
            auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
            *responseBody << R"({"ack":"ok"})";
            result.ReplaceBody(responseBody);

            rawMockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));
        };

    auto testAssert =
        [this, functionName, executedVersion, statusCode](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 1);
            auto msg = messages.at(0);

            ASSERT(msg.dataMsg);
            ASSERT(!msg.controlMsg);
            ASSERT_EQ(msg.dataMsg->docs.size(), 1);

            auto streamDoc = msg.dataMsg->docs[0];
            auto doc = streamDoc.doc.toBson();
            auto response = doc["response"].Obj();
            ASSERT_TRUE(!response.isEmpty());

            auto ack = response["ack"];
            ASSERT_TRUE(ack.ok());
            ASSERT_EQ(ack.String(), "ok");

            StreamMetaExternalFunction expectedStreamMetaExternalFunction;
            expectedStreamMetaExternalFunction.setFunctionName(functionName);
            expectedStreamMetaExternalFunction.setExecutedVersion(executedVersion);
            expectedStreamMetaExternalFunction.setStatusCode(statusCode);
            assertStreamMetaExternalFunction(
                expectedStreamMetaExternalFunction,
                doc[*_context->streamMetaFieldName].Obj()["externalFunction"].Obj());
        };

    setupDag(std::move(options));

    for (int rateLimitPerSec : {1, 2, 4}) {
        auto now = timer.elapsed();

        updateRateLimitPerSecond(rateLimitPerSec);

        // Generate full burst
        for (int i = 0; i < rateLimitPerSec; i++) {
            addMockClientExpectation();
            testAgainstDocs(std::vector<StreamDocument>{Document{fromjson(payload)}}, testAssert);
        }

        // Assert that no throttling occured during the burst
        ASSERT_EQ(timer.elapsed() - now, Milliseconds(0));

        addMockClientExpectation();
        testAgainstDocs(std::vector<StreamDocument>{Document{fromjson(payload)}}, testAssert);

        // Assert that throttling occured after initial burst
        ASSERT_EQ(timer.elapsed() - now, Milliseconds(1000 / rateLimitPerSec));
    }
}

TEST_F(ExternalFunctionOperatorTest, DocumentTooLarge) {
    ExternalFunctionOperator::Options options{
        .lambdaClient = std::make_unique<MockLambdaClient>(),
        .functionName = "foo",
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
    };

    setupDag(std::move(options));

    const auto documentSize1MB = 1024 * 1024;
    MutableDocument mutDoc;
    for (int i = 0; i < 17; i++) {
        mutDoc.addField("value" + std::to_string(i), Value{std::string(documentSize1MB, 'a' + i)});
    }

    std::vector<StreamDocument> inputMsgs = {mutDoc.freeze()};
    std::queue<mongo::BSONObj> dlqMsgs;
    testAgainstDocs(
        std::move(inputMsgs),
        [this, &dlqMsgs](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 0);

            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
            ASSERT_EQ(dlq->numMessages(), 1);

            dlqMsgs = dlq->getMessages();
            for (auto dlqMsgsCopy{dlqMsgs}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
                auto dlqDoc = dlqMsgsCopy.front();
                ASSERT_TRUE(!dlqDoc.isEmpty());
                ASSERT_STRING_CONTAINS(
                    dlqDoc["errInfo"]["reason"].String(),
                    "Failed to process input document in $externalFunction with error: "
                    "BSONObjectTooLarge: BSONObj size: 17826025");
                ASSERT_STRING_CONTAINS(dlqDoc["errInfo"]["reason"].String(),
                                       " is invalid. Size must be between 0 "
                                       "and 16793600(16MB) First element: value0:");
                ASSERT_EQ("ExternalFunctionOperator", dlqDoc["operatorName"].String());
            }
        },
        [this, &dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 0);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 1);
        });
}

TEST_F(ExternalFunctionOperatorTest, ShouldDLQOnErrorWithFunctionErrorByDefault) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto functionName = "foo";
    auto payload = R"({"ack":"ok"})";
    auto statusCode = 200;

    const auto docsToInsert = 2;
    std::vector<StreamDocument> docs;
    docs.reserve(docsToInsert);

    for (int i = 0; i < docsToInsert; i++) {
        Aws::Lambda::Model::InvokeRequest req;
        req.SetFunctionName(functionName);
        req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

        auto expectedDoc = fmt::format(R"({{"data":{}}})", i);
        std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(expectedDoc);
        req.SetBody(body);

        Aws::Lambda::Model::InvokeResult result;
        result.SetStatusCode(statusCode);
        result.SetExecutedVersion("foo123");
        result.SetFunctionError("Unhandled");
        auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
        *responseBody << payload;
        result.ReplaceBody(responseBody);

        mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));
        docs.emplace_back(StreamDocument{Document{fromjson(fmt::format(R"({{"data":{}}})", i))}});
    }

    ExternalFunctionOperator::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
    };

    setupDag(std::move(options));
    std::queue<mongo::BSONObj> dlqMsgs;
    testAgainstDocs(
        std::move(docs),
        [this, &dlqMsgs, statusCode, payload](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 0);

            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
            ASSERT_EQ(dlq->numMessages(), 2);

            dlqMsgs = dlq->getMessages();
            for (auto dlqMsgsCopy{dlqMsgs}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
                auto msg = dlqMsgsCopy.front();
                ASSERT_EQ(
                    msg["errInfo"]["reason"].String(),
                    "Failed to process input document in $externalFunction with error: Request "
                    "failure in $externalFunction with error: Received error response from "
                    "external function. Payload: " +
                        std::string(payload));
            }
        },
        [this, &dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 20);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 2);
            ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
        });
}


TEST_F(ExternalFunctionOperatorTest, ShouldDLQOnErrorWithFunctionError) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto functionName = "foo";
    auto payload = R"({"ack":"ok"})";
    auto statusCode = 200;

    const auto docsToInsert = 2;
    std::vector<StreamDocument> docs;
    docs.reserve(docsToInsert);

    for (int i = 0; i < docsToInsert; i++) {
        Aws::Lambda::Model::InvokeRequest req;
        req.SetFunctionName(functionName);
        req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

        auto expectedDoc = fmt::format(R"({{"data":{}}})", i);
        std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(expectedDoc);
        req.SetBody(body);

        Aws::Lambda::Model::InvokeResult result;
        result.SetStatusCode(statusCode);
        result.SetExecutedVersion("foo123");
        result.SetFunctionError("Unhandled");
        auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
        *responseBody << payload;
        result.ReplaceBody(responseBody);

        mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));
        docs.emplace_back(StreamDocument{Document{fromjson(fmt::format(R"({{"data":{}}})", i))}});
    }

    ExternalFunctionOperator::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
        .onError = mongo::OnErrorEnum::DLQ,
    };

    setupDag(std::move(options));
    std::queue<mongo::BSONObj> dlqMsgs;
    testAgainstDocs(
        std::move(docs),
        [this, &dlqMsgs, statusCode, payload](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 0);

            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
            ASSERT_EQ(dlq->numMessages(), 2);

            dlqMsgs = dlq->getMessages();
            for (auto dlqMsgsCopy{dlqMsgs}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
                auto msg = dlqMsgsCopy.front();
                ASSERT_EQ(
                    msg["errInfo"]["reason"].String(),
                    "Failed to process input document in $externalFunction with error: Request "
                    "failure in $externalFunction with error: Received error response from "
                    "external function. Payload: " +
                        std::string(payload));
            }
        },
        [this, &dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 20);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 2);
            ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
        });
}

TEST_F(ExternalFunctionOperatorTest, ShouldDLQInnerPayloadDoc) {
    auto rawPipeline = std::vector<mongo::BSONObj>{
        fromjson(R"({ $replaceRoot: { newRoot: "$fullDocument.payload" }})"),
        fromjson(R"({ $project: { include: 1 }})")};
    auto pipeline = Pipeline::parse(rawPipeline, _context->expCtx);
    pipeline->optimizePipeline();

    auto mockClient = std::make_unique<MockLambdaClient>();

    auto functionName = "foo";
    auto responsePayload = R"({"ack":"ok"})";
    auto statusCode = 200;

    Aws::Lambda::Model::InvokeRequest req;
    req.SetFunctionName(functionName);
    req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

    auto expectedDoc = R"({"include":"feefie"})";
    std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(expectedDoc);
    req.SetBody(body);

    Aws::Lambda::Model::InvokeResult result;
    result.SetStatusCode(statusCode);
    result.SetExecutedVersion("foo123");
    result.SetFunctionError("Unhandled");
    auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
    *responseBody << responsePayload;
    result.ReplaceBody(responseBody);
    mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));

    ExternalFunctionOperator::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
        .payloadPipeline = FeedablePipeline{std::move(pipeline)},
    };

    setupDag(std::move(options));

    std::vector<StreamDocument> inputMsgs = {
        Document{fromjson("{'fullDocument':{ 'payload': {'include': 'feefie'}}}")}};
    std::queue<mongo::BSONObj> dlqMsgs;
    testAgainstDocs(
        std::move(inputMsgs),
        [this, &dlqMsgs](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 0);

            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
            ASSERT_EQ(dlq->numMessages(), 1);

            dlqMsgs = dlq->getMessages();
            for (auto dlqMsgsCopy{dlqMsgs}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
                auto dlqDoc = dlqMsgsCopy.front();
                ASSERT_TRUE(!dlqDoc.isEmpty());
                auto ack = dlqDoc["doc"];
                ASSERT_TRUE(ack.ok());
                ack = dlqDoc["doc"]["include"];
                ASSERT_TRUE(ack.ok());
                ASSERT_EQ(dlqDoc["doc"]["include"].String(), "feefie");
            }
        },
        [this, &dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 12);
            ASSERT_EQ(stats.numOutputBytes, 20);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 1);
        });
}

TEST_F(ExternalFunctionOperatorTest, ShouldIgnoreOnErrorResponse) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto functionName = "foo";
    auto executedVersion = "foo123";
    auto payload = R"({"ack":"ok"})";
    auto statusCode = 200;

    const auto docsToInsert = 2;
    std::vector<StreamDocument> docs;
    docs.reserve(docsToInsert);

    for (int i = 0; i < docsToInsert; i++) {
        Aws::Lambda::Model::InvokeRequest req;
        req.SetFunctionName(functionName);
        req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

        auto expectedDoc = fmt::format(R"({{"data":{}}})", i);
        std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(expectedDoc);
        req.SetBody(body);

        Aws::Lambda::Model::InvokeResult result;
        result.SetStatusCode(statusCode);
        result.SetExecutedVersion(executedVersion);
        result.SetFunctionError("Unhandled");
        auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
        *responseBody << payload;
        result.ReplaceBody(responseBody);

        mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));
        docs.emplace_back(StreamDocument{Document{fromjson(fmt::format(R"({{"data":{}}})", i))}});
    }

    ExternalFunctionOperator::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
        .onError = mongo::OnErrorEnum::Ignore,
    };

    setupDag(std::move(options));

    testAgainstDocs(
        std::move(docs),
        [this, functionName, executedVersion, statusCode](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 1);
            auto msg = messages.at(0);

            ASSERT(msg.dataMsg);
            ASSERT(!msg.controlMsg);
            ASSERT_EQ(msg.dataMsg->docs.size(), 2);

            for (const auto& streamDoc : msg.dataMsg->docs) {
                auto docBSON = streamDoc.doc.toBson();
                auto response = docBSON["response"].Obj();
                ASSERT_TRUE(!response.isEmpty());
                auto ack = response["ack"];
                ASSERT_TRUE(ack.ok());
                ASSERT_EQ(ack.String(), "ok");

                StreamMetaExternalFunction expectedStreamMetaExternalFunction;
                expectedStreamMetaExternalFunction.setFunctionName(functionName);
                expectedStreamMetaExternalFunction.setExecutedVersion(executedVersion);
                expectedStreamMetaExternalFunction.setStatusCode(statusCode);
                assertStreamMetaExternalFunction(
                    expectedStreamMetaExternalFunction,
                    docBSON[*_context->streamMetaFieldName].Obj()["externalFunction"].Obj());
            }
        },
        [](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 20);
            ASSERT_EQ(stats.numDlqBytes, 0);
            ASSERT_EQ(stats.numDlqDocs, 0);
            ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
        });
}

TEST_F(ExternalFunctionOperatorTest, ShouldFailOnFailureStatusCodes) {
    auto codes = std::vector<Aws::Http::HttpResponseCode>{
        Aws::Http::HttpResponseCode::MULTIPLE_CHOICES,
        Aws::Http::HttpResponseCode::UNAUTHORIZED,
        Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR,
    };

    for (const auto& code : codes) {
        LOGV2_DEBUG(9929410, 1, "Running test case", "code"_attr = code);
        auto mockClient = std::make_unique<MockLambdaClient>();

        auto functionName = "foo";

        Aws::Lambda::Model::InvokeRequest req;
        req.SetFunctionName(functionName);
        req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

        auto expectedDoc = R"({"foo":"bar"})";
        std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(expectedDoc);
        req.SetBody(body);

        Aws::Lambda::LambdaError error;
        error.SetResponseCode(code);
        error.SetMessage("shouldFail");
        error.SetRequestId("r1");
        mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(error)));

        ExternalFunctionOperator::Options options{
            .lambdaClient = std::move(mockClient),
            .functionName = functionName,
            .execution = mongo::ExternalFunctionExecutionEnum::Sync,
            .as = boost::optional<std::string>("response"),
        };

        setupDag(std::move(options));

        ASSERT_THROWS_WHAT(
            testAgainstDocs(std::vector<StreamDocument>{Document{fromjson(expectedDoc)}},
                            [](auto _) {}),
            DBException,
            "Received error response from external function. Error: shouldFail");
    }
}

TEST_F(ExternalFunctionOperatorTest, ShouldFailOnFailureErrors) {
    for (Aws::Lambda::LambdaError error : {
             []() -> Aws::Lambda::LambdaError {
                 Aws::Lambda::LambdaError error = Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
                     Aws::Lambda::LambdaErrors::INVALID_REQUEST_CONTENT,
                     Aws::Client::RetryableType::NOT_RETRYABLE);
                 error.SetResponseCode(Aws::Http::HttpResponseCode::BAD_REQUEST);
                 return error;
             }(),
             []() -> Aws::Lambda::LambdaError {
                 Aws::Lambda::LambdaError error = Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
                     Aws::Lambda::LambdaErrors::THROTTLING,
                     Aws::Client::RetryableType::NOT_RETRYABLE);
                 error.SetResponseCode(Aws::Http::HttpResponseCode::BAD_REQUEST);
                 return error;
             }(),
             []() -> Aws::Lambda::LambdaError {
                 Aws::Lambda::LambdaError error = Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
                     Aws::Lambda::LambdaErrors::RESOURCE_NOT_FOUND,
                     Aws::Client::RetryableType::NOT_RETRYABLE);
                 error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
                 return error;
             }(),
             []() -> Aws::Lambda::LambdaError {
                 Aws::Lambda::LambdaError error = Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
                     Aws::Lambda::LambdaErrors::E_F_S_I_O,
                     Aws::Client::RetryableType::NOT_RETRYABLE);
                 error.SetResponseCode(Aws::Http::HttpResponseCode::GONE);
                 return error;
             }(),
         }) {

        auto mockClient = std::make_unique<MockLambdaClient>();

        auto functionName = "foo";
        auto payload = R"({"data":"foo_bar"})";

        Aws::Lambda::Model::InvokeRequest req;
        req.SetFunctionName(functionName);
        req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

        std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(payload);
        req.SetBody(body);

        error.SetMessage("shouldFail");
        error.SetRequestId("r1");
        mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(error)));

        setupDag(ExternalFunctionOperator::Options{
            .lambdaClient = std::move(mockClient),
            .functionName = functionName,
            .execution = mongo::ExternalFunctionExecutionEnum::Sync,
            .as = boost::optional<std::string>("response"),
        });


        ASSERT_THROWS_WHAT(testAgainstDocs(std::vector<StreamDocument>{Document{fromjson(payload)}},
                                           [](auto _) {}),
                           DBException,
                           "Received error response from external function. Error: shouldFail");
    }
}


TEST_F(ExternalFunctionOperatorTest, ShouldUseOnErrorBehaviorForStatusCodes) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto rawMockClient = mockClient.get();

    auto functionName = "foo";
    auto payload = R"({"data":"foo_bar"})";

    ExternalFunctionOperator::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
    };

    setupDag(std::move(options));
    for (Aws::Http::HttpResponseCode statusCode :
         {Aws::Http::HttpResponseCode::BAD_REQUEST,
          Aws::Http::HttpResponseCode::GONE,
          Aws::Http::HttpResponseCode::REQUEST_ENTITY_TOO_LARGE,
          Aws::Http::HttpResponseCode::REQUEST_URI_TOO_LONG,
          Aws::Http::HttpResponseCode::REQUEST_HEADER_FIELDS_TOO_LARGE}) {

        Aws::Lambda::Model::InvokeRequest req;
        req.SetFunctionName(functionName);
        req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

        std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(payload);
        req.SetBody(body);

        Aws::Lambda::LambdaError error = Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
            Aws::Lambda::LambdaErrors::UNKNOWN, Aws::Client::RetryableType::NOT_RETRYABLE);
        error.SetResponseCode(statusCode);
        error.SetMessage("shouldUseOnError");
        error.SetRequestId("r1");
        rawMockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(error)));

        testAgainstDocs(std::vector<StreamDocument>{Document{fromjson(payload)}},
                        [this](std::deque<StreamMsgUnion> messages) {
                            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
                            ASSERT_EQ(dlq->numMessages(), 1);

                            auto dlqMsgs = dlq->getMessages();
                            auto dlqDoc = std::move(dlqMsgs.front());
                            ASSERT_EQ(
                                dlqDoc["errInfo"]["reason"].String(),
                                "Failed to process input document in $externalFunction with "
                                "error: Request failure in $externalFunction with error: Received "
                                "error response from external function. Error: shouldUseOnError");
                        });
    }
}

TEST_F(ExternalFunctionOperatorTest, ShouldSupportEmptyPayload) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto functionName = "foo";
    auto responsePayload = "";
    auto executedVersion = "foo123";
    auto statusCode = 200;

    Aws::Lambda::Model::InvokeRequest req;
    req.SetFunctionName(functionName);
    req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

    auto expectedDoc = R"({"foo":"bar"})";
    std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(expectedDoc);
    req.SetBody(body);

    Aws::Lambda::Model::InvokeResult result;
    result.SetStatusCode(statusCode);
    result.SetExecutedVersion(executedVersion);
    auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
    *responseBody << responsePayload;
    result.ReplaceBody(responseBody);
    mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));

    ExternalFunctionOperator::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
    };

    setupDag(std::move(options));

    testAgainstDocs(
        std::vector<StreamDocument>{Document{fromjson(expectedDoc)}},
        [this, functionName, executedVersion, statusCode](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 1);
            auto msg = messages.at(0);

            ASSERT(msg.dataMsg);
            ASSERT(!msg.controlMsg);
            ASSERT_EQ(msg.dataMsg->docs.size(), 1);

            auto streamDoc = msg.dataMsg->docs[0];
            auto doc = streamDoc.doc.toBson();
            auto response = doc["response"];
            ASSERT_FALSE(response.ok());

            StreamMetaExternalFunction expectedStreamMetaExternalFunction;
            expectedStreamMetaExternalFunction.setFunctionName(functionName);
            expectedStreamMetaExternalFunction.setExecutedVersion(executedVersion);
            expectedStreamMetaExternalFunction.setStatusCode(statusCode);
            assertStreamMetaExternalFunction(
                expectedStreamMetaExternalFunction,
                doc[*_context->streamMetaFieldName].Obj()["externalFunction"].Obj());
        });
}

TEST_F(ExternalFunctionOperatorTest, ShouldNotLogSameIDWithinAMinute) {
    int count{0};

    TickSourceMock<Milliseconds> tickSource;
    _oper = std::make_unique<ExternalFunctionOperator>(_context.get(),
                                                       ExternalFunctionOperator::Options{
                                                           .functionName = "foo",
                                                           .timer = Timer{&tickSource},
                                                       });

    auto incCount = [&count](int _) -> void { count++; };

    // Should log
    tryLog(0, incCount);
    ASSERT_EQ(count, 1);

    // Should not log since no time has passed
    tryLog(0, incCount);
    ASSERT_EQ(count, 1);

    // Should not log since insufficient time has passed
    tickSource.advance(Seconds(59));
    tryLog(0, incCount);
    ASSERT_EQ(count, 1);

    // Should log since sufficient time has passed
    tickSource.advance(Seconds(1));
    tryLog(0, incCount);
    ASSERT_EQ(count, 2);

    // Should not log since no time has passed
    tryLog(0, incCount);
    ASSERT_EQ(count, 2);
}

TEST_F(ExternalFunctionOperatorTest, ShouldLogDifferentIDWithinAMinute) {
    int count{0};

    TickSourceMock<Milliseconds> tickSource;
    _oper = std::make_unique<ExternalFunctionOperator>(_context.get(),
                                                       ExternalFunctionOperator::Options{
                                                           .functionName = "foo",
                                                           .timer = Timer{&tickSource},
                                                       });

    auto incCount = [&count](int _) -> void { count++; };

    // Should log for any ID
    tryLog(0, incCount);
    ASSERT_EQ(count, 1);
    tryLog(1, incCount);
    ASSERT_EQ(count, 2);

    // Should only log for new IDs
    tickSource.advance(Seconds(59));
    tryLog(0, incCount);
    ASSERT_EQ(count, 2);
    tryLog(1, incCount);
    ASSERT_EQ(count, 2);
    tryLog(2, incCount);
    ASSERT_EQ(count, 3);

    // Should only log for IDs 0 and 1
    tickSource.advance(Seconds(59));
    tryLog(0, incCount);
    ASSERT_EQ(count, 4);
    tryLog(1, incCount);
    ASSERT_EQ(count, 5);
    tryLog(2, incCount);
    ASSERT_EQ(count, 5);

    // Should log for all IDs since sufficient time has passed for
    tickSource.advance(Minutes(2));
    tryLog(0, incCount);
    ASSERT_EQ(count, 6);
    tryLog(1, incCount);
    ASSERT_EQ(count, 7);
    tryLog(2, incCount);
    ASSERT_EQ(count, 8);
}
};  // namespace streams
