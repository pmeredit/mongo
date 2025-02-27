/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/external_function.h"

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
#include "streams/exec/external_function_operator.h"
#include "streams/exec/external_function_sink_operator.h"
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

class ExternalFunctionTest : public AggregationContextFixture {
public:
    ExternalFunctionTest() : AggregationContextFixture() {
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
    }


    ~ExternalFunctionTest() override {
        if (_externalFunctionSinkOper) {
            _externalFunctionSinkOper->stop();
        }
    }

    // Middle Stage Functions
    void setupMiddleStageOper(ExternalFunction::Options opts) {
        _externalFunctionOper =
            std::make_unique<ExternalFunctionOperator>(_context.get(), std::move(opts));
        _externalFunctionOper->registerMetrics(_executor->getMetricManager());
        _sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);

        // Build DAG.
        _externalFunctionOper->addOutput(_sink.get(), 0);
        _externalFunctionOper->start();
        _sink->start();
    }

    void testMiddleStageOper(
        std::vector<StreamDocument> inputDocs,
        std::function<void(const std::deque<StreamMsgUnion>&)> msgsAssert = nullptr,
        std::function<void(OperatorStats)> statsAssert = nullptr) {
        // Send data message to operator and let process + flow to sink.
        _externalFunctionOper->onDataMsg(
            0, StreamDataMsg{.docs = inputDocs, .creationTimer = mongo::Timer{}});
        if (msgsAssert != nullptr) {
            auto messages = _sink->getMessages();
            msgsAssert(messages);
        }
        if (statsAssert != nullptr) {
            statsAssert(_externalFunctionOper->getStats());
        }
    }

    void stopMiddleStageOper() {
        _externalFunctionOper->stop();
        _sink->stop();
    }

    // Sink Stage Functions
    void setupSinkStageOper(ExternalFunction::Options opts) {
        _externalFunctionSinkOper =
            std::make_unique<ExternalFunctionSinkOperator>(_context.get(), std::move(opts));
        _externalFunctionSinkOper->registerMetrics(_executor->getMetricManager());
        _externalFunctionSinkOper->start();
        auto deadline = Date_t::now() + Seconds{10};
        // Wait for the source to be connected like the Executor does.
        while (_externalFunctionSinkOper->getConnectionStatus().isConnecting()) {
            stdx::this_thread::sleep_for(stdx::chrono::milliseconds(100));
            ASSERT(Date_t::now() < deadline);
        }
        ASSERT(_externalFunctionSinkOper->getConnectionStatus().isConnected());
    }

    void testSinkStageOper(std::vector<StreamDocument> inputDocs,
                           std::function<void(OperatorStats)> statsAssert = nullptr) {
        _externalFunctionSinkOper->onDataMsg(
            0, StreamDataMsg{.docs = inputDocs, .creationTimer = mongo::Timer{}});
        _externalFunctionSinkOper->flush();
        if (statsAssert != nullptr) {
            statsAssert(_externalFunctionSinkOper->getStats());
        }
    }

    void stopSinkStageOper() {
        _externalFunctionSinkOper->stop();
    }


    ConnectionStatus getConnectionStatus() {
        return _externalFunctionSinkOper->getConnectionStatus();
    }

    // Common Test Functions
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
        _externalFunction->tryLog(id, logFn);
    }

    // if called twice can clear out the dlq bytes.
    int computeNumDlqBytes() {
        auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
        _dlqMessages = dlq->getMessages();
        int numBytes{};
        for (auto dlqMsgsCopy{_dlqMessages}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
            numBytes += dlqMsgsCopy.front().objsize();
        }
        return numBytes;
    }

protected:
    std::unique_ptr<Context> _context;
    std::unique_ptr<Executor> _executor;

    std::queue<mongo::BSONObj> _dlqMessages;

    // Used for testing some common external function logic
    std::unique_ptr<ExternalFunction> _externalFunction;

    std::unique_ptr<ExternalFunctionOperator> _externalFunctionOper;
    std::unique_ptr<InMemorySinkOperator> _sink;

    std::unique_ptr<ExternalFunctionSinkOperator> _externalFunctionSinkOper;
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
        std::string body;
        if (request.GetBody()) {
            std::stringstream tempStream;
            tempStream << request.GetBody()->rdbuf();
            body = tempStream.str();
        }
        auto it = _expectations.find(
            ComparableInvokeRequest{.functionName = request.GetFunctionName(),
                                    .invocationType = request.GetInvocationType(),
                                    .payloadBody = body});
        uassert(ErrorCodes::OperationFailed,
                fmt::format("Unexpected request submitted to mock lambda client {} {} {}",
                            request.GetFunctionName(),
                            fmt::underlying(request.GetInvocationType()),
                            body),
                it != _expectations.end());
        auto outcome = std::move(it->second.first);
        sleepFor(Milliseconds(it->second.second));
        if (request.GetInvocationType() != Aws::Lambda::Model::InvocationType::DryRun) {
            _expectations.erase(it);
        }

        return outcome;
    }

    void expectValidation(std::string functionName) {
        Aws::Lambda::Model::InvokeRequest req;
        req.SetFunctionName(functionName);
        req.SetInvocationType(Aws::Lambda::Model::InvocationType::DryRun);

        std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>("");
        req.SetBody(body);

        Aws::Lambda::Model::InvokeResult result;
        result.SetStatusCode(200);
        result.SetExecutedVersion("foo123");
        auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
        *responseBody << R"({"ack":"ok"})";
        result.ReplaceBody(responseBody);
        expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));
    }

    void expect(Aws::Lambda::Model::InvokeRequest& request,
                Aws::Lambda::Model::InvokeOutcome outcome,
                int expectedDelay = 0) {
        std::string body;
        if (request.GetBody()) {
            std::stringstream tempStream;
            tempStream << request.GetBody()->rdbuf();
            body = tempStream.str();
        }
        _expectations.emplace(ComparableInvokeRequest{.functionName = request.GetFunctionName(),
                                                      .invocationType = request.GetInvocationType(),
                                                      .payloadBody = body},
                              std::make_pair<Aws::Lambda::Model::InvokeOutcome, int64_t>(
                                  std::move(outcome), expectedDelay));
    }

private:
    mutable stdx::unordered_map<ComparableInvokeRequest,
                                std::pair<Aws::Lambda::Model::InvokeOutcome, int64_t>>
        _expectations;
};

struct ExternalFunctionTestCase {
    const std::string description;
    const std::function<ExternalFunction::Options()> optionsFn;
    const std::function<std::vector<StreamDocument>()> inputDocsFn;
    const std::function<void(const std::deque<StreamMsgUnion>&)> msgsAssertFn;
    const std::function<void(OperatorStats)> statsAssertFn;
    const std::function<void(OperatorStats)> statsSinkAssertFn;
};

TEST_F(ExternalFunctionTest, ExternalFunctionTestCases) {
    ExternalFunctionTestCase tests[] = {
        {
            "should make a sync invoke request",
            [&] {
                auto mockClient = std::make_unique<MockLambdaClient>();
                mockClient->expectValidation("foo");

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

                return ExternalFunction::Options{
                    .lambdaClient = std::move(mockClient),
                    .functionName = "foo",
                    .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                    .as = boost::optional<std::string>("response"),
                };
            },
            [] {
                return std::vector<StreamDocument>{
                    Document{fromjson(R"({"data":"foo"})")},
                };
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
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 12);
                ASSERT_EQ(stats.numOutputBytes, 14);
                ASSERT_EQ(stats.numOutputDocs, 1);
                ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
            },
        },
        {
            "should make an async invoke request",
            [&] {
                auto mockClient = std::make_unique<MockLambdaClient>();
                mockClient->expectValidation("foo");

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

                return ExternalFunction::Options{
                    .lambdaClient = std::move(mockClient),
                    .functionName = "foo",
                    .execution = mongo::ExternalFunctionExecutionEnum::Async,
                };
            },
            [] {
                return std::vector<StreamDocument>{
                    Document{fromjson(R"({"data":"foo"})")},
                };
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
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 0);
                ASSERT_EQ(stats.numOutputBytes, 14);
                ASSERT_EQ(stats.numOutputDocs, 1);
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
                mockClient->expectValidation("foo");

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

                return ExternalFunction::Options{
                    .lambdaClient = std::move(mockClient),
                    .functionName = functionName,
                    .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                    .as = boost::optional<std::string>("response"),
                    .payloadPipeline = FeedablePipeline{std::move(pipeline)},
                };
            },
            [] {
                return std::vector<StreamDocument>{
                    Document{fromjson("{'fullDocument':{ 'payload': {'include': 'feefie', "
                                      "'exclude': 'fohfum'}}}")},
                };
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
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 12);
                ASSERT_EQ(stats.numOutputBytes, 20);
                ASSERT_EQ(stats.numOutputDocs, 1);
                ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
            },
        },
        {
            "should handle being returned a text/plain response",
            [&] {
                auto mockClient = std::make_unique<MockLambdaClient>();
                mockClient->expectValidation("foo");

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

                return ExternalFunction::Options{
                    .lambdaClient = std::move(mockClient),
                    .functionName = "foo",
                    .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                    .as = boost::optional<std::string>("response"),
                };
            },
            [] {
                return std::vector<StreamDocument>{
                    Document{fromjson(R"({"data":"foo"})")},
                };
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
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 13);
                ASSERT_EQ(stats.numOutputBytes, 14);
                ASSERT_EQ(stats.numOutputDocs, 1);
                ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
            },
        },
        {
            "should handle being returned an empty payload response",
            [&] {
                auto mockClient = std::make_unique<MockLambdaClient>();
                mockClient->expectValidation("foo");

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
                *responseBody << "";
                result.ReplaceBody(responseBody);
                mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)));

                return ExternalFunction::Options{
                    .lambdaClient = std::move(mockClient),
                    .functionName = "foo",
                    .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                    .as = boost::optional<std::string>("response"),
                };
            },
            [] {
                return std::vector<StreamDocument>{
                    Document{fromjson(R"({"data":"foo"})")},
                };
            },
            [this](std::deque<StreamMsgUnion> messages) {
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
                expectedStreamMetaExternalFunction.setFunctionName("foo");
                expectedStreamMetaExternalFunction.setExecutedVersion("foo123");
                expectedStreamMetaExternalFunction.setStatusCode(200);
                assertStreamMetaExternalFunction(
                    expectedStreamMetaExternalFunction,
                    doc[*_context->streamMetaFieldName].Obj()["externalFunction"].Obj());
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 0);
                ASSERT_EQ(stats.numOutputBytes, 14);
                ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 0);
                ASSERT_EQ(stats.numOutputBytes, 14);
                ASSERT_EQ(stats.numOutputDocs, 1);
                ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
            },
        },
    };

    for (const auto& tc : tests) {
        for (const std::string stage : {"middle", "sink"}) {
            LOGV2_DEBUG(9929650,
                        1,
                        "Running test case",
                        "stage"_attr = stage,
                        "description"_attr = tc.description);
            if (stage == "middle") {
                setupMiddleStageOper(tc.optionsFn());
                testMiddleStageOper(tc.inputDocsFn(), tc.msgsAssertFn, tc.statsAssertFn);
                stopMiddleStageOper();
            } else {
                auto options = tc.optionsFn();
                options.isSink = true;
                setupSinkStageOper(std::move(options));
                testSinkStageOper(tc.inputDocsFn(), tc.statsSinkAssertFn);
                stopSinkStageOper();
            }
        }
    }
}

TEST_F(ExternalFunctionTest, IsThrottledWithDefaultThrottleFnAndTimer) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto functionName = "foo";

    mockClient->expectValidation(functionName);

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

    ExternalFunction::Options options{
        .lambdaClient = std::move(mockClient),
        .functionName = functionName,
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
    };

    _externalFunction = std::make_unique<ExternalFunction>(
        _context.get(), std::move(options), _context->streamName, 1);
    _externalFunction->doRegisterMetrics(_executor->getMetricManager());

    Timer timer{};
    auto start = timer.elapsed();
    mongo::Microseconds timeSpent{0};
    for (auto& streamDoc : docs) {
        _externalFunction->processStreamDoc(&streamDoc);
        timeSpent += timer.elapsed();
    }
    ASSERT_GREATER_THAN(timeSpent.count(), 4);

    auto elapsedTime = timer.elapsed() - start;
    ASSERT_GREATER_THAN_OR_EQUALS(elapsedTime, minTestDuration);
    TestMetricsVisitor metrics;
    _executor->getMetricManager()->visitAllMetrics(&metrics);
    const auto& operatorCounters = metrics.counters().find(_context->streamProcessorId);
    ASSERT_NOT_EQUALS(operatorCounters, metrics.counters().end());

    auto operatorCountersByLabel = operatorCounters->second.find(
        std::string{"external_function_operator_throttle_duration_micros"});
    ASSERT_NOT_EQUALS(operatorCountersByLabel, operatorCounters->second.end());

    auto it = operatorCountersByLabel->second.find("");
    ASSERT_NOT_EQUALS(it, operatorCountersByLabel->second.end());

    auto metricThrottleDuration = it->second->value();
    ASSERT_GREATER_THAN_OR_EQUALS(
        Microseconds(metricThrottleDuration),
        minTestDuration - Milliseconds(20));  // metric is a hair flakey based on host and build
}

TEST_F(ExternalFunctionTest, IsThrottledWithOverridenThrottleFnAndTimer) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    auto rawMockClient = mockClient.get();
    auto functionName = "foo";
    auto executedVersion = "foo123";
    auto payload = R"({"data":"foo_bar"})";
    auto statusCode = 200;

    mockClient->expectValidation(functionName);

    TickSourceMock<Microseconds> tickSource{};
    Timer timer{&tickSource};

    ExternalFunction::Options options{
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

    _externalFunction = std::make_unique<ExternalFunction>(
        _context.get(), std::move(options), _context->streamName, 1);
    _externalFunction->doRegisterMetrics(_executor->getMetricManager());

    auto now = timer.elapsed();
    addMockClientExpectation();
    auto doc = StreamDocument{Document{fromjson(payload)}};
    _externalFunction->processStreamDoc(&doc);
    ASSERT_EQ(timer.elapsed() - now, Seconds(0));

    addMockClientExpectation();
    doc = StreamDocument{Document{fromjson(payload)}};
    _externalFunction->processStreamDoc(&doc);
    ASSERT_EQ(timer.elapsed() - now, Seconds(1));
}


struct ExternalFunctionStatsTestCase {
    const std::string description;
    const std::function<void(ExternalFunction::Options)> setupFn;
    const std::function<void(std::vector<StreamDocument>, std::function<void(OperatorStats)>)>
        testFn;
    const std::function<void()> stopFn;
    std::string operatorName;
};

TEST_F(ExternalFunctionTest, RateLimitingParametersUpdate) {
    for (int rateLimitPerSec : {1, 2, 4}) {
        ExternalFunctionStatsTestCase tests[] = {
            {
                "middle",
                [&](ExternalFunction::Options options) {
                    setupMiddleStageOper(std::move(options));
                },
                [&](std::vector<StreamDocument> inputDocs,
                    std::function<void(OperatorStats)> statsAssert) {
                    testMiddleStageOper(inputDocs, nullptr, statsAssert);
                },
                [&]() { stopMiddleStageOper(); },
                ExternalFunctionOperator::kName.toString(),
            },
            {
                "sink",
                [&](ExternalFunction::Options options) {
                    options.isSink = true;
                    setupSinkStageOper(std::move(options));
                },
                [&](std::vector<StreamDocument> inputDocs,
                    std::function<void(OperatorStats)> statsAssert) {
                    testSinkStageOper(inputDocs, statsAssert);
                },
                [&]() { stopSinkStageOper(); },
                ExternalFunctionSinkOperator::kName.toString(),
            },
        };
        for (const auto& tc : tests) {
            LOGV2_DEBUG(
                9929651, 1, "Running rate limiter update test case", "stage"_attr = tc.description);
            auto mockClient = std::make_unique<MockLambdaClient>();

            auto rawMockClient = mockClient.get();

            auto functionName = "foo";
            auto executedVersion = "foo123";
            auto payload = R"({"data":"foo_bar"})";
            auto statusCode = 200;

            mockClient->expectValidation(functionName);

            TickSourceMock<Microseconds> tickSource{};
            Timer timer{&tickSource};

            ExternalFunction::Options options{
                .lambdaClient = std::move(mockClient),
                .functionName = functionName,
                .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                .as = boost::optional<std::string>("response"),
                .throttleFn =
                    [&tickSource](Microseconds throttleDelay) {
                        tickSource.advance(throttleDelay);
                    },
                .timer = timer,
            };

            auto addMockClientExpectation =
                [rawMockClient, functionName, executedVersion, payload, statusCode]() {
                    Aws::Lambda::Model::InvokeRequest req;
                    req.SetFunctionName(functionName);
                    req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

                    std::shared_ptr<Aws::IOStream> body =
                        std::make_shared<Aws::StringStream>(payload);
                    req.SetBody(body);

                    Aws::Lambda::Model::InvokeResult result;
                    result.SetStatusCode(statusCode);
                    result.SetExecutedVersion(executedVersion);
                    auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
                    *responseBody << R"({"ack":"ok"})";
                    result.ReplaceBody(responseBody);

                    rawMockClient->expect(req,
                                          Aws::Lambda::Model::InvokeOutcome(std::move(result)));
                };

            tc.setupFn(std::move(options));

            auto now = timer.elapsed();

            updateRateLimitPerSecond(rateLimitPerSec);

            // Generate full burst
            for (int i = 0; i < rateLimitPerSec; i++) {
                addMockClientExpectation();
                tc.testFn(std::vector<StreamDocument>{Document{fromjson(payload)}},
                          [tc, i](OperatorStats stats) {
                              ASSERT_EQ(stats.numInputBytes, 12 * (i + 1));
                              ASSERT_EQ(stats.numOutputBytes, 18 * (i + 1));
                              if (tc.operatorName == ExternalFunctionSinkOperator::kName) {
                                  ASSERT_EQ(stats.numOutputDocs, 1 * (i + 1));
                              }
                              ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
                          });
            }

            // Assert that no throttling occured during the burst
            ASSERT_EQ(timer.elapsed() - now, Milliseconds(0));

            addMockClientExpectation();
            tc.testFn(std::vector<StreamDocument>{Document{fromjson(payload)}},
                      [tc, rateLimitPerSec](OperatorStats stats) {
                          ASSERT_EQ(stats.numInputBytes, 12 * (rateLimitPerSec + 1));
                          ASSERT_EQ(stats.numOutputBytes, 18 * (rateLimitPerSec + 1));
                          if (tc.operatorName == ExternalFunctionSinkOperator::kName) {
                              ASSERT_EQ(stats.numOutputDocs, 1 * (rateLimitPerSec + 1));
                          }
                          ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
                      });

            // Assert that throttling occured after initial burst
            ASSERT_EQ(timer.elapsed() - now, Milliseconds(1000 / rateLimitPerSec));

            tc.stopFn();
        }
    }
}

TEST_F(ExternalFunctionTest, DocumentTooLarge) {
    ExternalFunctionStatsTestCase tests[] = {
        {
            "middle",
            [&](ExternalFunction::Options options) { setupMiddleStageOper(std::move(options)); },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testMiddleStageOper(inputDocs, nullptr, statsAssert);
            },
            [&]() { stopMiddleStageOper(); },
            ExternalFunctionOperator::kName.toString(),
        },
        {
            "sink",
            [&](ExternalFunction::Options options) {
                options.isSink = true;
                setupSinkStageOper(std::move(options));
            },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testSinkStageOper(inputDocs, statsAssert);
            },
            [&]() { stopSinkStageOper(); },
            ExternalFunctionSinkOperator::kName.toString(),
        },
    };
    for (const auto& tc : tests) {
        LOGV2_DEBUG(
            9929652, 1, "Running document too large test case", "stage"_attr = tc.description);

        auto mockClient = std::make_unique<MockLambdaClient>();

        mockClient->expectValidation("foo");

        ExternalFunction::Options options{
            .lambdaClient = std::move(mockClient),
            .functionName = "foo",
            .execution = mongo::ExternalFunctionExecutionEnum::Sync,
            .as = boost::optional<std::string>("response"),
        };

        tc.setupFn(std::move(options));

        const auto documentSize1MB = 1024 * 1024;
        MutableDocument mutDoc;
        for (int i = 0; i < 17; i++) {
            mutDoc.addField("value" + std::to_string(i),
                            Value{std::string(documentSize1MB, 'a' + i)});
        }

        std::vector<StreamDocument> inputMsgs = {mutDoc.freeze()};
        tc.testFn(std::move(inputMsgs), [&](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 0);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes());
            ASSERT_EQ(stats.numDlqDocs, 1);
        });
        auto dlqDoc = _dlqMessages.front();
        ASSERT_TRUE(!dlqDoc.isEmpty());
        ASSERT_STRING_CONTAINS(dlqDoc["errInfo"]["reason"].String(),
                               "Failed to process input document in $externalFunction with error: "
                               "BSONObjectTooLarge: BSONObj size: 17826025");
        ASSERT_STRING_CONTAINS(dlqDoc["errInfo"]["reason"].String(),
                               " is invalid. Size must be between 0 "
                               "and 16793600(16MB) First element: value0:");
        ASSERT_EQ(tc.operatorName, dlqDoc["operatorName"].String());

        tc.stopFn();
    }
}

TEST_F(ExternalFunctionTest, ShouldDLQOnErrorWithFunctionErrorByDefault) {
    ExternalFunctionStatsTestCase tests[] = {
        {
            "middle",
            [&](ExternalFunction::Options options) { setupMiddleStageOper(std::move(options)); },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testMiddleStageOper(inputDocs, nullptr, statsAssert);
            },
            [&]() { stopMiddleStageOper(); },
            ExternalFunctionOperator::kName.toString(),
        },
        {
            "sink",
            [&](ExternalFunction::Options options) {
                options.isSink = true;
                setupSinkStageOper(std::move(options));
            },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testSinkStageOper(inputDocs, statsAssert);
            },
            [&]() { stopSinkStageOper(); },
            ExternalFunctionSinkOperator::kName.toString(),
        },
    };
    for (const auto& tc : tests) {
        LOGV2_DEBUG(9929653,
                    1,
                    "Running should dlq on function error test case",
                    "stage"_attr = tc.description);

        auto mockClient = std::make_unique<MockLambdaClient>();

        auto functionName = "foo";
        auto payload = R"({"ack":"ok"})";
        auto statusCode = 200;

        mockClient->expectValidation(functionName);

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
            docs.emplace_back(
                StreamDocument{Document{fromjson(fmt::format(R"({{"data":{}}})", i))}});
        }

        ExternalFunction::Options options{
            .lambdaClient = std::move(mockClient),
            .functionName = functionName,
            .execution = mongo::ExternalFunctionExecutionEnum::Sync,
            .as = boost::optional<std::string>("response"),
        };

        tc.setupFn(std::move(options));

        tc.testFn(std::move(docs), [&](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 20);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes());
            ASSERT_EQ(stats.numDlqDocs, 2);
            ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
        });

        for (auto dlqMsgsCopy{_dlqMessages}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
            auto msg = dlqMsgsCopy.front();
            ASSERT_EQ(msg["errInfo"]["reason"].String(),
                      "Failed to process input document in $externalFunction with error: Request "
                      "failure in $externalFunction with error: Received error response from "
                      "external function. Payload: " +
                          std::string(payload));
        }
    }
}


TEST_F(ExternalFunctionTest, ShouldDLQOnErrorWithFunctionError) {
    ExternalFunctionStatsTestCase tests[] = {
        {
            "middle",
            [&](ExternalFunction::Options options) { setupMiddleStageOper(std::move(options)); },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testMiddleStageOper(inputDocs, nullptr, statsAssert);
            },
            [&]() { stopMiddleStageOper(); },
            ExternalFunctionOperator::kName.toString(),
        },
        {
            "sink",
            [&](ExternalFunction::Options options) {
                options.isSink = true;
                setupSinkStageOper(std::move(options));
            },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testSinkStageOper(inputDocs, statsAssert);
            },
            [&]() { stopSinkStageOper(); },
            ExternalFunctionSinkOperator::kName.toString(),
        },
    };
    for (const auto& tc : tests) {
        LOGV2_DEBUG(9929654,
                    1,
                    "Running should dlq on function error test case",
                    "stage"_attr = tc.description);
        auto mockClient = std::make_unique<MockLambdaClient>();
        auto functionName = "foo";
        auto payload = R"({"ack":"ok"})";
        auto statusCode = 200;

        mockClient->expectValidation(functionName);

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
            docs.emplace_back(
                StreamDocument{Document{fromjson(fmt::format(R"({{"data":{}}})", i))}});
        }

        ExternalFunction::Options options{
            .lambdaClient = std::move(mockClient),
            .functionName = functionName,
            .execution = mongo::ExternalFunctionExecutionEnum::Sync,
            .as = boost::optional<std::string>("response"),
            .onError = mongo::OnErrorEnum::DLQ,
        };

        tc.setupFn(std::move(options));

        tc.testFn(std::move(docs), [&](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 20);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes());
            ASSERT_EQ(stats.numDlqDocs, 2);
            ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
        });
        for (auto dlqMsgsCopy{_dlqMessages}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
            auto msg = dlqMsgsCopy.front();
            ASSERT_EQ(msg["errInfo"]["reason"].String(),
                      "Failed to process input document in $externalFunction with error: Request "
                      "failure in $externalFunction with error: Received error response from "
                      "external function. Payload: " +
                          std::string(payload));
        }
    }
}

TEST_F(ExternalFunctionTest, ShouldDLQInnerPayloadDoc) {
    ExternalFunctionStatsTestCase tests[] = {
        {
            "middle",
            [&](ExternalFunction::Options options) { setupMiddleStageOper(std::move(options)); },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testMiddleStageOper(inputDocs, nullptr, statsAssert);
            },
            [&]() { stopMiddleStageOper(); },
            ExternalFunctionOperator::kName.toString(),
        },
        {
            "sink",
            [&](ExternalFunction::Options options) {
                options.isSink = true;
                setupSinkStageOper(std::move(options));
            },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testSinkStageOper(inputDocs, statsAssert);
            },
            [&]() { stopSinkStageOper(); },
            ExternalFunctionSinkOperator::kName.toString(),
        },
    };
    for (const auto& tc : tests) {
        LOGV2_DEBUG(9929655,
                    1,
                    "Running should dlq on function error test case",
                    "stage"_attr = tc.description);
        auto rawPipeline = std::vector<mongo::BSONObj>{
            fromjson(R"({ $replaceRoot: { newRoot: "$fullDocument.payload" }})"),
            fromjson(R"({ $project: { include: 1 }})")};
        auto pipeline = Pipeline::parse(rawPipeline, _context->expCtx);
        pipeline->optimizePipeline();

        auto mockClient = std::make_unique<MockLambdaClient>();

        auto functionName = "foo";
        auto responsePayload = R"({"ack":"ok"})";
        auto statusCode = 200;

        mockClient->expectValidation(functionName);

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

        ExternalFunction::Options options{
            .lambdaClient = std::move(mockClient),
            .functionName = functionName,
            .execution = mongo::ExternalFunctionExecutionEnum::Sync,
            .as = boost::optional<std::string>("response"),
            .payloadPipeline = FeedablePipeline{std::move(pipeline)},
        };

        tc.setupFn(std::move(options));

        std::vector<StreamDocument> inputMsgs = {
            Document{fromjson("{'fullDocument':{ 'payload': {'include': 'feefie'}}}")}};
        std::queue<mongo::BSONObj> dlqMsgs;
        tc.testFn(std::move(inputMsgs), [this, &dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 12);
            ASSERT_EQ(stats.numOutputBytes, 20);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes());
            ASSERT_EQ(stats.numDlqDocs, 1);
        });
        for (auto dlqMsgsCopy{_dlqMessages}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
            auto dlqDoc = dlqMsgsCopy.front();
            ASSERT_TRUE(!dlqDoc.isEmpty());
            auto ack = dlqDoc["doc"];
            ASSERT_TRUE(ack.ok());
            ack = dlqDoc["doc"]["include"];
            ASSERT_TRUE(ack.ok());
            ASSERT_EQ(dlqDoc["doc"]["include"].String(), "feefie");
        }
    }
}

TEST_F(ExternalFunctionTest, ShouldIgnoreOnErrorResponse) {
    ExternalFunctionStatsTestCase tests[] = {
        {
            "middle",
            [&](ExternalFunction::Options options) { setupMiddleStageOper(std::move(options)); },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testMiddleStageOper(inputDocs, nullptr, statsAssert);
            },
            [&]() { stopMiddleStageOper(); },
            ExternalFunctionOperator::kName.toString(),
        },
        {
            "sink",
            [&](ExternalFunction::Options options) {
                options.isSink = true;
                setupSinkStageOper(std::move(options));
            },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testSinkStageOper(inputDocs, statsAssert);
            },
            [&]() { stopSinkStageOper(); },
            ExternalFunctionSinkOperator::kName.toString(),
        },
    };
    for (const auto& tc : tests) {
        LOGV2_DEBUG(9929656,
                    1,
                    "Running should dlq on function error test case",
                    "stage"_attr = tc.description);
        auto mockClient = std::make_unique<MockLambdaClient>();

        auto functionName = "foo";
        auto executedVersion = "foo123";
        auto payload = R"({"ack":"ok"})";
        auto statusCode = 200;

        mockClient->expectValidation(functionName);

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
            docs.emplace_back(
                StreamDocument{Document{fromjson(fmt::format(R"({{"data":{}}})", i))}});
        }

        ExternalFunction::Options options{
            .lambdaClient = std::move(mockClient),
            .functionName = functionName,
            .execution = mongo::ExternalFunctionExecutionEnum::Sync,
            .as = boost::optional<std::string>("response"),
            .onError = mongo::OnErrorEnum::Ignore,
        };

        tc.setupFn(std::move(options));

        tc.testFn(std::move(docs), [](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 20);
            ASSERT_EQ(stats.numDlqBytes, 0);
            ASSERT_EQ(stats.numDlqDocs, 0);
            ASSERT_GREATER_THAN(stats.timeSpent.count(), 0);
        });
        tc.stopFn();
    }
}

TEST_F(ExternalFunctionTest, ShouldFailOnFailureStatusCodes) {
    ExternalFunctionStatsTestCase tests[] = {
        {
            "middle",
            [&](ExternalFunction::Options options) { setupMiddleStageOper(std::move(options)); },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testMiddleStageOper(inputDocs, nullptr, statsAssert);
            },
            [&]() { stopMiddleStageOper(); },
            ExternalFunctionOperator::kName.toString(),
        },
        {
            "sink",
            [&](ExternalFunction::Options options) {
                options.isSink = true;
                setupSinkStageOper(std::move(options));
            },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testSinkStageOper(inputDocs, statsAssert);
            },
            [&]() { stopSinkStageOper(); },
            ExternalFunctionSinkOperator::kName.toString(),
        },
    };
    for (const auto& tc : tests) {
        LOGV2_DEBUG(9929657,
                    1,
                    "Running should dlq on function error test case",
                    "stage"_attr = tc.description);
        auto codes = std::vector<Aws::Http::HttpResponseCode>{
            Aws::Http::HttpResponseCode::MULTIPLE_CHOICES,
            Aws::Http::HttpResponseCode::UNAUTHORIZED,
            Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR,
        };

        for (const auto& code : codes) {
            LOGV2_DEBUG(9929410, 1, "Running test case", "code"_attr = code);
            auto mockClient = std::make_unique<MockLambdaClient>();

            auto functionName = "foo";

            mockClient->expectValidation(functionName);

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

            ExternalFunction::Options options{
                .lambdaClient = std::move(mockClient),
                .functionName = functionName,
                .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                .as = boost::optional<std::string>("response"),
            };

            tc.setupFn(std::move(options));

            ASSERT_THROWS_WHAT(
                tc.testFn(std::vector<StreamDocument>{Document{fromjson(expectedDoc)}}, nullptr),
                std::exception,
                "Received error response from external function. Error: shouldFail");
            tc.stopFn();
        }
    }
}

TEST_F(ExternalFunctionTest, ShouldFailOnFailureErrors) {
    ExternalFunctionStatsTestCase tests[] = {
        {
            "middle",
            [&](ExternalFunction::Options options) { setupMiddleStageOper(std::move(options)); },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testMiddleStageOper(inputDocs, nullptr, statsAssert);
            },
            [&]() { stopMiddleStageOper(); },
            ExternalFunctionOperator::kName.toString(),
        },
        {
            "sink",
            [&](ExternalFunction::Options options) {
                options.isSink = true;
                setupSinkStageOper(std::move(options));
            },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testSinkStageOper(inputDocs, statsAssert);
            },
            [&]() { stopSinkStageOper(); },
            ExternalFunctionSinkOperator::kName.toString(),
        },
    };
    for (const auto& tc : tests) {
        LOGV2_DEBUG(9929658,
                    1,
                    "Running should dlq on function error test case",
                    "stage"_attr = tc.description);
        for (Aws::Lambda::LambdaError error : {
                 []() -> Aws::Lambda::LambdaError {
                     Aws::Lambda::LambdaError error =
                         Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
                             Aws::Lambda::LambdaErrors::INVALID_REQUEST_CONTENT,
                             Aws::Client::RetryableType::NOT_RETRYABLE);
                     error.SetResponseCode(Aws::Http::HttpResponseCode::BAD_REQUEST);
                     return error;
                 }(),
                 []() -> Aws::Lambda::LambdaError {
                     Aws::Lambda::LambdaError error =
                         Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
                             Aws::Lambda::LambdaErrors::THROTTLING,
                             Aws::Client::RetryableType::NOT_RETRYABLE);
                     error.SetResponseCode(Aws::Http::HttpResponseCode::BAD_REQUEST);
                     return error;
                 }(),
                 []() -> Aws::Lambda::LambdaError {
                     Aws::Lambda::LambdaError error =
                         Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
                             Aws::Lambda::LambdaErrors::RESOURCE_NOT_FOUND,
                             Aws::Client::RetryableType::NOT_RETRYABLE);
                     error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
                     return error;
                 }(),
                 []() -> Aws::Lambda::LambdaError {
                     Aws::Lambda::LambdaError error =
                         Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
                             Aws::Lambda::LambdaErrors::E_F_S_I_O,
                             Aws::Client::RetryableType::NOT_RETRYABLE);
                     error.SetResponseCode(Aws::Http::HttpResponseCode::GONE);
                     return error;
                 }(),
             }) {

            auto mockClient = std::make_unique<MockLambdaClient>();

            auto functionName = "foo";
            auto payload = R"({"data":"foo_bar"})";

            mockClient->expectValidation(functionName);

            Aws::Lambda::Model::InvokeRequest req;
            req.SetFunctionName(functionName);
            req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

            std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(payload);
            req.SetBody(body);

            error.SetMessage("shouldFail");
            error.SetRequestId("r1");
            mockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(error)));

            tc.setupFn(ExternalFunction::Options{
                .lambdaClient = std::move(mockClient),
                .functionName = functionName,
                .execution = mongo::ExternalFunctionExecutionEnum::Sync,
                .as = boost::optional<std::string>("response"),
            });


            ASSERT_THROWS_WHAT(
                tc.testFn(std::vector<StreamDocument>{Document{fromjson(payload)}}, nullptr),
                std::exception,
                "Received error response from external function. Error: shouldFail");
            tc.stopFn();
        }
    }
}


TEST_F(ExternalFunctionTest, ShouldUseOnErrorBehaviorForStatusCodes) {
    ExternalFunctionStatsTestCase tests[] = {
        {
            "middle",
            [&](ExternalFunction::Options options) { setupMiddleStageOper(std::move(options)); },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testMiddleStageOper(inputDocs, nullptr, statsAssert);
            },
            [&]() { stopMiddleStageOper(); },
            ExternalFunctionOperator::kName.toString(),
        },
        {
            "sink",
            [&](ExternalFunction::Options options) {
                options.isSink = true;
                setupSinkStageOper(std::move(options));
            },
            [&](std::vector<StreamDocument> inputDocs,
                std::function<void(OperatorStats)> statsAssert) {
                testSinkStageOper(inputDocs, statsAssert);
            },
            [&]() { stopSinkStageOper(); },
            ExternalFunctionSinkOperator::kName.toString(),
        },
    };
    for (const auto& tc : tests) {
        LOGV2_DEBUG(9929659,
                    1,
                    "Running should dlq on function error test case",
                    "stage"_attr = tc.description);
        auto mockClient = std::make_unique<MockLambdaClient>();

        auto rawMockClient = mockClient.get();
        auto functionName = "foo";
        auto payload = R"({"data":"foo_bar"})";

        mockClient->expectValidation(functionName);

        ExternalFunction::Options options{
            .lambdaClient = std::move(mockClient),
            .functionName = functionName,
            .execution = mongo::ExternalFunctionExecutionEnum::Sync,
            .as = boost::optional<std::string>("response"),
        };

        tc.setupFn(std::move(options));
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

            tc.testFn(std::vector<StreamDocument>{Document{fromjson(payload)}}, nullptr);
            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
            _dlqMessages = dlq->getMessages();
            auto dlqDoc = _dlqMessages.front();
            ASSERT_EQ(dlqDoc["errInfo"]["reason"].String(),
                      "Failed to process input document in $externalFunction with "
                      "error: Request failure in $externalFunction with error: Received "
                      "error response from external function. Error: shouldUseOnError");
        }
    }
}

TEST_F(ExternalFunctionTest, ShouldNotLogSameIDWithinAMinute) {
    int count{0};

    TickSourceMock<Milliseconds> tickSource;
    _externalFunction = std::make_unique<ExternalFunction>(
        _context.get(),
        ExternalFunction::Options{
            .functionName = "foo",
            .timer = Timer{&tickSource},
        },
        "test",
        getExternalFunctionRateLimitPerSec(_context->featureFlags));

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

TEST_F(ExternalFunctionTest, ShouldLogDifferentIDWithinAMinute) {
    int count{0};

    TickSourceMock<Milliseconds> tickSource;
    _externalFunction = std::make_unique<ExternalFunction>(
        _context.get(),
        ExternalFunction::Options{
            .functionName = "foo",
            .timer = Timer{&tickSource},
        },
        "test",
        getExternalFunctionRateLimitPerSec(_context->featureFlags));

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

TEST_F(ExternalFunctionTest, ShouldReportCorrectRequestRoundtripTimes) {
    auto mockClient = std::make_unique<MockLambdaClient>();

    const auto functionName = "foo";
    mockClient->expectValidation(functionName);

    auto rawMockClient = mockClient.get();
    auto options = ExternalFunction::Options{
        .lambdaClient = std::move(mockClient),
        .functionName = "foo",
        .execution = mongo::ExternalFunctionExecutionEnum::Sync,
        .as = boost::optional<std::string>("response"),
    };

    _externalFunction = std::make_unique<ExternalFunction>(
        _context.get(), std::move(options), _context->streamName, 100);
    _externalFunction->doRegisterMetrics(_executor->getMetricManager());

    const auto payload = R"({"data":"foo"})";
    auto addMockClientExpectation = [&](int delayMs, bool isError) {
        Aws::Lambda::Model::InvokeRequest req;
        req.SetFunctionName(functionName);
        req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);

        std::shared_ptr<Aws::IOStream> body = std::make_shared<Aws::StringStream>(payload);
        req.SetBody(body);

        if (isError) {
            Aws::Lambda::LambdaError error = Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
                Aws::Lambda::LambdaErrors::UNKNOWN, Aws::Client::RetryableType::NOT_RETRYABLE);
            error.SetResponseCode(Aws::Http::HttpResponseCode::BAD_REQUEST);
            error.SetMessage("Not Found");
            error.SetRequestId("r1");
            rawMockClient->expect(
                req, Aws::Lambda::Model::InvokeOutcome(std::move(error)), delayMs);
            return;
        }

        Aws::Lambda::Model::InvokeResult result;
        result.SetStatusCode(200);
        result.SetExecutedVersion("foo123");
        auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
        *responseBody << R"({"ack":"ok"})";
        result.ReplaceBody(responseBody);
        rawMockClient->expect(req, Aws::Lambda::Model::InvokeOutcome(std::move(result)), delayMs);
    };

    auto retrieveBucketCounts = [&](std::string result) {
        TestMetricsVisitor metrics;
        _executor->getMetricManager()->visitAllMetrics(&metrics);

        const auto& processorHistograms = metrics.histograms().find(_context->streamProcessorId);
        ASSERT_NOT_EQUALS(processorHistograms, metrics.histograms().end());

        auto histogramsByName =
            processorHistograms->second.find("external_function_operator_aws_sdk_request_time");
        ASSERT_NOT_EQUALS(histogramsByName, processorHistograms->second.end());

        auto histogramsByLabels = histogramsByName->second.find(fmt::format("result={},", result));
        ASSERT_NOT_EQUALS(histogramsByLabels, histogramsByName->second.end());

        histogramsByLabels->second->takeSnapshot();
        return histogramsByLabels->second->snapshotValue();
    };

    auto runTestAndAssert = [&](int delayMs, int bucketIndex, std::string expectedResult) {
        auto buckets = retrieveBucketCounts(expectedResult);
        ASSERT_GT(buckets.size(), bucketIndex);
        ASSERT_EQ(buckets[bucketIndex].count, 0);

        bool isError{false};
        if (expectedResult == "fail") {
            isError = true;
        }
        addMockClientExpectation(delayMs, isError);

        auto doc = StreamDocument{Document{fromjson(payload)}};
        _externalFunction->processStreamDoc(&doc);

        buckets = retrieveBucketCounts(expectedResult);
        ASSERT_GT(buckets.size(), bucketIndex);
        ASSERT_EQ(buckets[bucketIndex].count, 1);
    };

    auto delayMs = 1;
    for (int i = 0; i < 5; i++) {
        runTestAndAssert(delayMs, i, "success");
        runTestAndAssert(delayMs, i, "fail");

        // Multiply delay by a value greater than the histogram's bucket factor and starting
        // boundary, but lesser that the factor's square
        delayMs *= 5;
    }
}
};  // namespace streams
