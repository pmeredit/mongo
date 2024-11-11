/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/external_api_operator.h"

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
#include "mongo/util/net/http_client.h"
#include "mongo/util/net/http_client_mock.h"
#include "mongo/util/str.h"
#include "mongo/util/tick_source_mock.h"
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

class ExternalApiOperatorTest : public AggregationContextFixture {
public:
    ExternalApiOperatorTest() : AggregationContextFixture() {
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
    }

    void setupDag(ExternalApiOperator::Options opts) {
        _oper = std::make_unique<ExternalApiOperator>(_context.get(), std::move(opts));
        _oper->registerMetrics(_executor->getMetricManager());
        _sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);

        // Build DAG.
        _oper->addOutput(_sink.get(), 0);
        _oper->start();
        _sink->start();
    }

    void testAgainstDocs(std::vector<StreamDocument> inputDocs,
                         std::function<void(const std::deque<StreamMsgUnion>&)> testAssert) {
        // Send data message to operator and let process + flow to sink.
        _oper->onDataMsg(0, StreamDataMsg{.docs = inputDocs});
        auto messages = _sink->getMessages();
        testAssert(messages);
    }

    void testAgainstDocs(std::vector<StreamDocument> inputDocs,
                         std::function<void(const std::deque<StreamMsgUnion>&)> msgsAssert,
                         std::function<void(OperatorStats)> statsAssert) {
        // Send data message to operator and let process + flow to sink.
        _oper->onDataMsg(0, StreamDataMsg{.docs = inputDocs});
        auto messages = _sink->getMessages();
        msgsAssert(messages);
        statsAssert(_oper->getStats());
    }

    void stopDag() {
        _oper->stop();
        _sink->stop();
    }

    void updateRateLimitPerSecond(int64_t rate) {
        auto featureFlags = _context->featureFlags->testOnlyGetFeatureFlags();
        featureFlags[FeatureFlags::kExternalAPIRateLimitPerSecond.name] =
            mongo::Value::createIntOrLong(rate);
        _context->featureFlags->updateFeatureFlags(
            StreamProcessorFeatureFlags{featureFlags,
                                        std::chrono::time_point<std::chrono::system_clock>{
                                            std::chrono::system_clock::now().time_since_epoch()}});
    }

    void assertStreamMetaExternalAPI(StreamMetaExternalAPI expectedStreamMetaExternalAPI,
                                     BSONObj actualStreamMetaExternalAPIBson) {
        auto url = actualStreamMetaExternalAPIBson["url"];
        ASSERT_TRUE(url.ok());
        ASSERT_STRING_CONTAINS(url.String(), expectedStreamMetaExternalAPI.getUrl());

        auto requestType = actualStreamMetaExternalAPIBson["requestType"];
        ASSERT_TRUE(requestType.ok());
        ASSERT_EQ(requestType.String(),
                  HttpMethod_serializer(expectedStreamMetaExternalAPI.getRequestType()));

        auto httpStatusCode = actualStreamMetaExternalAPIBson["httpStatusCode"];
        ASSERT_TRUE(httpStatusCode.ok());
        ASSERT_EQ(httpStatusCode.Int(), expectedStreamMetaExternalAPI.getHttpStatusCode());
    }

protected:
    std::unique_ptr<Context> _context;
    std::unique_ptr<Executor> _executor;

    std::unique_ptr<ExternalApiOperator> _oper;
    std::unique_ptr<InMemorySinkOperator> _sink;
};

int computeNumDlqBytes(std::queue<mongo::BSONObj> dlqMsgs) {
    int numBytes{};
    for (auto dlqMsgsCopy = std::move(dlqMsgs); !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
        numBytes += dlqMsgsCopy.front().objsize();
    }

    return numBytes;
}

std::string expectedErrResponseErrMsg(int statusCode, std::string body = "") {
    return fmt::format(
        "Received error response from server. status code: {}, body: {}", statusCode, body);
}

struct ExternalApiOpereratorTestCase {
    const std::string description;
    const std::function<ExternalApiOperator::Options()> optionsFn;
    const std::vector<StreamDocument> inputDocs;
    const std::function<void(const std::deque<StreamMsgUnion>&)> msgsAssertFn;
    const std::function<void(OperatorStats)> statsAssertFn;
};

TEST_F(ExternalApiOperatorTest, ExternalApiOperatorTestCases) {
    ExternalApiOpereratorTestCase tests[] = {
        {
            "should make 2 get requests basing the url path on a string expression",
            [&] {
                std::string uri = "http://localhost:10000";
                // Set up mock http client.
                std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kGET,
                        uri + "/1",
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kGET,
                        uri + "/2",
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                return ExternalApiOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .url = uri,
                    .urlPathExpr = ExpressionFieldPath::parse(
                        _context->expCtx.get(), "$path", _context->expCtx->variablesParseState),
                    .as = "response",
                };
            },
            std::vector<StreamDocument>{Document{fromjson("{'path': '/1'}")},
                                        Document{fromjson("{'path': '/2'}")}},
            [this](std::deque<StreamMsgUnion> messages) {
                ASSERT_EQ(messages.size(), 1);
                auto msg = messages.at(0);

                ASSERT(msg.dataMsg);
                ASSERT(!msg.controlMsg);
                ASSERT_EQ(msg.dataMsg->docs.size(), 2);

                for (const auto& streamDoc : msg.dataMsg->docs) {
                    auto doc = streamDoc.doc.toBson();
                    auto response = doc["response"].Obj();
                    ASSERT_TRUE(!response.isEmpty());

                    auto ack = response["ack"];
                    ASSERT_TRUE(ack.ok());
                    ASSERT_EQ(ack.String(), "ok");

                    StreamMetaExternalAPI expectedStreamMetaExternalAPI;
                    expectedStreamMetaExternalAPI.setUrl(StringData{"http://localhost:10000"});
                    expectedStreamMetaExternalAPI.setRequestType(HttpMethodEnum::MethodGet);
                    expectedStreamMetaExternalAPI.setHttpStatusCode(200);
                    assertStreamMetaExternalAPI(
                        expectedStreamMetaExternalAPI,
                        doc[*_context->streamMetaFieldName].Obj()["externalAPI"].Obj());
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 26);
                ASSERT_EQ(stats.numOutputBytes, 0);
            },
        },
        {
            "should make 2 post requests basing the url path on a bson expression and save the "
            "response to a nested field",
            [&] {
                std::string uri = "http://localhost:10000";
                // Set up mock http client.
                std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kPOST,
                        uri + "/1",
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kPOST,
                        uri + "/2",
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                return ExternalApiOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .requestType = HttpClient::HttpMethod::kPOST,
                    .url = uri,
                    .urlPathExpr =
                        Expression::parseExpression(_context->expCtx.get(),
                                                    fromjson("{$getField: 'path'}"),
                                                    _context->expCtx->variablesParseState),
                    .as = "response.inner",
                };
            },
            std::vector<StreamDocument>{Document{fromjson("{'path': '/1'}")},
                                        Document{fromjson("{'path': '/2'}")}},
            [this](std::deque<StreamMsgUnion> messages) {
                ASSERT_EQ(messages.size(), 1);
                auto msg = messages.at(0);

                ASSERT(msg.dataMsg);
                ASSERT(!msg.controlMsg);
                ASSERT_EQ(msg.dataMsg->docs.size(), 2);

                for (const auto& streamDoc : msg.dataMsg->docs) {
                    auto doc = streamDoc.doc.toBson();
                    auto response = doc["response"].Obj();
                    ASSERT_TRUE(!response.isEmpty());

                    auto inner = response["inner"].Obj();
                    ASSERT_TRUE(!inner.isEmpty());

                    auto ack = inner["ack"];
                    ASSERT_TRUE(ack.ok());
                    ASSERT_EQ(ack.String(), "ok");

                    StreamMetaExternalAPI expectedStreamMetaExternalAPI;
                    expectedStreamMetaExternalAPI.setUrl(StringData{"http://localhost:10000"});
                    expectedStreamMetaExternalAPI.setRequestType(HttpMethodEnum::MethodPost);
                    expectedStreamMetaExternalAPI.setHttpStatusCode(200);
                    assertStreamMetaExternalAPI(
                        expectedStreamMetaExternalAPI,
                        doc[*_context->streamMetaFieldName].Obj()["externalAPI"].Obj());
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 26);
                ASSERT_EQ(stats.numOutputBytes, 26);
            },
        },
        {
            "should make a valid GET request with valid query parameters",
            [&] {
                std::string uri = "http://localhost:10000";
                auto expCtx =
                    boost::intrusive_ptr<ExpressionContextForTest>(new ExpressionContextForTest{});
                auto stringExpr = Expression::parseExpression(
                    expCtx.get(), fromjson("{$getField: 'str'}"), expCtx->variablesParseState);
                auto intExpr = Expression::parseExpression(
                    expCtx.get(), fromjson("{$getField: 'int'}"), expCtx->variablesParseState);
                auto decimalExpr = Expression::parseExpression(
                    expCtx.get(), fromjson("{$getField: 'decimal'}"), expCtx->variablesParseState);
                auto boolExpr = Expression::parseExpression(
                    expCtx.get(), fromjson("{$getField: 'bool'}"), expCtx->variablesParseState);

                std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kGET,
                        uri +
                            "?stringParam=bar&strExprParam=DynamicValue&intExprParam=10&"
                            "decimalExprParam=1.1&boolExprParam=true",
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                return ExternalApiOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .requestType = HttpClient::HttpMethod::kGET,
                    .url = uri,
                    .queryParams =
                        std::vector<std::pair<std::string, StringOrExpression>>{
                            {"stringParam", "bar"},
                            {"strExprParam", stringExpr},
                            {"intExprParam", intExpr},
                            {"decimalExprParam", decimalExpr},
                            {"boolExprParam", boolExpr},
                        },
                    .as = "response",
                };
            },
            std::vector<StreamDocument>{
                Document{fromjson(
                    "{\"str\": \"DynamicValue\", \"int\": 10, \"decimal\": 1.1, \"bool\": true}")},
            },
            [this](std::deque<StreamMsgUnion> messages) {
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

                    StreamMetaExternalAPI expectedStreamMetaExternalAPI;
                    expectedStreamMetaExternalAPI.setUrl(
                        StringData{"http://localhost:10000"} +
                        "?stringParam=bar&strExprParam=DynamicValue&intExprParam=10&"
                        "decimalExprParam=1.1&boolExprParam=true");
                    expectedStreamMetaExternalAPI.setRequestType(HttpMethodEnum::MethodGet);
                    expectedStreamMetaExternalAPI.setHttpStatusCode(200);
                    assertStreamMetaExternalAPI(
                        expectedStreamMetaExternalAPI,
                        doc[*_context->streamMetaFieldName].Obj()["externalAPI"].Obj());
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 13);
                ASSERT_EQ(stats.numOutputBytes, 0);
            },
        },
        {
            "should make a POST request with valid headers",
            [&] {
                std::string uri = "http://localhost:10000";
                auto expCtx =
                    boost::intrusive_ptr<ExpressionContextForTest>(new ExpressionContextForTest{});
                auto getFieldExpr = Expression::parseExpression(
                    expCtx.get(), fromjson("{$getField: 'foo'}"), expCtx->variablesParseState);

                std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kPOST,
                        uri,
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                return ExternalApiOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .requestType = HttpClient::HttpMethod::kPOST,
                    .url = uri,
                    .operatorHeaders =
                        std::vector<std::pair<std::string, StringOrExpression>>{
                            {"stringParam", "bar"},
                            {"exprParam", getFieldExpr},
                        },
                    .as = "response",
                };
            },
            std::vector<StreamDocument>{
                Document{fromjson("{'foo': 'DynamicValue'}")},
            },
            [this](std::deque<StreamMsgUnion> messages) {
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

                    StreamMetaExternalAPI expectedStreamMetaExternalAPI;
                    expectedStreamMetaExternalAPI.setUrl(StringData{"http://localhost:10000"});
                    expectedStreamMetaExternalAPI.setRequestType(HttpMethodEnum::MethodPost);
                    expectedStreamMetaExternalAPI.setHttpStatusCode(200);
                    assertStreamMetaExternalAPI(
                        expectedStreamMetaExternalAPI,
                        doc[*_context->streamMetaFieldName].Obj()["externalAPI"].Obj());
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 13);
                ASSERT_EQ(stats.numOutputBytes, 22);
            },
        },
        {
            "should make a valid GET request with a url-encoded URL",
            [&] {
                std::string uri = "http://localhost:10000";
                std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kGET,
                        uri + "?%25%2B-%3D_%2F%24=%3A%40%23%24%5B%5D%28%29",
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                return ExternalApiOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .requestType = HttpClient::HttpMethod::kGET,
                    .url = uri,
                    .queryParams =
                        std::vector<std::pair<std::string, StringOrExpression>>{
                            {"%+-=_/$", ":@#$[]()"},
                        },
                    .as = "response",
                };
            },
            std::vector<StreamDocument>{
                Document{fromjson("{foo: \"bar\"}")},
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
                ASSERT_EQ(stats.numInputBytes, 13);
                ASSERT_EQ(stats.numOutputBytes, 0);
            },
        },
    };

    for (const auto& tc : tests) {
        LOGV2_DEBUG(9503599, 1, "Running test case", "description"_attr = tc.description);
        setupDag(tc.optionsFn());
        testAgainstDocs(tc.inputDocs, tc.msgsAssertFn, tc.statsAssertFn);
        stopDag();
    }
}

TEST_F(ExternalApiOperatorTest, IsThrottledWithDefaultThrottleFnAndTimer) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    auto rawMockHttpClient = mockHttpClient.get();

    const auto docsToInsert = 5;
    const auto minTestDuration = Seconds(4);
    std::vector<StreamDocument> docs;
    docs.reserve(docsToInsert);

    for (int i = 0; i < docsToInsert; i++) {
        auto path = fmt::format("/{}", i);
        rawMockHttpClient->expect(
            MockHttpClient::Request{
                HttpClient::HttpMethod::kGET,
                uri.toString() + path,
            },
            MockHttpClient::Response{.code = 200,
                                     .body = tojson(BSON("ack"
                                                         << "ok"))});
        docs.emplace_back(
            StreamDocument{Document{fromjson("{" + fmt::format("'path': '{}'", path) + "}")}});
    }

    updateRateLimitPerSecond(1);

    ExternalApiOperator::Options options{
        .httpClient = std::move(mockHttpClient),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .urlPathExpr = ExpressionFieldPath::parse(
            _context->expCtx.get(), "$path", _context->expCtx->variablesParseState),
        .as = "response",
    };

    setupDag(std::move(options));


    Timer timer{};
    auto start = timer.elapsed();

    testAgainstDocs(docs, [&](std::deque<StreamMsgUnion> messages) {
        ASSERT_EQ(messages.size(), 1);
        auto msg = messages.at(0);

        ASSERT(msg.dataMsg);
        ASSERT(!msg.controlMsg);
        ASSERT_EQ(msg.dataMsg->docs.size(), docsToInsert);
    });

    auto elapsedTime = timer.elapsed() - start;
    ASSERT_GREATER_THAN_OR_EQUALS(elapsedTime, minTestDuration);
    TestMetricsVisitor metrics;
    _executor->getMetricManager()->visitAllMetrics(&metrics);
    const auto& operatorCounters = metrics.counters().find(_context->streamProcessorId);
    long metricThrottleDuration = -1;
    if (operatorCounters != metrics.counters().end()) {
        auto it =
            operatorCounters->second.find(std::string{"rest_operator_throttle_duration_micros"});
        if (it != operatorCounters->second.end()) {
            metricThrottleDuration = it->second->value();
        }
    }
    ASSERT_GREATER_THAN_OR_EQUALS(
        Microseconds(metricThrottleDuration),
        minTestDuration - Milliseconds(5));  // metric is a hair flakey based on host configuration
}

TEST_F(ExternalApiOperatorTest, IsThrottledWithOverridenThrottleFnAndTimer) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    auto rawMockHttpClient = mockHttpClient.get();

    TickSourceMock<Microseconds> tickSource{};
    Timer timer{&tickSource};

    ExternalApiOperator::Options options{
        .httpClient = std::move(mockHttpClient),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",

        .throttleFn =
            [&tickSource](Microseconds throttleDelay) { tickSource.advance(throttleDelay); },
        .timer = timer,
    };

    auto addMockClientExpectation = [rawMockHttpClient, uri]() {
        rawMockHttpClient->expect(
            MockHttpClient::Request{
                HttpClient::HttpMethod::kGET,
                uri.toString(),
            },
            MockHttpClient::Response{.code = 200,
                                     .body = tojson(BSON("ack"
                                                         << "ok"))});
    };

    auto testAssert = [this, uri](std::deque<StreamMsgUnion> messages) {
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

        StreamMetaExternalAPI expectedStreamMetaExternalAPI;
        expectedStreamMetaExternalAPI.setUrl(uri);
        expectedStreamMetaExternalAPI.setRequestType(HttpMethodEnum::MethodGet);
        expectedStreamMetaExternalAPI.setHttpStatusCode(200);
        assertStreamMetaExternalAPI(expectedStreamMetaExternalAPI,
                                    doc[*_context->streamMetaFieldName].Obj()["externalAPI"].Obj());
    };

    setupDag(std::move(options));

    auto now = timer.elapsed();

    updateRateLimitPerSecond(1);
    addMockClientExpectation();
    testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                    testAssert);
    ASSERT_EQ(timer.elapsed() - now, Seconds(0));

    addMockClientExpectation();
    testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                    testAssert);
    ASSERT_EQ(timer.elapsed() - now, Seconds(1));
}

TEST_F(ExternalApiOperatorTest, RateLimitingParametersUpdate) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    auto rawMockHttpClient = mockHttpClient.get();

    TickSourceMock<Microseconds> tickSource{};
    Timer timer{&tickSource};

    ExternalApiOperator::Options options{
        .httpClient = std::move(mockHttpClient),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",

        .throttleFn =
            [&tickSource](Microseconds throttleDelay) { tickSource.advance(throttleDelay); },
        .timer = timer,
    };

    auto addMockClientExpectation = [rawMockHttpClient, uri]() {
        rawMockHttpClient->expect(
            MockHttpClient::Request{
                HttpClient::HttpMethod::kGET,
                uri.toString(),
            },
            MockHttpClient::Response{.code = 200,
                                     .body = tojson(BSON("ack"
                                                         << "ok"))});
    };

    auto testAssert = [this, uri](std::deque<StreamMsgUnion> messages) {
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

        StreamMetaExternalAPI expectedStreamMetaExternalAPI;
        expectedStreamMetaExternalAPI.setUrl(uri);
        expectedStreamMetaExternalAPI.setRequestType(HttpMethodEnum::MethodGet);
        expectedStreamMetaExternalAPI.setHttpStatusCode(200);
        assertStreamMetaExternalAPI(expectedStreamMetaExternalAPI,
                                    doc[*_context->streamMetaFieldName].Obj()["externalAPI"].Obj());
    };

    setupDag(std::move(options));

    for (int rateLimitPerSec : {1, 2, 4}) {
        auto now = timer.elapsed();

        updateRateLimitPerSecond(rateLimitPerSec);

        // Generate full burst
        for (int i = 0; i < rateLimitPerSec; i++) {
            addMockClientExpectation();
            testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                            testAssert);
        }

        // Assert that no throttling occured during the burst
        ASSERT_EQ(timer.elapsed() - now, Milliseconds(0));

        addMockClientExpectation();
        testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                        testAssert);

        // Assert that throttling occured after initial burst
        ASSERT_EQ(timer.elapsed() - now, Milliseconds(1000 / rateLimitPerSec));
    }
}

TEST_F(ExternalApiOperatorTest, GetWithBadQueryParams) {
    StringData uri{"http://localhost:10000"};
    auto expCtx = boost::intrusive_ptr<ExpressionContextForTest>(new ExpressionContextForTest{});

    std::vector<boost::intrusive_ptr<mongo::Expression>> badExpressionParams{
        // Object
        mongo::Expression::parseExpression(expCtx.get(),
                                           fromjson("{$arrayToObject: [[{k: \"age\", v: 91}]] }"),
                                           _context->expCtx->variablesParseState),
        // Array
        mongo::Expression::parseExpression(expCtx.get(),
                                           fromjson("{$objectToArray: { item: \"foo\", qty: 25}}"),
                                           _context->expCtx->variablesParseState),
        // BinData / UUID
        mongo::Expression::parseExpression(
            expCtx.get(),
            fromjson("{$toUUID: \"0e3b9063-8abd-4eb3-9f9f-f4c59fd30a60\"}"),
            _context->expCtx->variablesParseState),
        // ObjectId
        mongo::Expression::parseExpression(expCtx.get(),
                                           fromjson("{$toObjectId: \"5ab9cbfa31c2ab715d42129e\"}"),
                                           _context->expCtx->variablesParseState),
        // Date
        mongo::Expression::parseExpression(expCtx.get(),
                                           fromjson("{$toDate: 1730145590000}"),
                                           _context->expCtx->variablesParseState),
        // Null
        mongo::Expression::parseExpression(
            expCtx.get(),
            // Note: $convert returns null if input is null.
            fromjson("{$convert: {input: null, to: {type: \"double\"}}}"),
            _context->expCtx->variablesParseState),
    };

    for (const auto& badParam : badExpressionParams) {
        // param evaluating to empty value.
        std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
        ExternalApiOperator::Options options{
            .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
            .requestType = HttpClient::HttpMethod::kGET,
            .url = uri.toString(),
            .queryParams = std::vector<std::pair<std::string, StringOrExpression>>{{
                "badInput",
                badParam,
            }},
            .as = "response",
        };

        setupDag(std::move(options));
        testAgainstDocs(
            std::vector<StreamDocument>{
                Document{fromjson("{'foo': 'DynamicValue'}")},
            },
            [&](std::deque<StreamMsgUnion> messages) {
                // The invalid expression should've caused the doc to go into the DLQ.
                ASSERT_EQ(messages.size(), 0);

                auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
                auto dlqMsgs = dlq->getMessages();
                ASSERT_EQ(1, dlqMsgs.size());
                ASSERT_EQ(
                    "Failed to process input document in ExternalApiOperator with error: Expected "
                    "$externalAPI.parameters values to evaluate as a number, "
                    "string, or boolean.",
                    dlqMsgs.front()["errInfo"]["reason"].String());
                ASSERT_EQ("ExternalApiOperator", dlqMsgs.front()["operatorName"].String());
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 0);
                ASSERT_EQ(stats.numOutputBytes, 0);
                ASSERT_EQ(stats.numDlqBytes, 314);
                ASSERT_EQ(stats.numDlqDocs, 1);
            });
    }
}

TEST_F(ExternalApiOperatorTest, GetWithBadHeaders) {
    StringData uri{"http://localhost:10000"};
    auto expCtx = boost::intrusive_ptr<ExpressionContextForTest>(new ExpressionContextForTest{});


    std::vector<boost::intrusive_ptr<mongo::Expression>> badExpressionHeaders{
        // Undefined
        mongo::ExpressionFieldPath::parse(
            expCtx.get(), "$randomFieldThatDoesntExist", _context->expCtx->variablesParseState),
        // Boolean
        mongo::Expression::parseExpression(
            expCtx.get(), fromjson("{$eq: [24, 24] }"), _context->expCtx->variablesParseState),
        // Int
        mongo::Expression::parseExpression(
            expCtx.get(), fromjson("{$sum: [1,2,3]}"), _context->expCtx->variablesParseState),
        // Decimal
        mongo::Expression::parseExpression(
            expCtx.get(), fromjson("{$sum: [1,2,3]}"), _context->expCtx->variablesParseState),
        // Object
        mongo::Expression::parseExpression(expCtx.get(),
                                           fromjson("{$arrayToObject: [[{k: \"age\", v: 91}]] }"),
                                           _context->expCtx->variablesParseState),
        // Array
        mongo::Expression::parseExpression(expCtx.get(),
                                           fromjson("{$objectToArray: { item: \"foo\", qty: 25}}"),
                                           _context->expCtx->variablesParseState),
        // BinData / UUID
        mongo::Expression::parseExpression(
            expCtx.get(),
            fromjson("{$toUUID: \"0e3b9063-8abd-4eb3-9f9f-f4c59fd30a60\"}"),
            _context->expCtx->variablesParseState),
        // ObjectId
        mongo::Expression::parseExpression(expCtx.get(),
                                           fromjson("{$toObjectId: \"5ab9cbfa31c2ab715d42129e\"}"),
                                           _context->expCtx->variablesParseState),
        // Date
        mongo::Expression::parseExpression(expCtx.get(),
                                           fromjson("{$toDate: 1730145590000}"),
                                           _context->expCtx->variablesParseState),
        // Null
        mongo::Expression::parseExpression(
            expCtx.get(),
            // Note: $convert returns null if input is null.
            fromjson("{$convert: {input: null, to: {type: \"double\"}}}"),
            _context->expCtx->variablesParseState),
    };

    for (const auto& badHeader : badExpressionHeaders) {
        // header evaluating to empty value.
        std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();

        ExternalApiOperator::Options options{
            .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
            .requestType = HttpClient::HttpMethod::kGET,
            .url = uri.toString(),
            .operatorHeaders =
                std::vector<std::pair<std::string, StringOrExpression>>{{"badHeader", badHeader}},
            .as = "response",
        };

        setupDag(std::move(options));
        testAgainstDocs(
            std::vector<StreamDocument>{
                Document{fromjson("{'foo': 'DynamicValue'}")},
            },
            [&](std::deque<StreamMsgUnion> messages) {
                // The invalid expression should've caused the doc to go into the DLQ.
                ASSERT_EQ(messages.size(), 0);

                auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
                auto dlqMsgs = dlq->getMessages();
                ASSERT_EQ(1, dlqMsgs.size());
                ASSERT_EQ(
                    "Failed to process input document in ExternalApiOperator with error: Expected "
                    "$externalAPI.headers values to evaluate to a string.",
                    dlqMsgs.front()["errInfo"]["reason"].String());
                ASSERT_EQ("ExternalApiOperator", dlqMsgs.front()["operatorName"].String());
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 0);
                ASSERT_EQ(stats.numOutputBytes, 0);
                ASSERT_EQ(stats.numDlqBytes, 291);
                ASSERT_EQ(stats.numDlqDocs, 1);
            });
    }
}
TEST_F(ExternalApiOperatorTest, ShouldDLQOnErrorResponseByDefault) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::uint16_t statusCode{400};
    std::string responseBody{tojson(BSON("ack"
                                         << "ok"))};
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString() + "/1",
        },
        MockHttpClient::Response{.code = statusCode, .body = responseBody});
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString() + "/2",
        },
        MockHttpClient::Response{.code = statusCode, .body = responseBody});

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .urlPathExpr = ExpressionFieldPath::parse(
            _context->expCtx.get(), "$path", _context->expCtx->variablesParseState),
        .as = "response",
    };

    setupDag(std::move(options));

    std::vector<StreamDocument> inputMsgs = {Document{fromjson("{'path': '/1'}")},
                                             Document{fromjson("{'path': '/2'}")}};
    std::queue<mongo::BSONObj> dlqMsgs;
    testAgainstDocs(
        std::move(inputMsgs),
        [this, &dlqMsgs, statusCode, responseBody](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 0);

            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
            ASSERT_EQ(dlq->numMessages(), 2);

            dlqMsgs = dlq->getMessages();
            for (auto dlqMsgsCopy{dlqMsgs}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
                auto msg = dlqMsgsCopy.front();
                ASSERT_EQ(msg["errInfo"]["reason"].String(),
                          "Failed to process input document in ExternalApiOperator with error: " +
                              expectedErrResponseErrMsg(statusCode, responseBody));
            }
        },
        [this, &dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 2);
        });
}

TEST_F(ExternalApiOperatorTest, ShouldDLQOnErrorResponse) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::uint16_t statusCode{400};
    std::string responseBody{tojson(BSON("ack"
                                         << "ok"))};
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString() + "/1",
        },
        MockHttpClient::Response{.code = statusCode, .body = responseBody});
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString() + "/2",
        },
        MockHttpClient::Response{.code = 400, .body = responseBody});

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .urlPathExpr = ExpressionFieldPath::parse(
            _context->expCtx.get(), "$path", _context->expCtx->variablesParseState),
        .as = "response",
        .onError = mongo::OnErrorEnum::DLQ,
    };

    setupDag(std::move(options));

    std::vector<StreamDocument> inputMsgs = {Document{fromjson("{'path': '/1'}")},
                                             Document{fromjson("{'path': '/2'}")}};
    std::queue<mongo::BSONObj> dlqMsgs;
    testAgainstDocs(
        std::move(inputMsgs),
        [this, &dlqMsgs, statusCode, responseBody](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 0);

            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
            ASSERT_EQ(dlq->numMessages(), 2);

            dlqMsgs = dlq->getMessages();
            for (auto dlqMsgsCopy{dlqMsgs}; !dlqMsgsCopy.empty(); dlqMsgsCopy.pop()) {
                auto msg = dlqMsgsCopy.front();
                ASSERT_EQ(msg["errInfo"]["reason"].String(),
                          "Failed to process input document in ExternalApiOperator with "
                          "error: " +
                              expectedErrResponseErrMsg(statusCode, responseBody));
            }
        },
        [this, &dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 2);
        });
}

TEST_F(ExternalApiOperatorTest, ShouldIgnoreOnErrorResponse) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kDELETE,
            uri.toString() + "/1",
        },
        MockHttpClient::Response{.code = 400,
                                 .body = tojson(BSON("ack"
                                                     << "ok"))});
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kDELETE,
            uri.toString() + "/2",
        },
        MockHttpClient::Response{.code = 400,
                                 .body = tojson(BSON("ack"
                                                     << "ok"))});

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kDELETE,
        .url = uri.toString(),
        .urlPathExpr = ExpressionFieldPath::parse(
            _context->expCtx.get(), "$path", _context->expCtx->variablesParseState),
        .as = "response",
        .onError = mongo::OnErrorEnum::Ignore,
    };

    setupDag(std::move(options));

    std::vector<StreamDocument> inputMsgs = {Document{fromjson("{'path': '/1'}")},
                                             Document{fromjson("{'path': '/2'}")}};
    testAgainstDocs(
        std::move(inputMsgs),
        [this, uri](std::deque<StreamMsgUnion> messages) {
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

                StreamMetaExternalAPI expectedStreamMetaExternalAPI;
                expectedStreamMetaExternalAPI.setUrl(uri);
                expectedStreamMetaExternalAPI.setRequestType(HttpMethodEnum::MethodDelete);
                expectedStreamMetaExternalAPI.setHttpStatusCode(400);
                assertStreamMetaExternalAPI(
                    expectedStreamMetaExternalAPI,
                    docBSON[*_context->streamMetaFieldName].Obj()["externalAPI"].Obj());
            }
        },
        [](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, 0);
            ASSERT_EQ(stats.numDlqDocs, 0);
        });
}

TEST_F(ExternalApiOperatorTest, FailOnError) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();

    std::uint16_t statusCode{400};
    auto responseBody = tojson(BSON("ack"
                                    << "ok"));
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = statusCode, .body = responseBody});

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",
        .onError = mongo::OnErrorEnum::Fail,
    };

    setupDag(std::move(options));

    ASSERT_THROWS_WHAT(
        testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                        [](auto _) {}),
        DBException,
        expectedErrResponseErrMsg(statusCode, responseBody));
}

TEST_F(ExternalApiOperatorTest, ShouldFailOnNon2XXStatus) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    auto rawMockHttpClient = mockHttpClient.get();

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",
    };

    setupDag(std::move(options));

    auto responseBody = tojson(BSON("ack"
                                    << "ok"));
    for (uint16_t httpStatusCode : {199, 300, 400, 500}) {
        rawMockHttpClient->expect(
            MockHttpClient::Request{
                HttpClient::HttpMethod::kGET,
                uri.toString(),
            },
            MockHttpClient::Response{.code = httpStatusCode, .body = responseBody});

        testAgainstDocs(
            std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
            [this, httpStatusCode, responseBody](std::deque<StreamMsgUnion> _) {
                auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
                ASSERT_EQ(dlq->numMessages(), 1);

                auto dlqMsgs = dlq->getMessages();
                auto dlqDoc = std::move(dlqMsgs.front());
                ASSERT_EQ(dlqDoc["errInfo"]["reason"].String(),
                          "Failed to process input document in ExternalApiOperator with error: " +
                              expectedErrResponseErrMsg(httpStatusCode, responseBody));
            });
    }
}

TEST_F(ExternalApiOperatorTest, ShouldDLQOnUnsupportedResponseFormatByDefault) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = "<info>im xml</info>"});

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",
    };

    setupDag(std::move(options));

    std::queue<BSONObj> dlqMsgs;
    testAgainstDocs(
        std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
        [this, &dlqMsgs](std::deque<StreamMsgUnion> _) {
            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
            ASSERT_EQ(dlq->numMessages(), 1);

            dlqMsgs = dlq->getMessages();
            auto dlqMsgsCopy{dlqMsgs};

            auto dlqDoc = std::move(dlqMsgsCopy.front());
            ASSERT_STRING_CONTAINS(
                dlqDoc["errInfo"]["reason"].String(),
                "Failed to process input document in ExternalApiOperator with error: "
                "Failed to parse response body in ExternalApiOperator with error:");
        },
        [&dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 19);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 1);
        });
}

TEST_F(ExternalApiOperatorTest, ShouldDLQOnUnsupportedResponseFormat) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = "<info>im xml</info>"});

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",
        .onError = mongo::OnErrorEnum::DLQ,
    };

    setupDag(std::move(options));

    std::queue<BSONObj> dlqMsgs;
    testAgainstDocs(
        std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
        [this, &dlqMsgs](std::deque<StreamMsgUnion> _) {
            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
            ASSERT_EQ(dlq->numMessages(), 1);

            dlqMsgs = dlq->getMessages();
            auto dlqMsgsCopy{dlqMsgs};

            auto dlqDoc = std::move(dlqMsgsCopy.front());
            ASSERT_STRING_CONTAINS(
                dlqDoc["errInfo"]["reason"].String(),
                "Failed to process input document in ExternalApiOperator with error: "
                "Failed to parse response body in ExternalApiOperator with error:");
        },
        [&dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 19);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 1);
        });
}

TEST_F(ExternalApiOperatorTest, ShouldFailOnUnsupportedResponseFormat) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = "<info>im xml</info>"});

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",
        .onError = mongo::OnErrorEnum::Fail,
    };

    setupDag(std::move(options));

    ASSERT_THROWS(
        testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                        [](auto _) {}),
        bsoncxx::exception);
}

TEST_F(ExternalApiOperatorTest, ShouldIgnoreUnsupportedResponseFormat) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = "<info>im xml</info>"});

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",
        .onError = mongo::OnErrorEnum::Ignore,
    };

    setupDag(std::move(options));

    testAgainstDocs(
        std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
        [this, uri](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 1);
            auto msg = messages.at(0);

            ASSERT(msg.dataMsg);
            ASSERT(!msg.controlMsg);
            ASSERT_EQ(msg.dataMsg->docs.size(), 1);

            auto streamDoc = msg.dataMsg->docs[0];
            auto docBSON = streamDoc.doc.toBson();
            auto response = docBSON["response"].Obj();
            ASSERT_TRUE(response.isEmpty());

            StreamMetaExternalAPI expectedStreamMetaExternalAPI;
            expectedStreamMetaExternalAPI.setUrl(uri);
            expectedStreamMetaExternalAPI.setRequestType(HttpMethodEnum::MethodGet);
            expectedStreamMetaExternalAPI.setHttpStatusCode(200);
            assertStreamMetaExternalAPI(
                expectedStreamMetaExternalAPI,
                docBSON[*_context->streamMetaFieldName].Obj()["externalAPI"].Obj());
        },
        [](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 19);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, 0);
            ASSERT_EQ(stats.numDlqDocs, 0);
        });
}

TEST_F(ExternalApiOperatorTest, ShouldSupportEmptyPayload) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kDELETE,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = ""});

    ExternalApiOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .requestType = HttpClient::HttpMethod::kDELETE,
        .url = uri.toString(),
        .as = "response",
    };

    setupDag(std::move(options));

    testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                    [this, uri](std::deque<StreamMsgUnion> messages) {
                        ASSERT_EQ(messages.size(), 1);
                        auto msg = messages.at(0);

                        ASSERT(msg.dataMsg);
                        ASSERT(!msg.controlMsg);
                        ASSERT_EQ(msg.dataMsg->docs.size(), 1);

                        auto streamDoc = msg.dataMsg->docs[0];
                        auto docBSON = streamDoc.doc.toBson();
                        auto response = docBSON["response"].Obj();
                        ASSERT_TRUE(response.isEmpty());

                        StreamMetaExternalAPI expectedStreamMetaExternalAPI;
                        expectedStreamMetaExternalAPI.setUrl(uri);
                        expectedStreamMetaExternalAPI.setRequestType(HttpMethodEnum::MethodDelete);
                        expectedStreamMetaExternalAPI.setHttpStatusCode(200);
                        assertStreamMetaExternalAPI(
                            expectedStreamMetaExternalAPI,
                            docBSON[*_context->streamMetaFieldName].Obj()["externalAPI"].Obj());
                    });
}

// TODO(SERVER-95032): Add failure case test where we DLQ a a document once we can use the planner
// to create the pipeline

};  // namespace streams
