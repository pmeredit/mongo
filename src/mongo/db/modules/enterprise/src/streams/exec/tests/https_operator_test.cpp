/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/https_operator.h"

#include <bsoncxx/exception/error_code.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <cstdint>
#include <curl/curl.h>
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

class HttpsOperatorTest : public AggregationContextFixture {
public:
    HttpsOperatorTest() : AggregationContextFixture() {
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
        curl_global_init(CURL_GLOBAL_ALL);
    }

    ~HttpsOperatorTest() override {
        curl_global_cleanup();
    }

    void setupDag(HttpsOperator::Options opts) {
        _oper = std::make_unique<HttpsOperator>(_context.get(), std::move(opts));
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
        featureFlags[FeatureFlags::kHttpsRateLimitPerSecond.name] =
            mongo::Value::createIntOrLong(rate);
        _context->featureFlags->updateFeatureFlags(
            StreamProcessorFeatureFlags{featureFlags,
                                        std::chrono::time_point<std::chrono::system_clock>{
                                            std::chrono::system_clock::now().time_since_epoch()}});
    }

    void assertStreamMetaHttps(StreamMetaHttps expectedStreamMetaHttps,
                               BSONObj actualStreamMetaHttpsBson) {
        auto url = actualStreamMetaHttpsBson["url"];
        ASSERT_TRUE(url.ok());
        ASSERT_STRING_CONTAINS(url.String(), expectedStreamMetaHttps.getUrl());

        auto method = actualStreamMetaHttpsBson["method"];
        ASSERT_TRUE(method.ok());
        ASSERT_EQ(method.String(), HttpMethod_serializer(expectedStreamMetaHttps.getMethod()));

        auto httpStatusCode = actualStreamMetaHttpsBson["httpStatusCode"];
        ASSERT_TRUE(httpStatusCode.ok());
        ASSERT_EQ(httpStatusCode.Int(), expectedStreamMetaHttps.getHttpStatusCode());

        auto responseTimeMs = actualStreamMetaHttpsBson["responseTimeMs"];
        ASSERT_TRUE(responseTimeMs.ok());
        auto responseTimeMsDiff =
            expectedStreamMetaHttps.getResponseTimeMs() - responseTimeMs.Int();
        if (responseTimeMsDiff < 0) {
            responseTimeMsDiff *= -1;
        }
        ASSERT_LESS_THAN_OR_EQUALS(responseTimeMsDiff, 50);
    }

    void tryLog(int id, std::function<void(int logID)> logFn) {
        _oper->tryLog(id, logFn);
    }

    std::string createUrlFromParts(const std::string& baseUrl,
                                   const std::string& path,
                                   const std::vector<std::string>& queryParams) {
        auto oper =
            std::make_unique<HttpsOperator>(_context.get(), HttpsOperator::Options{.url = baseUrl});
        oper->parseBaseUrl();
        return oper->makeUrlString(path, queryParams);
    }

    boost::optional<std::string> parseContentTypeFromHeaders(StringData rawHeaders) {
        return HttpsOperator::parseContentTypeFromHeaders(rawHeaders);
    }

protected:
    std::unique_ptr<Context> _context;
    std::unique_ptr<Executor> _executor;

    std::unique_ptr<HttpsOperator> _oper;
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

struct HttpsOpereratorTestCase {
    const std::string description;
    const std::function<HttpsOperator::Options()> optionsFn;
    const std::vector<StreamDocument> inputDocs;
    const std::function<void(const std::deque<StreamMsgUnion>&)> msgsAssertFn;
    const std::function<void(OperatorStats)> statsAssertFn;
};


// These tests require the use of curl_url_set and curl_url_get which was introduced to curl
// in 7.78.0. We cannot run these tests in evergreen hosts with libcurl versions lower than this.
// https://curl.se/libcurl/c/curl_url_set.html
#if LIBCURL_VERSION_NUM < 0x074e00

// Add a single target for this test suite to avoid "no suites registered" error.
TEST_F(HttpsOperatorTest, Noop) {}

#else

TEST_F(HttpsOperatorTest, HttpsOperatorTestCases) {
    HttpsOpereratorTestCase tests[] = {
        {
            "should make 1 basic GET request",
            [&] {
                std::string uri = "http://localhost:10000/";
                // Set up mock http client.
                std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kGET,
                        uri,
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                return HttpsOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .url = uri,
                    .as = "response",
                };
            },
            std::vector<StreamDocument>{Document{fromjson("{'foo': 'bar'}")}},
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

                    StreamMetaHttps expectedStreamMetaHttps;
                    expectedStreamMetaHttps.setUrl(StringData{"http://localhost:10000"});
                    expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodGet);
                    expectedStreamMetaHttps.setHttpStatusCode(200);
                    assertStreamMetaHttps(expectedStreamMetaHttps,
                                          doc[*_context->streamMetaFieldName].Obj()["https"].Obj());
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 13);
                ASSERT_EQ(stats.numOutputBytes, 0);
            },
        },
        {
            "should make 2 get requests basing the url path on a string expression",
            [&] {
                std::string uri = "http://localhost:10000";
                // Set up mock http client.
                std::unique_ptr<StubbableMockHttpClient> mockHttpClient =
                    std::make_unique<StubbableMockHttpClient>(
                        boost::make_optional<std::function<void()>>([]() { sleep(1); }));
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

                return HttpsOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .url = uri,
                    .pathExpr = ExpressionFieldPath::parse(
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

                    StreamMetaHttps expectedStreamMetaHttps;
                    expectedStreamMetaHttps.setUrl(StringData{"http://localhost:10000"});
                    expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodGet);
                    expectedStreamMetaHttps.setHttpStatusCode(200);
                    expectedStreamMetaHttps.setResponseTimeMs(1000);
                    assertStreamMetaHttps(expectedStreamMetaHttps,
                                          doc[*_context->streamMetaFieldName].Obj()["https"].Obj());
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

                return HttpsOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .method = HttpClient::HttpMethod::kPOST,
                    .url = uri,
                    .pathExpr = Expression::parseExpression(_context->expCtx.get(),
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

                    StreamMetaHttps expectedStreamMetaHttps;
                    expectedStreamMetaHttps.setUrl(StringData{"http://localhost:10000"});
                    expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodPost);
                    expectedStreamMetaHttps.setHttpStatusCode(200);
                    assertStreamMetaHttps(expectedStreamMetaHttps,
                                          doc[*_context->streamMetaFieldName].Obj()["https"].Obj());
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
                            "/?stringParam=bar&strExprParam=DynamicValue&intExprParam=10&"
                            "decimalExprParam=1.1&boolExprParam=true",
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                return HttpsOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .method = HttpClient::HttpMethod::kGET,
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

                    StreamMetaHttps expectedStreamMetaHttps;
                    expectedStreamMetaHttps.setUrl(
                        StringData{"http://localhost:10000"} +
                        "/?stringParam=bar&strExprParam=DynamicValue&intExprParam=10&"
                        "decimalExprParam=1.1&boolExprParam=true");
                    expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodGet);
                    expectedStreamMetaHttps.setHttpStatusCode(200);
                    assertStreamMetaHttps(expectedStreamMetaHttps,
                                          doc[*_context->streamMetaFieldName].Obj()["https"].Obj());
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
                std::string uri = "http://localhost:10000/";
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

                return HttpsOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .method = HttpClient::HttpMethod::kPOST,
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

                    StreamMetaHttps expectedStreamMetaHttps;
                    expectedStreamMetaHttps.setUrl(StringData{"http://localhost:10000"});
                    expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodPost);
                    expectedStreamMetaHttps.setHttpStatusCode(200);
                    assertStreamMetaHttps(expectedStreamMetaHttps,
                                          doc[*_context->streamMetaFieldName].Obj()["https"].Obj());
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
                        uri + "/?%25%2b-=_%2f%24%3d%3a%40%23%24%5b%5d%28%29",
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                return HttpsOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .method = HttpClient::HttpMethod::kGET,
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
        {
            "should make a POST request with payload pipeline",
            [&] {
                auto rawPipeline = std::vector<mongo::BSONObj>{
                    fromjson(
                        R"({ $replaceRoot: { newRoot: "$fullDocument.payload" }}, { $project: { include: 1 }})"),
                };
                auto pipeline = Pipeline::parse(rawPipeline, _context->expCtx);
                pipeline->optimizePipeline();

                std::string uri = "http://localhost:10000/";

                std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kPOST,
                        uri,
                    },
                    MockHttpClient::Response{.code = 200, .body = R"({"ack": "ok"})"});

                return HttpsOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .method = HttpClient::HttpMethod::kPOST,
                    .url = uri,
                    .as = "response",
                    .payloadPipeline = FeedablePipeline{std::move(pipeline)},
                };
            },
            std::vector<StreamDocument>{
                Document{fromjson(
                    "{'fullDocument':{ 'payload': {'include': 'feefie', 'exlude': 'fohfum'}}}")},
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
                ASSERT_EQ(stats.numOutputBytes, 38);
            },
        },
        {
            "should handle being returned a text/plain response",
            [&] {
                std::string uri = "http://localhost:10000/";
                std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
                mockHttpClient->expect(
                    MockHttpClient::Request{
                        HttpClient::HttpMethod::kGET,
                        uri,
                    },
                    MockHttpClient::Response{
                        .code = 200,
                        // mock client does not include carriage return like HTTP headers expect
                        .header = std::vector<std::string>{"content-type: text/plain\r"},
                        .body = "simple text response"});

                return HttpsOperator::Options{
                    .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                    .method = HttpClient::HttpMethod::kGET,
                    .url = uri,
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
                    auto response = doc["response"].valueStringData();
                    ASSERT_EQ(response, "simple text response");
                }
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 20);
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

TEST_F(HttpsOperatorTest, IsThrottledWithDefaultThrottleFnAndTimer) {
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

    HttpsOperator::Options options{
        .httpClient = std::move(mockHttpClient),
        .method = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .pathExpr = ExpressionFieldPath::parse(
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

TEST_F(HttpsOperatorTest, IsThrottledWithOverridenThrottleFnAndTimer) {
    StringData uri{"http://localhost:10000/"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    auto rawMockHttpClient = mockHttpClient.get();

    TickSourceMock<Microseconds> tickSource{};
    Timer timer{&tickSource};

    HttpsOperator::Options options{
        .httpClient = std::move(mockHttpClient),
        .method = HttpClient::HttpMethod::kGET,
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

        StreamMetaHttps expectedStreamMetaHttps;
        expectedStreamMetaHttps.setUrl(uri);
        expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodGet);
        expectedStreamMetaHttps.setHttpStatusCode(200);
        assertStreamMetaHttps(expectedStreamMetaHttps,
                              doc[*_context->streamMetaFieldName].Obj()["https"].Obj());
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

TEST_F(HttpsOperatorTest, RateLimitingParametersUpdate) {
    StringData uri{"http://localhost:10000/"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    auto rawMockHttpClient = mockHttpClient.get();

    TickSourceMock<Microseconds> tickSource{};
    Timer timer{&tickSource};

    HttpsOperator::Options options{
        .httpClient = std::move(mockHttpClient),
        .method = HttpClient::HttpMethod::kGET,
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

        StreamMetaHttps expectedStreamMetaHttps;
        expectedStreamMetaHttps.setUrl(uri);
        expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodGet);
        expectedStreamMetaHttps.setHttpStatusCode(200);
        assertStreamMetaHttps(expectedStreamMetaHttps,
                              doc[*_context->streamMetaFieldName].Obj()["https"].Obj());
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

TEST_F(HttpsOperatorTest, GetWithBadQueryParams) {
    StringData uri{"http://localhost:10000"};
    auto expCtx = boost::intrusive_ptr<ExpressionContextForTest>(new ExpressionContextForTest{});

    // Test bad param types.
    {
        std::vector<boost::intrusive_ptr<mongo::Expression>> badExpressionParams{
            // Object
            mongo::Expression::parseExpression(
                expCtx.get(),
                fromjson("{$arrayToObject: [[{k: \"age\", v: 91}]] }"),
                _context->expCtx->variablesParseState),
            // Array
            mongo::Expression::parseExpression(
                expCtx.get(),
                fromjson("{$objectToArray: { item: \"foo\", qty: 25}}"),
                _context->expCtx->variablesParseState),
            // BinData / UUID
            mongo::Expression::parseExpression(
                expCtx.get(),
                fromjson("{$toUUID: \"0e3b9063-8abd-4eb3-9f9f-f4c59fd30a60\"}"),
                _context->expCtx->variablesParseState),
            // ObjectId
            mongo::Expression::parseExpression(
                expCtx.get(),
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
            HttpsOperator::Options options{
                .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                .method = HttpClient::HttpMethod::kGET,
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
                        "Failed to process input document in $https with error: "
                        "Expected "
                        "$https.parameters values to evaluate as a number, "
                        "string, or boolean.",
                        dlqMsgs.front()["errInfo"]["reason"].String());
                    ASSERT_EQ("HttpsOperator", dlqMsgs.front()["operatorName"].String());
                },
                [](OperatorStats stats) {
                    ASSERT_EQ(stats.numInputBytes, 0);
                    ASSERT_EQ(stats.numOutputBytes, 0);
                    ASSERT_EQ(stats.numDlqBytes, 289);
                    ASSERT_EQ(stats.numDlqDocs, 1);
                });
        }
    }

    // Test malformed param formatting
    {
        std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
        HttpsOperator::Options options{
            .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
            .method = HttpClient::HttpMethod::kGET,
            .url = uri.toString(),
            .queryParams = std::vector<std::pair<std::string, StringOrExpression>>{{
                "",  // Empty key is invalid.
                "bar",
            }},
            .as = "response",
        };

        setupDag(std::move(options));
        testAgainstDocs(
            std::vector<StreamDocument>{
                Document{fromjson("{'foo': 'DynamicValue'}")},
            },
            [&](std::deque<StreamMsgUnion> messages) {
                ASSERT_EQ(messages.size(), 0);

                auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
                auto dlqMsgs = dlq->getMessages();
                ASSERT_EQ(1, dlqMsgs.size());
                ASSERT_EQ(
                    "Failed to process input document in $https with error: Query "
                    "parameters defined in $https must have key-value pairs separated with a "
                    "'=' character non-empty keys.",
                    dlqMsgs.front()["errInfo"]["reason"].String());
                ASSERT_EQ("HttpsOperator", dlqMsgs.front()["operatorName"].String());
            },
            [](OperatorStats stats) {
                ASSERT_EQ(stats.numInputBytes, 0);
                ASSERT_EQ(stats.numOutputBytes, 0);
                ASSERT_EQ(stats.numDlqBytes, 318);
                ASSERT_EQ(stats.numDlqDocs, 1);
            });
    }
}

TEST_F(HttpsOperatorTest, GetWithBadHeaders) {
    StringData uri{"http://localhost:10000/"};
    auto expCtx = boost::intrusive_ptr<ExpressionContextForTest>(new ExpressionContextForTest{});


    // Test bad header value types
    {
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
            mongo::Expression::parseExpression(
                expCtx.get(),
                fromjson("{$arrayToObject: [[{k: \"age\", v: 91}]] }"),
                _context->expCtx->variablesParseState),
            // Array
            mongo::Expression::parseExpression(
                expCtx.get(),
                fromjson("{$objectToArray: { item: \"foo\", qty: 25}}"),
                _context->expCtx->variablesParseState),
            // BinData / UUID
            mongo::Expression::parseExpression(
                expCtx.get(),
                fromjson("{$toUUID: \"0e3b9063-8abd-4eb3-9f9f-f4c59fd30a60\"}"),
                _context->expCtx->variablesParseState),
            // ObjectId
            mongo::Expression::parseExpression(
                expCtx.get(),
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

            HttpsOperator::Options options{
                .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
                .method = HttpClient::HttpMethod::kGET,
                .url = uri.toString(),
                .operatorHeaders =
                    std::vector<std::pair<std::string, StringOrExpression>>{
                        {"badHeader", badHeader}},
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
                        "Failed to process input document in $https with error: "
                        "Expected "
                        "$https.headers values to evaluate to a string.",
                        dlqMsgs.front()["errInfo"]["reason"].String());
                    ASSERT_EQ("HttpsOperator", dlqMsgs.front()["operatorName"].String());
                },
                [](OperatorStats stats) {
                    ASSERT_EQ(stats.numInputBytes, 0);
                    ASSERT_EQ(stats.numOutputBytes, 0);
                    ASSERT_EQ(stats.numDlqBytes, 266);
                    ASSERT_EQ(stats.numDlqDocs, 1);
                });
        }
    }

    // Test malformed headers
    {
        std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();

        HttpsOperator::Options options{
            .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
            .method = HttpClient::HttpMethod::kGET,
            .url = uri.toString(),
            .operatorHeaders =
                std::vector<std::pair<std::string, StringOrExpression>>{
                    {"", "bar"}},  // header key cannot be empty
            .as = "response",
        };

        ASSERT_THROWS_WHAT(setupDag(std::move(options)),
                           DBException,
                           "Keys defined in $https.headers must not be empty.");
    }
}

TEST_F(HttpsOperatorTest, ShouldDLQOnErrorResponseByDefault) {
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

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .pathExpr = ExpressionFieldPath::parse(
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
                          "Failed to process input document in $https with error: Request "
                          "failure in $https with error: " +
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

TEST_F(HttpsOperatorTest, ShouldDLQOnErrorResponse) {
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

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .pathExpr = ExpressionFieldPath::parse(
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
                          "Failed to process input document in $https with "
                          "error: Request failure in $https with error: " +
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

TEST_F(HttpsOperatorTest, ShouldIgnoreOnErrorResponse) {
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

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kDELETE,
        .url = uri.toString(),
        .pathExpr = ExpressionFieldPath::parse(
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

                StreamMetaHttps expectedStreamMetaHttps;
                expectedStreamMetaHttps.setUrl(uri);
                expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodDelete);
                expectedStreamMetaHttps.setHttpStatusCode(400);
                assertStreamMetaHttps(expectedStreamMetaHttps,
                                      docBSON[*_context->streamMetaFieldName].Obj()["https"].Obj());
            }
        },
        [](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 24);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, 0);
            ASSERT_EQ(stats.numDlqDocs, 0);
        });
}

TEST_F(HttpsOperatorTest, FailOnError) {
    StringData uri{"http://localhost:10000/"};
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

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kGET,
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

TEST_F(HttpsOperatorTest, ShouldFailOnNon2XXStatus) {
    StringData uri{"http://localhost:10000/"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    auto rawMockHttpClient = mockHttpClient.get();

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kGET,
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

        testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                        [this, httpStatusCode, responseBody](std::deque<StreamMsgUnion> _) {
                            auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
                            ASSERT_EQ(dlq->numMessages(), 1);

                            auto dlqMsgs = dlq->getMessages();
                            auto dlqDoc = std::move(dlqMsgs.front());
                            ASSERT_EQ(dlqDoc["errInfo"]["reason"].String(),
                                      "Failed to process input document in $https with "
                                      "error: Request failure in $https with error: " +
                                          expectedErrResponseErrMsg(httpStatusCode, responseBody));
                        });
    }
}

TEST_F(HttpsOperatorTest, ShouldDLQOnUnsupportedResponseFormatByDefault) {
    StringData uri{"http://localhost:10000/"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = "<info>im xml</info>"});

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kGET,
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
            ASSERT_STRING_CONTAINS(dlqDoc["errInfo"]["reason"].String(),
                                   "Failed to process input document in $https with error: "
                                   "Failed to parse response body in $https with error:");
        },
        [&dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 19);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 1);
        });
}

TEST_F(HttpsOperatorTest, ShouldDLQOnUnsupportedResponseFormat) {
    StringData uri{"http://localhost:10000/"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = "<info>im xml</info>"});

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kGET,
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
            ASSERT_STRING_CONTAINS(dlqDoc["errInfo"]["reason"].String(),
                                   "Failed to process input document in $https with error: "
                                   "Failed to parse response body in $https with error:");
        },
        [&dlqMsgs](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 19);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, computeNumDlqBytes(dlqMsgs));
            ASSERT_EQ(stats.numDlqDocs, 1);
        });
}

TEST_F(HttpsOperatorTest, ShouldFailOnUnsupportedResponseFormatInHeader) {
    StringData uri{"http://localhost:10000/"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200,
                                 .header =
                                     std::vector<std::string>{"content-type: application/xml\r"},
                                 .body = "<info>im xml</info>"});

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",
        .onError = mongo::OnErrorEnum::Fail,
    };

    setupDag(std::move(options));

    ASSERT_THROWS(
        testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                        [](auto _) {}),
        DBException);
}

TEST_F(HttpsOperatorTest, ShouldFailOnUnsupportedResponseFormatInBody) {
    StringData uri{"http://localhost:10000/"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = "<info>im xml</info>"});

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kGET,
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

TEST_F(HttpsOperatorTest, ShouldIgnoreUnsupportedResponseFormat) {
    StringData uri{"http://localhost:10000/"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = "<info>im xml</info>"});

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kGET,
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
            ASSERT_TRUE(docBSON["response"].eoo());

            StreamMetaHttps expectedStreamMetaHttps;
            expectedStreamMetaHttps.setUrl(uri);
            expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodGet);
            expectedStreamMetaHttps.setHttpStatusCode(200);
            assertStreamMetaHttps(expectedStreamMetaHttps,
                                  docBSON[*_context->streamMetaFieldName].Obj()["https"].Obj());
        },
        [](OperatorStats stats) {
            ASSERT_EQ(stats.numInputBytes, 19);
            ASSERT_EQ(stats.numOutputBytes, 0);
            ASSERT_EQ(stats.numDlqBytes, 0);
            ASSERT_EQ(stats.numDlqDocs, 0);
        });
}

TEST_F(HttpsOperatorTest, ShouldSupportEmptyPayload) {
    StringData uri{"http://localhost:10000/"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kDELETE,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = ""});

    HttpsOperator::Options options{
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .method = HttpClient::HttpMethod::kDELETE,
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
                        ASSERT_TRUE(docBSON["response"].eoo());

                        StreamMetaHttps expectedStreamMetaHttps;
                        expectedStreamMetaHttps.setUrl(uri);
                        expectedStreamMetaHttps.setMethod(HttpMethodEnum::MethodDelete);
                        expectedStreamMetaHttps.setHttpStatusCode(200);
                        assertStreamMetaHttps(
                            expectedStreamMetaHttps,
                            docBSON[*_context->streamMetaFieldName].Obj()["https"].Obj());
                    });
}

TEST_F(HttpsOperatorTest, ShouldNotLogSameIDWithinAMinute) {
    int count{0};

    TickSourceMock<Milliseconds> tickSource;
    _oper = std::make_unique<HttpsOperator>(_context.get(),
                                            HttpsOperator::Options{
                                                .url = "http://localhost",
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

TEST_F(HttpsOperatorTest, ShouldLogDifferentIDWithinAMinute) {
    int count{0};

    TickSourceMock<Milliseconds> tickSource;
    _oper = std::make_unique<HttpsOperator>(_context.get(),
                                            HttpsOperator::Options{
                                                .url = "http://localhost",
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

TEST_F(HttpsOperatorTest, ParseResponseHeadersTest) {
    for (const auto& rawHeaders : std::vector<std::string>{
             "DataToIgnore\r\nContent-Type: application/json\r\n\r\n",
             "DataToIgnore\r\ncontent-type: Application/JSON; charset=utf-8\r\n\r\n"}) {
        auto result = parseContentTypeFromHeaders(rawHeaders);
        ASSERT_TRUE(result);
        ASSERT_EQ(*result, "application/json");
    }
}


// This block validates libcurl parsing + url building behavior.
TEST_F(HttpsOperatorTest, LibCurlUrlBuilding) {
    struct TestCase {
        std::string baseUrl;
        std::string path;
        std::vector<std::string> queryParams;
        std::string expectedUrl;
        std::string expectedErrorMessage;
    };

    std::vector<TestCase> testCases{
        // scheme tests
        {"foo.com", "", {}, "", "Parsing url failed."},         // missing scheme
        {"asdf://foo.com", "", {}, "", "Parsing url failed."},  // invalid scheme

        // username/password tests
        {"http://username@foo.com", "", {}, "http://username@foo.com/", ""},    // only username
        {"http://username:@foo.com", "", {}, "http://username@foo.com/", ""},   // only username
        {"http://:password@foo.com", "", {}, "http://:password@foo.com/", ""},  // only password
        {"http://username::password@foo.com", "", {}, "http://username::password@foo.com/", ""},

        // hostname tests
        {"http://192.168.1.1", "", {}, "http://192.168.1.1/", ""},    // ip hostname
        {"http://:32", "", {}, "http://:32", "Parsing url failed."},  // no hostname

        // port tests
        {"https://localhost:23", "", {}, "https://localhost:23/", ""},  // basic
        {"http://localhost:", "", {}, "http://localhost/", ""},         // no port

        // path tests
        {"http://foo.com", "a", {}, "http://foo.com/a", ""},
        {"http://foo.com/", "b", {}, "http://foo.com/b", ""},
        {"http://foo.com//c", "", {}, "http://foo.com//c", ""},
        {"http://foo.com//", "c", {}, "http://foo.com//c", ""},
        {"http://foo.com/a", "b/c", {}, "http://foo.com/a/b/c", ""},
        {"http://foo.com/d/", "/e/f", {}, "http://foo.com/d/e/f", ""},
        {"http://foo.com/d/", "/", {}, "http://foo.com/d", ""},
        {"http://foo.com/d/", "..", {}, "http://foo.com/d/..", ""},
        {"http://foo.com/a/b$$/c", "/d", {}, "http://foo.com/a/b%24%24/c/d", ""},

        // query tests
        {"https://foo.com?foo=bar", {}, {}, "https://foo.com/?foo=bar", ""},  // basic
        {"https://foo.com?=bar",
         "",
         {},
         "",
         "Query parameters defined in $https must have a non-empty key."},  // empty key
        {"https://foo.com",
         "",
         {"bar=baz"},
         "https://foo.com/?bar=baz",
         ""},  // appending query to empty base query
        {"https://foo.com?foo=bar",
         "",
         {"bar=baz"},
         "https://foo.com/?foo=bar&bar=baz",
         ""},  // appending query to non-empty base query

        // fragment tests
        {"https://foo.com/#", "", {}, "https://foo.com/", ""},             // empty fragment
        {"https://foo.com/#foo", "", {}, "https://foo.com/#foo", ""},      // basic fragment
        {"https://foo.com/#foo#", "", {}, "https://foo.com/#foo%23", ""},  // fragment containing #

        // happy path
        {"http://foo.com", "", {}, "http://foo.com/", ""},  // basic
        {"http://user:password@foo.com:4000/foo?bar=baz#header",
         "",
         {},
         "http://user:password@foo.com:4000/foo?bar=baz#header",
         ""},
    };

    for (const auto& tc : testCases) {
        LOGV2_INFO(9688097,
                   "Running test case",
                   "baseUrl"_attr = tc.baseUrl,
                   "path"_attr = tc.path,
                   "queryParams"_attr = tc.queryParams);
        if (tc.expectedErrorMessage.empty()) {
            auto actualUrl = createUrlFromParts(tc.baseUrl, tc.path, tc.queryParams);
            ASSERT_EQ(actualUrl, tc.expectedUrl);
        } else {
            ASSERT_THROWS_WHAT(createUrlFromParts(tc.baseUrl, tc.path, tc.queryParams),
                               DBException,
                               tc.expectedErrorMessage);
        }
    }
}

// Tests URL path joining behavior
TEST_F(HttpsOperatorTest, JoinPath) {
    struct TestCase {
        std::string basePath;
        std::string path;
        std::string expected;
    };

    std::vector<TestCase> testCases{
        {"", "", "/"},
        {"/", "/", "/"},
        {"/", "", "/"},
        {"", "/", "/"},
        {"/foo", "", "/foo"},
        {"", "/foo", "/foo"},
        {"/", "/foo", "/foo"},
        {"/", "foo", "/foo"},
        {"/foo", "/", "/foo"},
        {"/foo/", "/", "/foo"},
        {"/foo/", "//", "/foo/"},
        {"/foo/bar", "//", "/foo/bar/"},
        {"/foo//bar", "//", "/foo//bar/"},
        {"/foo/bar", "/baz", "/foo/bar/baz"},
        {"/foo/bar", "baz", "/foo/bar/baz"},
        {"/foo/bar", "//baz", "/foo/bar//baz"},
        {"/foo/bar", "..", "/foo/bar/.."},
        {"/foo/bar", "../...", "/foo/bar/../..."},
        {"/", "../...", "/../..."},
        {"//c", "foo", "//c/foo"},
        {"//c", "foo//bar", "//c/foo//bar"},
    };


    for (const auto& tc : testCases) {
        LOGV2_INFO(9688096, "Running test", "basePath"_attr = tc.basePath, "path"_attr = tc.path);
        auto actualUrl = HttpsOperator::joinPaths(tc.basePath, tc.path);
        ASSERT_EQ(actualUrl, tc.expected);
    }
}

#endif  // LIBCURL_VERSION_NUM > 0x074e00

};  // namespace streams
