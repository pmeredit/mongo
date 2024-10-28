/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <deque>
#include <fmt/core.h>
#include <fmt/format.h>
#include <functional>
#include <memory>
#include <vector>

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/assert.h"
#include "mongo/util/net/http_client.h"
#include "mongo/util/net/http_client_mock.h"
#include "mongo/util/system_tick_source.h"
#include "mongo/util/tick_source_mock.h"
#include "streams/exec/external_api_operator.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/planner.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/tests/test_utils.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

class ExternalApiOperatorTest : public AggregationContextFixture {
public:
    ExternalApiOperatorTest() : AggregationContextFixture() {
        _context = std::get<0>(getTestContext(/*svcCtx*/ nullptr));
    }

    void setupDag(ExternalApiOperator::Options opts) {
        _oper = std::make_unique<ExternalApiOperator>(_context.get(), std::move(opts));
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

    void stopDag() {
        _oper->stop();
        _sink->stop();
    }

    void doLoadTest(int iterations,
                    Microseconds minTestDuration,
                    std::function<void()> setup,
                    std::function<void(const std::deque<StreamMsgUnion>&)> testAssert) {
        Timer timer{};
        auto start = timer.elapsed();

        for (int i = 0; i < iterations; i++) {
            setup();
            testAgainstDocs(std::vector<StreamDocument>{Document{fromjson("{'path': '/foobar'}")}},
                            testAssert);
        }

        ASSERT_GREATER_THAN_OR_EQUALS(timer.elapsed() - start, minTestDuration);
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

protected:
    std::unique_ptr<Context> _context;

    std::unique_ptr<ExternalApiOperator> _oper;
    std::unique_ptr<InMemorySinkOperator> _sink;
};

struct ExternalApiOpereratorTestCase {
    const std::string description;
    const std::function<ExternalApiOperator::Options()> optionsFn;
    const std::vector<StreamDocument> inputDocs;
    const std::function<void(const std::deque<StreamMsgUnion>&)> assertFn;
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
            [](std::deque<StreamMsgUnion> messages) {
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
                }
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
            [](std::deque<StreamMsgUnion> messages) {
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
                }
            },
        },
    };

    for (const auto& tc : tests) {
        LOGV2_DEBUG(9503599, 1, "Running test case", "description"_attr = tc.description);
        setupDag(tc.optionsFn());
        testAgainstDocs(tc.inputDocs, tc.assertFn);
        stopDag();
    }
}

TEST_F(ExternalApiOperatorTest, IsThrottledWithDefaultThrottleFnAndTimer) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    auto rawMockHttpClient = mockHttpClient.get();

    updateRateLimitPerSecond(1);

    ExternalApiOperator::Options options{
        .httpClient = std::move(mockHttpClient),
        .requestType = HttpClient::HttpMethod::kGET,
        .url = uri.toString(),
        .as = "response",
    };

    setupDag(std::move(options));

    doLoadTest(
        5,
        Seconds(4),
        [rawMockHttpClient, uri]() {
            rawMockHttpClient->expect(
                MockHttpClient::Request{
                    HttpClient::HttpMethod::kGET,
                    uri.toString(),
                },
                MockHttpClient::Response{.code = 200,
                                         .body = tojson(BSON("ack"
                                                             << "ok"))});
        },
        [](std::deque<StreamMsgUnion> messages) {
            ASSERT_EQ(messages.size(), 1);
            auto msg = messages.at(0);

            ASSERT(msg.dataMsg);
            ASSERT(!msg.controlMsg);
            ASSERT_EQ(msg.dataMsg->docs.size(), 1);

            auto doc = msg.dataMsg->docs[0].doc.toBson();
            auto response = doc["response"].Obj();
            ASSERT_TRUE(!response.isEmpty());

            auto ack = response["ack"];
            ASSERT_TRUE(ack.ok());
            ASSERT_EQ(ack.String(), "ok");
        });
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

    auto testAssert = [](std::deque<StreamMsgUnion> messages) {
        ASSERT_EQ(messages.size(), 1);
        auto msg = messages.at(0);

        ASSERT(msg.dataMsg);
        ASSERT(!msg.controlMsg);
        ASSERT_EQ(msg.dataMsg->docs.size(), 1);

        auto doc = msg.dataMsg->docs[0].doc.toBson();
        auto response = doc["response"].Obj();
        ASSERT_TRUE(!response.isEmpty());

        auto ack = response["ack"];
        ASSERT_TRUE(ack.ok());
        ASSERT_EQ(ack.String(), "ok");
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

    auto testAssert = [](std::deque<StreamMsgUnion> messages) {
        ASSERT_EQ(messages.size(), 1);
        auto msg = messages.at(0);

        ASSERT(msg.dataMsg);
        ASSERT(!msg.controlMsg);
        ASSERT_EQ(msg.dataMsg->docs.size(), 1);

        auto doc = msg.dataMsg->docs[0].doc.toBson();
        auto response = doc["response"].Obj();
        ASSERT_TRUE(!response.isEmpty());

        auto ack = response["ack"];
        ASSERT_TRUE(ack.ok());
        ASSERT_EQ(ack.String(), "ok");
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

// TODO(SERVER-95032): Add failure case test where we DLQ a a document once we can use the
// planner to create the pipeline
};  // namespace streams
