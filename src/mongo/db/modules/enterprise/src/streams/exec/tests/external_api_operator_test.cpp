/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <deque>
#include <fmt/core.h>
#include <fmt/format.h>
#include <functional>
#include <memory>

#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/assert.h"
#include "mongo/util/net/http_client_mock.h"
#include "streams/exec/external_api_operator.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/planner.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {

using namespace mongo;

class ExternalApiOperatorTest : public AggregationContextFixture {
public:
    ExternalApiOperatorTest() : AggregationContextFixture() {
        _context = std::get<0>(getTestContext(/*svcCtx*/ nullptr));
    }

    void doTest(ExternalApiOperator::Options opts,
                std::function<void(const std::deque<StreamMsgUnion>&)> testAssert) {
        auto oper = std::make_unique<ExternalApiOperator>(_context.get(), std::move(opts));
        auto sink = std::make_unique<InMemorySinkOperator>(_context.get(), /*numInputs*/ 1);

        // Build DAG.
        oper->addOutput(sink.get(), 0);
        oper->start();
        sink->start();

        // Flow data through the DAG.
        StreamDataMsg inputDataMsg;
        std::vector<StreamDocument> inputDocs;
        // Only process a single document here because each request consumes an expectation on the
        // mocked http client. We don't have a good way of handling this until we have the ability
        // to hit different URLs using the same operator. Being able to evaluate config.urlPath or
        // config.params will allow us to do this for better testing.
        //
        // TODO(SERVER-95031): Add query params support.
        inputDocs.reserve(1);
        inputDocs.emplace_back(
            Document{fromjson(fmt::format("{{_ts: {}, id: {}, val: {}}}", 1, 1, 1))});

        inputDataMsg.docs = std::move(inputDocs);

        // Send data message to operator and let process + flow to sink.
        oper->onDataMsg(0, inputDataMsg);
        auto messages = sink->getMessages();

        testAssert(messages);
    }

protected:
    std::unique_ptr<Context> _context;
};

TEST_F(ExternalApiOperatorTest, BasicGet) {
    StringData uri{"http://localhost:10000"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200,
                                 .body = tojson(BSON("ack"
                                                     << "ok"))});

    ExternalApiOperator::Options options{
        .uri = uri.toString(),
        .requestType = HttpClient::HttpMethod::kGET,
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .as = "response",
    };

    doTest(std::move(options), [](std::deque<StreamMsgUnion> messages) {
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

TEST_F(ExternalApiOperatorTest, BasicGetWithDottedAsField) {
    StringData uri{"http://localhost:10001"};
    // Set up mock http client.
    std::unique_ptr<MockHttpClient> mockHttpClient = std::make_unique<MockHttpClient>();
    mockHttpClient->expect(
        MockHttpClient::Request{
            HttpClient::HttpMethod::kGET,
            uri.toString(),
        },
        MockHttpClient::Response{.code = 200, .body = tojson(BSON("year" << 2024))});

    // Create $externalAPI operator.
    ExternalApiOperator::Options options{
        .uri = uri.toString(),
        .requestType = HttpClient::HttpMethod::kGET,
        .httpClient = std::unique_ptr<mongo::HttpClient>(std::move(mockHttpClient)),
        .as = "apiResponse.inner",
    };

    doTest(std::move(options), [](std::deque<StreamMsgUnion> messages) {
        ASSERT_EQ(messages.size(), 1);
        auto msg = messages.at(0);

        ASSERT(msg.dataMsg);
        ASSERT(!msg.controlMsg);
        ASSERT_EQ(msg.dataMsg->docs.size(), 1);

        auto doc = msg.dataMsg->docs[0].doc.toBson();
        auto response = doc["apiResponse"].Obj();
        ASSERT_TRUE(!response.isEmpty());

        auto inner = response["inner"].Obj();
        ASSERT_TRUE(!inner.isEmpty());

        auto year = inner["year"];
        ASSERT_TRUE(year.ok());
        ASSERT_EQ(year.Int(), 2024);
    });
}

// TODO(SERVER-95481): Add failure case test where we DLQ a a document once we can use the planner
// to create the pipeline

};  // namespace streams
