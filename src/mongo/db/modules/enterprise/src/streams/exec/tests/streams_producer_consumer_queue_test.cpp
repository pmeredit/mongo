/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/stdx/thread.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "streams/exec/message.h"
#include "streams/exec/producer_consumer_queue.h"
#include <fmt/format.h>
#include <memory>

namespace streams {

using namespace mongo;

class StreamsProducerConsumerQueueTest : public unittest::Test {
public:
    using StreamDataMsgPCQ = ProducerConsumerQueue<StreamDataMsg>;

    void createProducerConsumerQueue(StreamDataMsgPCQ::Options options) {
        _options = options;
        _producerConsumerQueue = std::make_unique<StreamDataMsgPCQ>(_options);
    }

    void setInputDataMessages(std::vector<StreamDataMsg> inputDataMsgs) {
        _inputDataMsgs = inputDataMsgs;
    }

    void startProducer() {
        auto producerT = [&]() {
            for (size_t i = 0; i < _inputDataMsgs.size(); i++) {
                auto status = _producerConsumerQueue->push(_inputDataMsgs.at(i));
                ASSERT_TRUE(status.isOK());
            }

            auto status = _producerConsumerQueue->waitForEmpty();
            ASSERT_TRUE(status.isOK());
            ASSERT_EQ(_producerConsumerQueue->_currentSize, 0);
            ASSERT_EQ(_producerConsumerQueue->_currentSizeBytes, 0);
            ASSERT_TRUE(_producerConsumerQueue->isEmpty());

            _producerConsumerQueue->shutDown();
        };
        _producerThread = std::make_unique<mongo::stdx::thread>(producerT);
    }

    void startConsumer() {
        auto consumerT = [&]() {
            bool done = false;
            int count = 0;
            while (!done) {
                auto nextEntryCost = _producerConsumerQueue->peekCost();
                auto msg = _producerConsumerQueue->pop();
                if (!msg) {
                    ASSERT_TRUE(_producerConsumerQueue->isEmpty());
                    ASSERT_TRUE(!nextEntryCost);
                    break;
                }
                if (nextEntryCost) {
                    ASSERT_EQ(nextEntryCost, _options.costFunc(*msg));
                }
                ASSERT_TRUE((size_t)count < _inputDataMsgs.size());
                int docCount = _inputDataMsgs[count].docs.size();
                ASSERT_EQUALS((*msg).docs.size(), (size_t)docCount);
                for (int i = 0; i < docCount; i++) {
                    ASSERT_VALUE_EQ(Value(count), (*msg).docs[i].doc.getField("a"));
                    ASSERT_VALUE_EQ(Value(i), (*msg).docs[i].doc.getField("b"));
                }
                count++;
            }
            ASSERT_TRUE((size_t)count == _inputDataMsgs.size());
            ASSERT_EQ(_producerConsumerQueue->_currentSize, 0);
            ASSERT_EQ(_producerConsumerQueue->_currentSizeBytes, 0);

            _producerConsumerQueue->shutDown();
        };
        _consumerThread = std::make_unique<mongo::stdx::thread>(consumerT);
    }

    void waitForMaxQueueSize() {
        while (true) {
            if (_producerConsumerQueue->_currentSize >= _options.maxQueueSize ||
                _producerConsumerQueue->_currentSizeBytes >= _options.maxQueueSizeBytes) {
                break;
            }
            sleep(1);
        }
    }

    void joinProducerConsumer() {
        if (_producerThread->joinable()) {
            _producerThread->join();
        }

        if (_consumerThread->joinable()) {
            _consumerThread->join();
        }
    }

private:
    std::unique_ptr<StreamDataMsgPCQ> _producerConsumerQueue;
    StreamDataMsgPCQ::Options _options;
    std::unique_ptr<mongo::stdx::thread> _producerThread;
    std::unique_ptr<mongo::stdx::thread> _consumerThread;
    std::vector<StreamDataMsg> _inputDataMsgs;
};

// BasicOperationTest does the basic validation of the producer-consumer queue with different
// variations of queue sizes.
TEST_F(StreamsProducerConsumerQueueTest, BasicOperationTest) {
    auto runTest = [&](int docCount,
                       int msgCount,
                       int64_t maxQueueSize,
                       int64_t maxQueueSizeBytes) {
        auto costFunc = [](const StreamDataMsg& msg) -> ProducerConsumerQueue<StreamDataMsg>::Cost {
            return {.entrySize = static_cast<int64_t>(msg.docs.size()),
                    .entrySizeBytes = msg.getByteSize()};
        };

        createProducerConsumerQueue(
            ProducerConsumerQueue<StreamDataMsg>::Options{.costFunc = costFunc,
                                                          .maxQueueSize = maxQueueSize,
                                                          .maxQueueSizeBytes = maxQueueSizeBytes});
        // Populate the data for the producer thread.
        std::vector<StreamDataMsg> inputDataMsgs;
        for (int i = 0; i < msgCount; i++) {
            inputDataMsgs.emplace_back();
            for (int j = 0; j < docCount; j++) {
                inputDataMsgs.back().docs.emplace_back(
                    Document(fromjson(fmt::format("{{a: {}, b: {}}}", i, j))));
            }
        }
        setInputDataMessages(inputDataMsgs);
        startProducer();
        startConsumer();
        joinProducerConsumer();
    };
    runTest(1, 10, 10, 1000);
    runTest(10, 10, 50, 1500);
    runTest(1, 100, 10, 1000);
    runTest(0, 0, 10, 1000);
    runTest(20, 100, 50, 3000);
    runTest(1, 100, 50, 500);
}

// The CostFuncCalculatorTest validates the correctness of the CostFunc logic in the
// ProducerConsumerQueue.
TEST_F(StreamsProducerConsumerQueueTest, CostFuncCalculatorTest) {
    auto costFunc = [](const StreamDataMsg& msg) -> ProducerConsumerQueue<StreamDataMsg>::Cost {
        return {.entrySize = static_cast<int64_t>(msg.docs.size()),
                .entrySizeBytes = msg.getByteSize()};
    };
    int64_t queueSize = 0;
    int64_t queueSizeBytes = 0;
    int docCount = 10;
    int msgCount = 100;
    std::vector<StreamDataMsg> inputDataMsgs;
    for (int i = 0; i < msgCount; i++) {
        inputDataMsgs.emplace_back();
        for (int j = 0; j < docCount; j++) {
            inputDataMsgs.back().docs.emplace_back(
                Document(fromjson(fmt::format("{{a: {}, b: {}}}", i, j))));
        }
        queueSize += inputDataMsgs.back().docs.size();
        queueSizeBytes += inputDataMsgs.back().getByteSize();
    }

    createProducerConsumerQueue(ProducerConsumerQueue<StreamDataMsg>::Options{
        .costFunc = costFunc, .maxQueueSize = queueSize, .maxQueueSizeBytes = queueSizeBytes});
    setInputDataMessages(inputDataMsgs);
    startProducer();
    // Wait for the producer thread to reach capacity to validate that the cost function is doing
    // its job.
    waitForMaxQueueSize();
    startConsumer();
    joinProducerConsumer();
}

// This test is to validate that we can insert a single large entry (larger than the queue size).
TEST_F(StreamsProducerConsumerQueueTest, LargeDataMessage) {
    auto costFunc = [](const StreamDataMsg& msg) -> ProducerConsumerQueue<StreamDataMsg>::Cost {
        return {.entrySize = static_cast<int64_t>(msg.docs.size()),
                .entrySizeBytes = msg.getByteSize()};
    };
    int64_t queueSize = 0;
    int64_t queueSizeBytes = 0;
    int docCount = 10;
    int msgCount = 100;
    std::vector<StreamDataMsg> inputDataMsgs;
    for (int i = 0; i < msgCount; i++) {
        inputDataMsgs.emplace_back();
        for (int j = 0; j < docCount; j++) {
            inputDataMsgs.back().docs.emplace_back(
                Document(fromjson(fmt::format("{{a: {}, b: {}}}", i, j))));
        }
        queueSize += inputDataMsgs.back().docs.size();
        queueSizeBytes += inputDataMsgs.back().getByteSize();
    }

    auto runTest = [&](int64_t maxQueueSize, int64_t maxQueueSizeBytes) {
        createProducerConsumerQueue(
            ProducerConsumerQueue<StreamDataMsg>::Options{.costFunc = costFunc,
                                                          .maxQueueSize = maxQueueSize,
                                                          .maxQueueSizeBytes = maxQueueSizeBytes});
        setInputDataMessages(inputDataMsgs);
        startProducer();
        startConsumer();
        joinProducerConsumer();
    };

    runTest(queueSize / 2, queueSizeBytes);
    runTest(queueSize, queueSizeBytes / 2);
    runTest(queueSize / 2, queueSizeBytes / 2);
}

}  // namespace streams
