/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <chrono>
#include <rdkafkacpp.h>

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_emit_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

using namespace mongo;

namespace streams {

using KafkaEmitTest = AggregationContextFixture;

int randomInt(mongo::PseudoRandom& random, int min, int max) {
    dassert(max > min);
    // nextInt32 returns a half open [0, max) interval so we add 1 to max.
    auto randomValue = random.nextInt32(max + 1 - min);
    return randomValue + min;
}

Document generateSolarDataDoc(mongo::PseudoRandom& random, Date_t timestamp) {
    const int maxWatts = 250;
    std::string deviceId(fmt::format("device_{}", randomInt(random, 0, 10)));
    int groupId = randomInt(random, 0, 10);
    // The maximum values of watts and temp are halved from 00:00-12:00 UTC time.
    int watts = randomInt(random, 0, maxWatts);
    int temp = randomInt(random, 5, 25);
    // Error has .5% change, i.e. 1 out of 200 change.
    bool error = randomInt(random, 1, 200) == 1;

    SampleDataSourceSolarSpec sampleDataSpec{
        deviceId, groupId, timestamp.toString(), maxWatts, error ? 1 : 0};
    if (error) {
        sampleDataSpec.setEvent_details(StringData{"Network error"});
    } else {
        sampleDataSpec.setObs(SampleDataSourceSolarSpecObs{watts, temp});
    }

    return Document(std::move(sampleDataSpec.toBSON()));
}

auto readNumDocs(Context* context,
                 KafkaConsumerOperator::Options options,
                 size_t numDocs,
                 stdx::chrono::milliseconds idleTimeout = std::chrono::milliseconds{60 * 1000}) {
    KafkaConsumerOperator source{context, options};
    InMemorySinkOperator sink{context, 1};
    source.addOutput(&sink, 0);
    source.start();
    source.connect();
    // Wait for the source to be connected.
    while (!source.getConnectionStatus().isConnected()) {
        stdx::this_thread::sleep_for(stdx::chrono::milliseconds(100));
        source.connect();
    }
    auto lastActiveTimestamp = stdx::chrono::steady_clock().now();
    std::vector<StreamDocument> allResults;
    while (allResults.size() < numDocs &&
           stdx::chrono::steady_clock().now() - lastActiveTimestamp < idleTimeout) {
        source.runOnce();
        auto results = sink.getMessages();
        while (!results.empty()) {
            lastActiveTimestamp = stdx::chrono::steady_clock().now();
            if (results.front().dataMsg) {
                const auto& docs = results.front().dataMsg->docs;
                allResults.insert(allResults.end(), docs.begin(), docs.end());
            }
            results.pop_front();
        }
    }
    source.stop();
    return allResults;
};

auto removeProjections(std::vector<StreamDocument> docs) {
    for (auto& doc : docs) {
        MutableDocument mut{std::move(doc.doc)};
        mut.remove("_ts");
        mut.remove("_stream_meta");
        doc.doc = mut.freeze();
    }
    return docs;
}

// This test uses KafkaEmitOperator to write 100k documents to 12 different Kafka
// partitions. The test then uses KafkaConsumerOperator to validate the same
// documents are read. Currently this test cannot run in evergreen.
TEST_F(KafkaEmitTest, RoundTrip) {
    std::string localKafkaBrokers{""};
    // Note: this is not currently used in evergreen, just for local testing.
    // If this environment variable is set we point the test at an actual Kafka broker.
    if (const char* localBroker = std::getenv("KAFKA_TEST_BROKERS")) {
        localKafkaBrokers = localBroker;
    } else {
        // Test does not run unless there is a local kafka broker running.
        return;
    }

    // Setup the test context and input parameters.
    auto metricManager = std::make_unique<MetricManager>();
    auto context = getTestContext(getServiceContext(), metricManager.get());
    mongo::PseudoRandom random(42);
    const std::string topicName = UUID::gen().toString();
    // TODO(matthew): Make it work for 12 partitions.
    const int partitionCount = 1;
    const int64_t sizePerPartition = 100'000;
    stdx::unordered_map<int32_t, std::vector<StreamDocument>> expectedOutput;
    // Write input to each partition.
    for (int32_t partition = 0; partition < partitionCount; ++partition) {
        std::vector<StreamDocument> input;
        for (int64_t i = 0; i < sizePerPartition; ++i) {
            input.push_back(StreamDocument{generateSolarDataDoc(random, Date_t::now())});
        }
        KafkaEmitOperator emitForTest{context.get(),
                                      {.bootstrapServers = localKafkaBrokers,
                                       .topicName = topicName,
                                       .testOnlyPartition = partition}};
        emitForTest.start();
        emitForTest.onDataMsg(0, StreamDataMsg{.docs = input});
        emitForTest.stop();
        expectedOutput[partition] = std::move(input);
    }

    // Create a KafkaConsumerOperator to read what we just wrote.
    auto deserializer = std::make_unique<JsonEventDeserializer>();
    KafkaConsumerOperator::Options options;
    options.bootstrapServers = localKafkaBrokers;
    options.topicName = topicName;
    options.startOffset = RdKafka::Topic::OFFSET_BEGINNING;
    options.deserializer = deserializer.get();
    options.timestampOutputFieldName = "_ts";
    int64_t expectedOutputSize = sizePerPartition * partitionCount;
    auto results = readNumDocs(context.get(), options, expectedOutputSize);
    stdx::unordered_map<int32_t, std::vector<StreamDocument>> actualOutput;
    for (auto& doc : results) {
        int32_t partition = doc.doc["_stream_meta"]["sourcePartition"].getInt();
        actualOutput[partition].push_back(std::move(doc));
    }

    ASSERT_EQ(expectedOutput.size(), actualOutput.size());
    ASSERT_EQ(results.size(), expectedOutputSize);
    for (auto& [partition, docs] : expectedOutput) {
        auto output = removeProjections(actualOutput[partition]);
        for (size_t i = 0; i < docs.size(); ++i) {
            auto expected = docs[i].doc.toBson();
            auto actual = output[i].doc.toBson();
            ASSERT_BSONOBJ_EQ(expected, actual);
        }
    }
}

}  // namespace streams
