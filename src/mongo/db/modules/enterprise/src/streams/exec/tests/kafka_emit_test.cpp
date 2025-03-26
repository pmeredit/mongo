/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <chrono>
#include <rdkafkacpp.h>

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/bsontypes_util.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_emit_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/util.h"
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
    // Wait for the source to be connected.
    while (!source.getConnectionStatus().isConnected()) {
        stdx::this_thread::sleep_for(stdx::chrono::milliseconds(100));
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
    auto [context, _] = getTestContext(getServiceContext());
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
    options.topicNames = {topicName};
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

void assertBinDataEquals(const BSONBinData& lhs, const BSONBinData& rhs) {
    ASSERT_EQ(lhs.type, rhs.type);
    ASSERT_EQ(lhs.length, rhs.length);
    auto lhsData = (const uint8_t*)lhs.data;
    auto rhsData = (const uint8_t*)rhs.data;
    for (int i = 0; i < lhs.length; ++i) {
        ASSERT_EQ(lhsData[i], rhsData[i]);
    }
}

TEST_F(KafkaEmitTest, TestSerializeHeaders) {
    auto [context, _] = getTestContext(getServiceContext());
    KafkaEmitOperator emitForTest{context.get(), {}};
    auto createHeaders = []() {
        auto headers = RdKafka::Headers::create();
        return headers;
    };
    auto headers = createHeaders();

    // Int
    emitForTest.serializeToHeaders(headers, "testing", "testKeyInt", Value(42));
    ASSERT_EQ(headers->size(), 1);
    auto header = headers->get("testKeyInt")[0];
    BSONBinData result{
        header.value(), static_cast<int>(header.value_size()), mongo::BinDataGeneral};
    std::vector<uint8_t> expectedBytes = {0x00, 0x00, 0x00, 0x2A};
    BSONBinData expectedBinData{
        expectedBytes.data(), static_cast<int>(expectedBytes.size()), mongo::BinDataGeneral};
    assertBinDataEquals(result, expectedBinData);

    // Long
    long long longVal = 200;
    emitForTest.serializeToHeaders(headers, "testing", "testKeyLong", Value(longVal));
    ASSERT_EQ(headers->size(), 2);
    header = headers->get("testKeyLong")[0];
    result =
        BSONBinData{header.value(), static_cast<int>(header.value_size()), mongo::BinDataGeneral};
    expectedBytes = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC8};
    expectedBinData = BSONBinData{
        expectedBytes.data(), static_cast<int>(expectedBytes.size()), mongo::BinDataGeneral};
    assertBinDataEquals(result, expectedBinData);

    // String
    std::string stringVal = "whatsup";
    emitForTest.serializeToHeaders(headers, "testing", "testKeyString", Value(stringVal));
    ASSERT_EQ(headers->size(), 3);
    header = headers->get("testKeyString")[0];
    result =
        BSONBinData{header.value(), static_cast<int>(header.value_size()), mongo::BinDataGeneral};
    expectedBinData =
        BSONBinData{"whatsup", static_cast<int>(stringVal.size()), mongo::BinDataGeneral};
    assertBinDataEquals(result, expectedBinData);

    // BinData
    BSONBinData binDataVal{"asdfghjkl", 5, mongo::BinDataGeneral};
    emitForTest.serializeToHeaders(headers, "testing", "testKeyBinData", Value(binDataVal));
    ASSERT_EQ(headers->size(), 4);
    header = headers->get("testKeyBinData")[0];
    result =
        BSONBinData{header.value(), static_cast<int>(header.value_size()), mongo::BinDataGeneral};
    assertBinDataEquals(result, binDataVal);

    // Object
    BSONObj objVal = BSON("x" << 8);
    emitForTest.serializeToHeaders(headers, "testing", "testKeyObj", Value(objVal));
    ASSERT_EQ(headers->size(), 5);
    header = headers->get("testKeyObj")[0];
    result =
        BSONBinData{header.value(), static_cast<int>(header.value_size()), mongo::BinDataGeneral};
    auto str = tojson(objVal, mongo::JsonStringFormat::ExtendedRelaxedV2_0_0, false);
    expectedBinData = BSONBinData{str.c_str(), static_cast<int>(str.size()), mongo::BinDataGeneral};
    assertBinDataEquals(result, expectedBinData);

    // Null / Missing
    emitForTest.serializeToHeaders(headers, "testing", "testKeyNull", Value(BSONNULL));
    ASSERT_EQ(headers->size(), 6);
    header = headers->get("testKeyNull")[0];
    result =
        BSONBinData{header.value(), static_cast<int>(header.value_size()), mongo::BinDataGeneral};
    expectedBinData = BSONBinData{"", 0, mongo::BinDataGeneral};
    assertBinDataEquals(result, expectedBinData);

    // Double
    double doubleVal = -2.5;
    emitForTest.serializeToHeaders(headers, "testing", "testKeyDouble", Value(doubleVal));
    ASSERT_EQ(headers->size(), 7);
    header = headers->get("testKeyDouble")[0];
    result =
        BSONBinData{header.value(), static_cast<int>(header.value_size()), mongo::BinDataGeneral};
    expectedBytes = {0xC0, 0x04, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0};
    expectedBinData = BSONBinData{
        expectedBytes.data(), static_cast<int>(expectedBytes.size()), mongo::BinDataGeneral};
    assertBinDataEquals(result, expectedBinData);

    delete headers;
}

TEST_F(KafkaEmitTest, TestDateFormat) {
    std::string isoString("2024-01-01T00:00:01.000Z");
    Date_t date = dateFromISOString(isoString).getValue();

    // basic case
    auto doc = Document(BSON("a" << 1 << "b" << date));
    ASSERT_VALUE_EQ(Value(doc), Value(doc));
    ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)),
                    Value(Document(BSON("a" << 1 << "b" << isoString))));

    // basic case 2
    doc = Document(BSON("b" << date));
    ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)), Value(Document(BSON("b" << isoString))));

    // basic case pre-1970
    Date_t date2 = dateFromISOString("1970-01-01T00:00:00.000Z").getValue() - mongo::Minutes{1};
    doc = Document(BSON("a" << 1 << "b" << date2));
    ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)),
                    Value(Document(BSON("a" << 1 << "b"
                                            << "1969-12-31T23:59:00.000Z"))));
    date2 = date2 - mongo::Days{365};
    doc = Document(BSON("a" << 1 << "b" << date2));
    ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)),
                    Value(Document(BSON("a" << 1 << "b"
                                            << "1968-12-31T23:59:00.000Z"))));
    date2 = date2 - 10 * mongo::Days{365};
    doc = Document(BSON("a" << 1 << "b" << date2));
    ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)),
                    Value(Document(BSON("a" << 1 << "b"
                                            << "1959-01-03T23:59:00.000Z"))));

    // no-op
    doc = Document(BSON("a" << 1 << "b"
                            << "foo"));
    ASSERT_VALUE_EQ(Value(doc), Value(doc));
    ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)), Value(doc));

    // nested doc
    doc = Document(BSON("a" << 1 << "b" << BSON("d" << date)));
    ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)),
                    Value(Document(BSON("a" << 1 << "b" << BSON("d" << isoString)))));

    // nested doc(doc)
    doc = Document(BSON("a" << 1 << "b"
                            << BSON("c" << BSON("d" << date << "e"
                                                    << "foo"))));
    ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)),
                    Value(Document(BSON("a" << 1 << "b"
                                            << BSON("c" << BSON("d" << isoString << "e"
                                                                    << "foo"))))));

    // nested doc(doc(doc))
    doc = Document(BSON("a" << 1 << "b"
                            << BSON("c" << BSON("d" << date << "e"
                                                    << "foo"
                                                    << "f" << BSON("g" << date << "h" << 1)))));
    ASSERT_VALUE_EQ(
        Value(convertDateToISO8601(doc)),
        Value(Document(
            BSON("a" << 1 << "b"
                     << BSON("c" << BSON("d" << isoString << "e"
                                             << "foo"
                                             << "f" << BSON("g" << isoString << "h" << 1)))))));

    // nested doc no-op
    doc = Document(BSON("a" << 1 << "b" << BSON("c" << BSON("e" << "foo"))));
    ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)), Value(doc));

    // nested array
    {
        auto arr = BSONArrayBuilder();
        arr.append(date);
        doc = Document(BSON("a" << 1 << "d" << arr.arr()));
        auto exArr = BSONArrayBuilder();
        exArr.append(isoString);
        ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)),
                        Value(Document(BSON("a" << 1 << "d" << exArr.arr()))));
    }

    // nested array no-op
    {
        auto arr = BSONArrayBuilder();
        arr.append(2);
        doc = Document(BSON("a" << 1 << "d" << arr.arr()));
        auto exArr = BSONArrayBuilder();
        exArr.append(isoString);
        ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)), Value(doc));
    }

    // nested array with multiple values
    {
        auto arr = BSONArrayBuilder();
        arr.append(date);
        arr.append("foo");
        arr.append(1);
        arr.append(date);
        doc = Document(BSON("a" << 1 << "d" << arr.arr()));
        auto exArr = BSONArrayBuilder();
        exArr.append(isoString);
        exArr.append("foo");
        exArr.append(1);
        exArr.append(isoString);
        ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)),
                        Value(Document(BSON("a" << 1 << "d" << exArr.arr()))));
    }

    // nested array(doc)
    {
        auto arr = BSONArrayBuilder();
        arr.append(date);
        arr.append("foo");
        arr.append(1);
        arr.append(BSON("d" << 2 << "e" << date));
        doc = Document(BSON("a" << 1 << "d" << arr.arr()));
        auto exArr = BSONArrayBuilder();
        exArr.append(isoString);
        exArr.append("foo");
        exArr.append(1);
        exArr.append(BSON("d" << 2 << "e" << isoString));
        ASSERT_VALUE_EQ(Value(convertDateToISO8601(doc)),
                        Value(Document(BSON("a" << 1 << "d" << exArr.arr()))));
    }

    // nested array(doc(array(doc)))
    {
        auto build = [&](bool useDate) {
            auto appendDate = [&](BSONArrayBuilder& b) {
                if (useDate) {
                    b.append(date);
                } else {
                    b.append(isoString);
                }
            };
            auto appendDateToObj = [&](BSONObjBuilder& b, const std::string& name) {
                if (useDate) {
                    b.appendDate(name, date);
                } else {
                    b.append(name, isoString);
                }
            };

            auto arr2 = BSONArrayBuilder();
            arr2.append("foo");
            arr2.append(1);
            appendDate(arr2);
            auto builder = BSONObjBuilder();
            builder.append("f", 1);
            appendDateToObj(builder, "g");
            builder.append("arr2", arr2.arr());

            auto arr = BSONArrayBuilder();
            appendDate(arr);
            auto builder2 = BSONObjBuilder();
            builder2.append("d", 2);
            appendDateToObj(builder2, "g");
            arr.append(builder2.obj());

            auto b = BSONObjBuilder();
            b.append("a", 1);
            b.append("d", arr.arr());
            appendDateToObj(b, "f");
            return Document(b.obj());
        };
        ASSERT_VALUE_EQ(Value(convertDateToISO8601(build(true))), Value(build(false)));
    }
}

}  // namespace streams
