/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/s3_emit_operator.h"

#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListBucketsResult.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <bsoncxx/json.hpp>
#include <chrono>
#include <fmt/core.h>
#include <memory>
#include <sstream>
#include <tuple>
#include <vector>

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/executor.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/planner.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/exception.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {
namespace {

using namespace mongo;

class S3EmitOperatorTest : public mongo::AggregationContextFixture {
public:
    S3EmitOperatorTest() {
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);

        _tsExtractor = std::make_unique<DocumentTimestampExtractor>(
            _context->expCtx,
            Expression::parseExpression(&(*_context->expCtx),
                                        fromjson("{$convert: { input: '$_ts', to: 'date' }}"),
                                        _context->expCtx->variablesParseState));
    }

    void setupDag(
        std::shared_ptr<S3Client> client,
        std::function<S3EmitOperator::Options(std::shared_ptr<S3Client> client)> optionsFn,
        bool shouldBeConnected) {
        _source = std::make_unique<InMemorySourceOperator>(
            _context.get(),
            InMemorySourceOperator::Options{SourceOperator::Options{
                .timestampExtractor = _tsExtractor.get(),
            }});
        auto options = optionsFn(client);
        options.testOnlyTimestampExtractor = _tsExtractor.get();
        options.testOnlyTaskIdSeed = 1234;
        _sink = std::make_unique<S3EmitOperator>(_context.get(), std::move(options));
        _sink->registerMetrics(_executor->getMetricManager());

        // Build DAG.
        _source->addOutput(_sink.get(), 0);
        _source->start();
        _sink->start();

        int numRetries = 0;
        while (_sink->getConnectionStatus().isConnecting()) {
            stdx::this_thread::sleep_for(stdx::chrono::milliseconds(500));
            if (++numRetries >= 5) {
                break;
            }
        }
        if (shouldBeConnected) {
            ASSERT(_sink->getConnectionStatus().isConnected());
        } else {
            ASSERT_THROWS(_sink->getConnectionStatus().throwIfNotConnected(), SPException);
        }
    }

    void stopDag() {
        _source->stop();
        _sink->stop();
    }

    void testFailure(std::shared_ptr<S3Client> client, std::vector<StreamDocument> inputDocs) {
        // This data message should cause the S3 emit consumer thread to exit.
        _source->addDataMsg({std::move(inputDocs)}, boost::none);
        _source->runOnce();

        auto func = [&]() {
            // When the S3 emit consumer thread goes down, the sink operator will throw an exception
            // if it is not connected when processing data. We'll keep inserting data until that
            // happens.
            for (int i = 0; i < 10; i++) {
                std::vector<StreamDocument> moreData;
                moreData.reserve(1);
                moreData.push_back(Document{
                    fromjson(R"({ "_ts" : "2023-04-10T18:00:00.000000", "foo" : "bar"})")});

                _source->addDataMsg({moreData}, boost::none);
                _source->runOnce();
                stdx::this_thread::sleep_for(stdx::chrono::seconds(1));
            }
        };

        ASSERT_THROWS(func(), SPException);
    }

    void testSuccess(std::shared_ptr<S3Client> client,
                     std::vector<StreamDocument> inputDocs,
                     std::function<std::string(std::shared_ptr<S3Client>)> getFileFn,
                     std::string expectedFile) {
        // Send data to the source. Sink writer thread processes it asynchronously.
        _source->addDataMsg({inputDocs}, boost::none);
        _source->runOnce();

        // Wait for writer thread to write file to mocked S3.
        std::string file;
        for (int i = 0; i < 5; i++) {
            file = getFileFn(client);
            if (file.size() > 0) {
                break;
            }

            stdx::this_thread::sleep_for(stdx::chrono::seconds(1));
        }
        ASSERT(!file.empty());
        ASSERT_EQ(file, expectedFile);
    }

protected:
    std::unique_ptr<Context> _context;
    std::unique_ptr<Executor> _executor;

    std::unique_ptr<DocumentTimestampExtractor> _tsExtractor;
    std::unique_ptr<InMemorySourceOperator> _source;
    std::unique_ptr<S3EmitOperator> _sink;
};

/**
 * MockS3Client mocks calls to the AWS S3 service. It hold a small in-memory version of buckets and
 * objects contained with them.
 *
 * If provided seed values for ListBucket or PutObject, the client will consume those seeded values
 * in order of insertion.
 */
class MockS3Client : public S3Client {
public:
    struct PutObjectRequest {
        std::string bucket;
        std::string body;
    };

    void seedListBucketOutcome(Aws::S3::Model::ListBucketsOutcome outcome) {
        _listBucketSeededOutcomes.push_back(std::move(outcome));
    }

    Aws::S3::Model::ListBucketsOutcome ListBuckets(
        const Aws::S3::Model::ListBucketsRequest& request) override {
        // Return seeded outcomes first, if any.
        if (_listBucketSeededOutcomes.size() > 0) {
            auto out = _listBucketSeededOutcomes.back();
            _listBucketSeededOutcomes.pop_back();
            return out;
        }
        Aws::S3::S3Error error{};
        error.SetMessage("unseeded mock ListBuckets response");
        return error;
    }

    void seedPutObjectAsyncOutcome(Aws::S3::Model::PutObjectOutcome outcome) {
        _putObjectSeededOutcomes.push_back(outcome);
    }

    void PutObjectAsync(const Aws::S3::Model::PutObjectRequest& request,
                        const Aws::S3::PutObjectResponseReceivedHandler& cb) override {
        // Return seeded outcomes first, if any.
        if (_putObjectSeededOutcomes.size() > 0) {
            auto seededOutcome = _putObjectSeededOutcomes.back();
            _putObjectSeededOutcomes.pop_back();
            cb(nullptr, request, std::move(seededOutcome), nullptr);
        }

        auto bucketName = request.GetBucket();
        if (!_filesByBucket.contains(bucketName)) {
            _filesByBucket[bucketName] = {};
        }

        auto key = request.GetKey();
        if (_filesByBucket.contains(key)) {
            Aws::S3::S3Error err;
            err.SetMessage(fmt::format(
                "Bucket '{}' already contains an object with specified key '{}'", bucketName, key));
            cb(nullptr, request, std::move(err), nullptr);
        }

        auto requestBody = request.GetBody();
        std::stringstream newObject;
        newObject << requestBody->rdbuf();
        _filesByBucket[bucketName][key] = newObject.str();

        Aws::S3::Model::PutObjectResult result;
        cb(nullptr, request, std::move(result), nullptr);
    }

    Aws::S3::Model::GetObjectOutcome GetObject(const Aws::S3::Model::GetObjectRequest& request) {
        std::string bucketName = request.GetBucket();

        auto bucket = _filesByBucket.find(bucketName);
        if (bucket == _filesByBucket.end()) {
            Aws::S3::S3Error out{};
            out.SetMessage(fmt::format("mock client did not find bucket '{}'", bucketName));
            return out;
        }

        std::string key = request.GetKey();
        auto files = bucket->second;
        auto file = files.find(key);
        if (file == files.end()) {
            Aws::S3::S3Error out{};
            out.SetMessage(
                fmt::format("mock client did not find an object with key '{}' in bucket '{}'",
                            key,
                            bucketName));
            return out;
        }

        Aws::S3::Model::GetObjectResult result;
        auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
        *responseBody << std::move(file->second);
        result.ReplaceBody(responseBody);
        files.erase(key);

        if (files.size() == 0) {
            _filesByBucket.erase(bucketName);
        }

        return result;
    }

private:
    std::vector<Aws::S3::Model::ListBucketsOutcome> _listBucketSeededOutcomes;
    std::vector<Aws::S3::Model::PutObjectOutcome> _putObjectSeededOutcomes;
    stdx::unordered_map<std::string, stdx::unordered_map<std::string, std::string>> _filesByBucket;
};

// setupS3ClientWithBucket creates and seeds a mock S3 client with a specified bucket.
std::shared_ptr<MockS3Client> setupMockS3ClientWithBucket(std::string bucketName) {
    // Ensure the client is able to fetch the specified bucket when performing ListBucket when
    // the writer thread starts up.
    Aws::S3::Model::Bucket bucket{};
    bucket.SetName(bucketName);

    Aws::S3::Model::ListBucketsResult result;
    result.AddBuckets(bucket);

    auto client = std::make_shared<MockS3Client>();
    client->seedListBucketOutcome(result);

    return client;
}

std::string getObject(std::shared_ptr<S3Client> client, std::string bucket, std::string file) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(file);

    auto mockClient = dynamic_cast<MockS3Client*>(client.get());

    auto outcome = mockClient->GetObject(request);
    std::stringstream ss;
    if (outcome.IsSuccess()) {
        ss << outcome.GetResult().GetBody().rdbuf();
    }

    return ss.str();
}

TEST_F(S3EmitOperatorTest, FailsOnStartWhenListBucketRequestFails) {
    LOGV2_DEBUG(9997308,
                1,
                "Running test",
                "description"_attr =
                    "should fail when operator cannot connect to the specified bucket");
    // The mock client didn't seed any ListBucket outcomes. The next call to ListBucket will return
    // a Aws::S3::S3Error.
    std::shared_ptr<S3Client> mockClient = std::make_shared<MockS3Client>();
    setupDag(
        mockClient,
        [](std::shared_ptr<S3Client> client) {
            return S3EmitOperator::Options{
                .client = client,
                .bucket = {"randomBucket"},
                .path = {"not/very/happy"},
                .delimiter = "\n",
            };
        },
        false /* shouldBeConnected */);
    stopDag();
}

TEST_F(S3EmitOperatorTest, FailsOnStartWhenBucketNotValid) {
    LOGV2_DEBUG(9997309,
                1,
                "Running test",
                "description"_attr =
                    "should fail when operator cannot connect to the specified bucket");
    // The mock client provides a ListBucket response lacking the desired bucket name.
    auto mockClient = setupMockS3ClientWithBucket("incorrectBucketName");
    setupDag(
        mockClient,
        [](std::shared_ptr<S3Client> client) {
            return S3EmitOperator::Options{
                .client = client,
                .bucket = {"randomBucket"},
                .path = {"not/very/happy"},
                .delimiter = "\n",
            };
        },
        false /* shouldBeConnected */);
    stopDag();
}

TEST_F(S3EmitOperatorTest, FailureCases) {
    struct TestCase {
        const std::string description;
        const std::function<S3EmitOperator::Options(std::shared_ptr<S3Client>)> optionsFn;
        const std::function<std::shared_ptr<S3Client>()> setupClientFn;
        std::vector<StreamDocument> inputDocs;
        const mongo::ErrorCodes::Error expectedErrorCode;
        const std::string expectedErrorMsg;
    };

    std::vector<TestCase> testCases{
        {
            .description = "Should fail when S3 returns a NoSuchBucket error response",
            .optionsFn =
                [](std::shared_ptr<S3Client> client) {
                    return S3EmitOperator::Options{
                        .client = client,
                        .bucket = {"randomBucket"},
                        .path = {"not/very/happy"},
                        .delimiter = "\n",
                    };
                },
            .setupClientFn =
                []() {
                    auto client = setupMockS3ClientWithBucket("randomBucket");
                    Aws::S3::S3Error error{{Aws::S3::S3Errors::NO_SUCH_BUCKET, false}};
                    error.SetMessage("StreamsProcessorTest: No such bucket");
                    client->seedPutObjectAsyncOutcome(error);
                    return client;
                },
            .inputDocs =
                std::vector<StreamDocument>{
                    Document{fromjson(R"({"_ts": "2023-04-10T18:00:00.000000", "foo": "bar"})")},
                },
        },
        {
            .description =
                "Should fail when S3 returns an error response with a 403 Forbidden status code",
            .optionsFn =
                [](std::shared_ptr<S3Client> client) {
                    return S3EmitOperator::Options{
                        .client = client,
                        .bucket = {"randomBucket"},
                        .path = {"not/very/happy"},
                        .delimiter = "\n",
                    };
                },
            .setupClientFn =
                []() {
                    auto client = setupMockS3ClientWithBucket("randomBucket");
                    Aws::S3::S3Error error{{Aws::S3::S3Errors::ACCESS_DENIED, false}};
                    error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
                    error.SetMessage("StreamsProcessorTest: Forbidden");
                    client->seedPutObjectAsyncOutcome(error);
                    return client;
                },
            .inputDocs =
                std::vector<StreamDocument>{
                    Document{fromjson(R"({"_ts": "2023-04-10T18:00:00.000000", "foo": "bar"})")},
                },
        },
    };

    for (const auto& tc : testCases) {
        LOGV2_DEBUG(9997310, 1, "Running test", "description"_attr = tc.description);
        auto mockClient = tc.setupClientFn();
        setupDag(mockClient, tc.optionsFn, true);
        ScopeGuard guard([&] { stopDag(); });
        testFailure(mockClient, tc.inputDocs);
    }
}

TEST_F(S3EmitOperatorTest, SuccessCases) {
    struct TestCase {
        const std::string description;
        const std::function<S3EmitOperator::Options(std::shared_ptr<S3Client>)> optionsFn;
        const std::function<std::shared_ptr<S3Client>()> setupClientFn;
        std::vector<StreamDocument> inputDocs;
        const std::function<std::string(std::shared_ptr<S3Client>)> getFileFn;
        const std::string expectedFile;
    };

    std::vector<TestCase> testCases{
        {
            .description = "Should write documents to S3 when provided valid configurations",
            .optionsFn =
                [](std::shared_ptr<S3Client> client) {
                    return S3EmitOperator::Options{
                        .client = client,
                        .bucket = {"happyPathBucket"},
                        .path = {"happy/path//"},  // The extra / characters should get stripped.
                        .delimiter = "\n",
                    };
                },
            .setupClientFn = [&]() { return setupMockS3ClientWithBucket("happyPathBucket"); },
            .inputDocs =
                std::vector<StreamDocument>{
                    Document{fromjson(R"({"_ts": "2023-04-10T18:00:00.000000", "foo":
                     "bar"})")},
                },
            .getFileFn =
                [](std::shared_ptr<S3Client> client) {
                    return getObject(
                        client, "happyPathBucket", "happy/path/1681149600000-cd1a-0000.json");
                },
            .expectedFile =
                R"({"_ts":"2023-04-10T18:00:00.000000","foo":"bar","_stream_meta":{"source":{"type":"generated"}}}
)",
        },
        {
            .description = "Should write documents serialized in basicJson format",
            .optionsFn =
                [](std::shared_ptr<S3Client> client) {
                    return S3EmitOperator::Options{
                        .client = client,
                        .bucket = {"happyPathBucket"},
                        .path = {"boring/path/"},
                        .delimiter = "\n",
                        .outputFormat = mongo::S3EmitOutputFormatEnum::BasicJson,
                    };
                },
            .setupClientFn = [&]() { return setupMockS3ClientWithBucket("happyPathBucket"); },
            .inputDocs =
                std::vector<StreamDocument>{
                    Document{BSON("_ts" << "2023-04-10T18:00:00.000000"
                                        << "foo"
                                        << "bar"
                                        << "someDate"
                                        << mongo::Date_t::fromMillisSinceEpoch(1729625275856))},
                },
            .getFileFn =
                [](std::shared_ptr<S3Client> client) {
                    return getObject(
                        client, "happyPathBucket", "boring/path/1681149600000-cd1a-0000.json");
                },
            .expectedFile =
                R"({"_ts":"2023-04-10T18:00:00.000000","foo":"bar","someDate":1729625275856,"_stream_meta":{"source":{"type":"generated"}}}
)",
        },
        {.description = "Should write documents serialized in canonical extended json format",
         .optionsFn =
             [](std::shared_ptr<S3Client> client) {
                 return S3EmitOperator::Options{
                     .client = client,
                     .bucket = {"happyPathBucket"},
                     .path = {"boring/path//"},
                     .delimiter = "\n",
                     .outputFormat = mongo::S3EmitOutputFormatEnum::CanonicalJson,
                 };
             },
         .setupClientFn = [&]() { return setupMockS3ClientWithBucket("happyPathBucket"); },
         .inputDocs =
             std::vector<StreamDocument>{
                 Document{BSON("_ts" << "2023-04-10T18:00:00.000000"
                                     << "foo"
                                     << "bar"
                                     << "someDate"
                                     << mongo::Date_t::fromMillisSinceEpoch(1729625275856))},
             },
         .getFileFn =
             [](std::shared_ptr<S3Client> client) {
                 return getObject(
                     client, "happyPathBucket", "boring/path/1681149600000-cd1a-0000.json");
             },
         .expectedFile =
             R"({"_ts":"2023-04-10T18:00:00.000000","foo":"bar","someDate":{"$date":{"$numberLong":"1729625275856"}},"_stream_meta":{"source":{"type":"generated"}}}
)"},
        {.description = "Should write documents serialized in relaxed extended json format",
         .optionsFn =
             [](std::shared_ptr<S3Client> client) {
                 return S3EmitOperator::Options{
                     .client = client,
                     .bucket = {"happyPathBucket"},
                     .path = {"boring/path//"},
                     .delimiter = "\n",
                     .outputFormat = mongo::S3EmitOutputFormatEnum::RelaxedJson,
                 };
             },
         .setupClientFn = [&]() { return setupMockS3ClientWithBucket("happyPathBucket"); },
         .inputDocs =
             std::vector<StreamDocument>{
                 Document{BSON("_ts" << "2023-04-10T18:00:00.000000"
                                     << "foo"
                                     << "bar"
                                     << "someDate"
                                     << mongo::Date_t::fromMillisSinceEpoch(1729625275856))},
             },
         .getFileFn =
             [](std::shared_ptr<S3Client> client) {
                 return getObject(
                     client, "happyPathBucket", "boring/path/1681149600000-cd1a-0000.json");
             },
         .expectedFile =
             R"({"_ts":"2023-04-10T18:00:00.000000","foo":"bar","someDate":{"$date":"2024-10-22T19:27:55.856Z"},"_stream_meta":{"source":{"type":"generated"}}}
)"},
        {.description =
             "Should write documents serialized in relaxed extended json format by default",
         .optionsFn =
             [](std::shared_ptr<S3Client> client) {
                 return S3EmitOperator::Options{
                     .client = client,
                     .bucket = {"happyPathBucket"},
                     .path = {"boring/path//"},
                     .delimiter = "\n",
                 };
             },
         .setupClientFn = [&]() { return setupMockS3ClientWithBucket("happyPathBucket"); },
         .inputDocs =
             std::vector<StreamDocument>{
                 Document{BSON("_ts" << "2023-04-10T18:00:00.000000"
                                     << "foo"
                                     << "bar"
                                     << "someDate"
                                     << mongo::Date_t::fromMillisSinceEpoch(1729625275856))},
             },
         .getFileFn =
             [](std::shared_ptr<S3Client> client) {
                 return getObject(
                     client, "happyPathBucket", "boring/path/1681149600000-cd1a-0000.json");
             },
         .expectedFile =
             R"({"_ts":"2023-04-10T18:00:00.000000","foo":"bar","someDate":{"$date":"2024-10-22T19:27:55.856Z"},"_stream_meta":{"source":{"type":"generated"}}}
)"},
    };

    for (const auto& tc : testCases) {
        LOGV2_DEBUG(9997311, 1, "Running test", "description"_attr = tc.description);
        auto mockClient = tc.setupClientFn();
        setupDag(mockClient, tc.optionsFn, true /* shouldBeConnected */);
        ScopeGuard guard([&] { stopDag(); });
        testSuccess(mockClient, tc.inputDocs, tc.getFileFn, tc.expectedFile);
    }
}

}  // namespace
}  // namespace streams
