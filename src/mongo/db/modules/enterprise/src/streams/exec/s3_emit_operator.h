/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <string>

#include "aws_util.h"
#include "mongo/db/pipeline/name_expression.h"
#include "mongo/stdx/unordered_map.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/planner.h"
#include "streams/exec/queued_sink_operator.h"
#include "streams/exec/util.h"
#include "streams/util/string_validator.h"

namespace streams {

constexpr auto kUnsafeS3ObjectPathChars = std::initializer_list<char>{
    '\\', '^', '%', '`', '~', '[', ']', '\'', '"', '<', '>', '#', '|', '{', '}', '&'};

/**
 * S3EmitOperator is a QueuedSinkOperator that uses S3EmitWriter to upload files asyncronously to
 * S3.
 */
class S3EmitOperator : public QueuedSinkOperator {
public:
    constexpr static auto kDefaultDelimiter = "\n";

    struct Options {
        // Client used to make requests to S3.
        std::shared_ptr<S3Client> client;

        // S3 bucket to upload files to.
        NameExpression bucket;
        // S3 key prefix to write files to within the configured bucket.
        NameExpression path;
        // Delimiter used between string-formatted documents in a file to be uploaded to S3.
        std::string delimiter{"\n"};
        // JSON format used to serialize BSON to JSON
        mongo::S3EmitOutputFormatEnum outputFormat{mongo::S3EmitOutputFormatEnum::RelaxedJson};

        // Used to ensure that task ids are consistent in tests to guarantee consistent filenames.
        int testOnlyTaskIdSeed{0};
        // Used to extract timestamps from documents in tests to guarantee consistent filenames.
        DocumentTimestampExtractor* testOnlyTimestampExtractor{nullptr};
    };

    S3EmitOperator(Context* context, Options options);

    // Make a SinkWriter instance.
    std::unique_ptr<SinkWriter> makeWriter(int id) override;

    std::string doGetName() const override {
        return "S3EmitOperator";
    }

    mongo::ConnectionTypeEnum getConnectionType() const override {
        return mongo::ConnectionTypeEnum::AWSS3;
    }

    const S3EmitOperator::Options& options_forTest() const {
        return _options;
    }

private:
    S3EmitOperator::Options _options;
};

class S3EmitWriter : public SinkWriter {
public:
    struct Options : S3EmitOperator::Options {
        std::string literalBucket;
        std::string literalPath;
    };

    S3EmitWriter(Context* context, SinkOperator* sinkOperator, S3EmitWriter::Options options);

protected:
    OperatorStats processDataMsg(StreamDataMsg dataMsg) override;

    // connect checks if the provided credentials can access the S3 bucket.
    void connect() override;

private:
    static constexpr double kLogTriesPerSecond{1.0 / 60};

    struct Bucket {
        struct File {
            // Timestamp of the first document in this file. This is event time within tests, but
            // will be wall clock time otherwise.
            int64_t ts{0};
            // Documents represented in JSON string format.
            std::vector<std::string> documents;
        };

        stdx::unordered_map<std::string, File> files;
    };

    // Iterates over all files within _buckets and performs an async PutObject request
    // for files that meet trigger requirements.
    OperatorStats uploadFiles();

    // Generates a key used when uploading the object to S3.
    std::string createObjectKey(const std::string& path, const Bucket::File& file);

    // Performs expression evaluation to get a bucket name.
    std::string getBucketFromExpr(const Document& doc);

    // Returns a cleaned path by evaluating an expression with an input
    // document. A cleaned path is one that has no trailing / characters.
    std::string getCleanPathFromExpr(const Document& doc);

    // Handles S3 errors depending on the input error's type for a given operation
    // (ListBuckets / PutObject).
    void handleS3Error(const Aws::S3::S3Error& outcome, const std::string& operation);

    // Limits the number of logs being emited for a given log id.
    void tryLog(int id, std::function<void(int logID)> logFn);

    // Return the streams JSON string format equivalent of an S3 output format if possible
    JsonStringFormat getJsonStringFormat();

    Context* _context{nullptr};
    S3EmitWriter::Options _options;
    // Holds all unwritten files
    stdx::unordered_map<std::string, Bucket> _buckets;
    // Used when creating an object key. Uniquely represents the sink writer thread.
    std::string _taskId;
    // Used to control the volume of logs being produced
    stdx::unordered_map<int, std::unique_ptr<RateLimiter>> _logIDToRateLimiter;
    // Used for rate limiting logs.
    Timer _timer{};

    // Protects the _status member
    mutable mongo::stdx::mutex _statusMu;
    mongo::Status _status{mongo::Status::OK()};
};

}  // namespace streams
