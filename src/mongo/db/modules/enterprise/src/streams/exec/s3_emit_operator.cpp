/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/s3_emit_operator.h"

#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fmt/format.h>
#include <memory>
#include <mongocxx/logger.hpp>

#include "mongo/bson/oid.h"
#include "mongo/db/query/random_utils.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/testing_proctor.h"
#include "mongo/util/time_support.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/util.h"
#include "streams/util/exception.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

namespace {

constexpr auto kOperationPutObject = "PutObject";
constexpr auto kOperationListBuckets = "ListBuckets";

// Returns a random string of 4 hexadecimal digits to be used as a taskId.
//
// TODO(SERVER-101619) When parallelism > 1 is supported, make sure that the
// generated task ID is unique amongst the threads spun up by the operator.
std::string createTaskId(const S3EmitOperator::Options& options) {
    static constexpr auto hexDigits = "0123456789abcdef"_sd;
    static constexpr size_t outputSize = 4;

    boost::optional<mongo::PseudoRandom> localRng;
    if (auto seed = options.testOnlyTaskIdSeed) {
        localRng.emplace(seed);
    }
    auto& rng = localRng ? *localRng : mongo::random_utils::getRNG();

    std::string out;
    uint32_t bits = 0;
    for (size_t i = 0; i < outputSize; ++i) {
        if (i == 0) {

            bits = rng.nextUInt32();
        }
        out += hexDigits[bits & 0xf];
        bits >>= 4;
    }
    return out;
}

void cleanPath(std::string& path) {
    while (path.ends_with('/')) {
        path.pop_back();
    }
}

}  // namespace

S3EmitOperator::S3EmitOperator(Context* context, S3EmitOperator::Options options)
    : QueuedSinkOperator(context, 1 /* numInputs */, 1 /* parallelism */),
      _options(std::move(options)) {}

S3EmitWriter::Options makeWriterOptions(int id, const S3EmitOperator::Options& opts) {
    // TODO(SERVER-101619) When parallelism > 1 is supported, make sure that we make a copy of the
    // bucket and path NameExpression instances for each writer thread.
    S3EmitWriter::Options out{opts};
    if (out.bucket.isLiteral()) {
        out.literalBucket = out.bucket.getLiteral();
    }

    if (out.path.isLiteral()) {
        auto path = out.path.getLiteral();
        cleanPath(path);
        out.literalPath = std::move(path);
    }

    if (out.testOnlyTaskIdSeed > 0) {
        // add the testId to the seed to ensure that each writer generates a unique taskId.
        out.testOnlyTaskIdSeed += id;
    }

    return out;
}

std::unique_ptr<SinkWriter> S3EmitOperator::makeWriter(int id) {
    return std::make_unique<S3EmitWriter>(_context, this, makeWriterOptions(0, _options));
}

S3EmitWriter::S3EmitWriter(Context* context,
                           SinkOperator* sinkOperator,
                           S3EmitWriter::Options options)
    : SinkWriter(context, sinkOperator),
      _context{context},
      _options(std::move(options)),
      _taskId(createTaskId(_options)) {}


void S3EmitWriter::connect() {
    if (_options.literalBucket.empty()) {
        // We cannot verify if the bucket exists now because the configured bucket is an
        // expression.
        return;
    }

    Aws::S3::Model::ListBucketsRequest request;
    const std::string& bucketName = _options.literalBucket;
    request.SetPrefix(bucketName);

    Aws::S3::Model::ListBucketsOutcome response = _options.client->ListBuckets(request);
    uassert(ErrorCodes::StreamProcessorS3ConnectionError,
            fmt::format("Failed to fetch authorized buckets while connecting to S3: {}",
                        response.GetError().GetMessage()),
            response.IsSuccess());

    // Find target bucket in list of buckets the provided credentials has access to.
    bool foundTargetBucket = false;
    auto buckets = response.GetResult().GetBuckets();
    for (const auto& bucket : buckets) {
        if (bucket.GetName() == bucketName) {
            foundTargetBucket = true;
            break;
        }
    }

    uassert(
        ErrorCodes::StreamProcessorInvalidOptions,
        fmt::format("AWS Role does not have access to bucket {}. Check AWS role access permissions "
                    "and try again.",
                    bucketName),
        foundTargetBucket);
}

JsonStringFormat S3EmitWriter::getJsonStringFormat() {
    switch (_options.outputFormat) {
        case mongo::S3EmitOutputFormatEnum::CanonicalJson:
            return JsonStringFormat::Canonical;
        case mongo::S3EmitOutputFormatEnum::RelaxedJson:
            return JsonStringFormat::Relaxed;
        case mongo::S3EmitOutputFormatEnum::BasicJson:
            return JsonStringFormat::Basic;
        default:
            tasserted(9997602, "Output format has no JSON string equivalent format");
    }
}

// Places documents in the data message in the appropriate file within _buckets. It then uploads
// all files whose trigger conditions have been fulfilled.
OperatorStats S3EmitWriter::processDataMsg(StreamDataMsg dataMsg) {
    // check if a previous PutObjectAsync call encountered an error.
    {
        stdx::lock_guard<stdx::mutex> lock(_statusMu);
        if (!_status.isOK()) {
            spasserted(_status);
        }
    }


    OperatorStats stats;
    // Batch input documents within files in _buckets
    for (const auto& doc : dataMsg.docs) {
        // TODO(SERVER-99974): support expressions for path and bucket. add a try/catch
        // here and dlq if bucket or path are malformed.
        std::string dynamicBucket;
        if (!_options.bucket.isLiteral()) {
            dynamicBucket = getBucketFromExpr(doc.doc);
        }
        const auto& bucketName =
            _options.bucket.isLiteral() ? _options.literalBucket : dynamicBucket;

        auto bucketIt = _buckets.find(bucketName);
        if (bucketIt == _buckets.end()) {
            auto [it, _] = _buckets.insert({bucketName, {}});
            bucketIt = it;
        }
        auto& bucket = bucketIt->second;

        std::string dynamicPath;
        if (!_options.path.isLiteral()) {
            dynamicPath = getCleanPathFromExpr(doc.doc);
        }
        const auto& path = _options.path.isLiteral() ? _options.literalPath : dynamicPath;

        auto fileIt = bucket.files.find(path);
        if (fileIt == bucket.files.end()) {
            std::vector<std::string> documents;
            documents.reserve(1);  // TODO(SERVER-99975) use trigger.count, if configured.
            Bucket::File file{
                .documents = std::move(documents),
            };

            // Assign the newly-created file's timestamp.
            if (_options.testOnlyTimestampExtractor) {
                file.ts = _options.testOnlyTimestampExtractor->extractTimestamp(doc.doc).asInt64();
            } else {
                file.ts = curTimeMillis64();
            }
            auto [it, _] = bucket.files.emplace(path, file);
            fileIt = it;
        }

        fileIt->second.documents.push_back(serializeJson(doc.doc.toBson(), getJsonStringFormat()));
        stats.numOutputDocs++;
    }

    stats += uploadFiles();

    return stats;
}

OperatorStats S3EmitWriter::uploadFiles() {
    OperatorStats stats;
    // TODO(SERVER-99975): support trigger.count
    // TODO(SERVER-99977): support trigger.bytes
    // TODO(SERVER-99978): support trigger.interval
    // For a simple implementation, we'll upload every document that is passed in.
    for (auto&& [bucketName, bucket] : _buckets) {
        for (auto it = bucket.files.begin(); it != bucket.files.end();) {
            const std::string& path = it->first;
            const Bucket::File& file = it->second;
            tassert(9997301, "Expected file to have documents", file.documents.size() > 0);
            Aws::S3::Model::PutObjectRequest request;
            request.SetBucket(bucketName);
            const auto& key = createObjectKey(path, file);
            request.SetKey(key);

            auto requestBody = std::make_shared<std::stringstream>();
            auto inputLength = file.documents.size();
            int64_t outputLength = 0;
            for (size_t i = 0; i < inputLength; i++) {
                *requestBody << file.documents[i];
                *requestBody << _options.delimiter;
                outputLength += (file.documents[i].size() + _options.delimiter.size());
            }
            request.SetBody(std::move(requestBody));

            if (TestingProctor::instance().isEnabled()) {
                LOGV2_INFO(9997302,
                           "Making S3 PutObject call.",
                           "context"_attr = _context,
                           "bucketName"_attr = bucketName,
                           "key"_attr = key);
            }
            stats.numOutputBytes += outputLength;
            _options.client->PutObjectAsync(
                std::move(request),
                [this](const Aws::S3::S3Client* s3Client,
                       const Aws::S3::Model::PutObjectRequest& request,
                       const Aws::S3::Model::PutObjectOutcome& outcome,
                       const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                    if (outcome.IsSuccess()) {
                        return;
                    }
                    LOGV2_INFO(
                        9997303,
                        "S3EmitOperator encountered an error while making a PutObject request",
                        "error"_attr = outcome.GetError().GetMessage(),
                        "context"_attr = _context,
                        "bucket"_attr = request.GetBucket(),
                        "key"_attr = request.GetKey());
                    handleS3Error(outcome.GetError(), kOperationPutObject);
                });
            bucket.files.erase(it++);
        }
    }
    return stats;
}

// Sets a status. Status is checked on next data message.
void S3EmitWriter::handleS3Error(const Aws::S3::S3Error& error, const std::string& operation) {
    // Manually handle non-retryable errors.
    auto errorCode = ErrorCodes::StreamProcessorS3ConnectionError;
    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN ||
        error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        error.GetResponseCode() == Aws::Http::HttpResponseCode::BAD_REQUEST) {
        errorCode = ErrorCodes::StreamProcessorS3Error;
    }

    {
        stdx::lock_guard<stdx::mutex> lock(_statusMu);
        _status =
            mongo::Status{errorCode,
                          fmt::format("S3EmitOperator encountered an error while calling {}: {}",
                                      operation,
                                      error.GetMessage())};
    }
}


std::string S3EmitWriter::createObjectKey(const std::string& path, const Bucket::File& file) {
    // TODO(SERVER-101517): increment part number if another file with the same path and
    // wall time was previously uploaded.
    // ex: myStaticPath/1739652142669-1ac2-0000.json
    return fmt::format("{}/{}-{}-0000.json", path, file.ts, _taskId);
}

std::string S3EmitWriter::getBucketFromExpr(const Document& document) {
    tassert(9997304, "Expected bucket to be an expression", !_options.path.isLiteral());

    // TODO(SERVER-99974) support expression evaluation
    MONGO_UNIMPLEMENTED;
}

std::string S3EmitWriter::getCleanPathFromExpr(const Document& document) {
    tassert(9997306, "Expected path to be an expression", !_options.path.isLiteral());

    // TODO(SERVER-99974) support expression evaluation
    MONGO_UNIMPLEMENTED;
}

void S3EmitWriter::tryLog(int id, std::function<void(int logID)> logFn) {
    auto it = _logIDToRateLimiter.find(id);
    if (it == _logIDToRateLimiter.end()) {
        auto limiter = std::make_unique<RateLimiter>(kLogTriesPerSecond, 1, &_timer);
        std::tie(it, std::ignore) = _logIDToRateLimiter.insert({id, std::move(limiter)});
    }
    if (it->second->consume() > Seconds(0)) {
        return;
    }

    logFn(id);
}
}  // namespace streams
