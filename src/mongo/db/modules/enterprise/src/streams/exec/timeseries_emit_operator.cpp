/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/timeseries_emit_operator.h"

#include <boost/optional.hpp>
#include <boost/optional/optional.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fmt/format.h>
#include <memory>
#include <mongocxx/collection.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <string>
#include <utility>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/operator.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;
using bsoncxx::builder::basic::kvp;

TimeseriesEmitOperator::TimeseriesEmitOperator(Context* context, Options options)
    : QueuedSinkOperator(context, 1 /* numInputs */), _options(std::move(options)) {
    _instance = getMongocxxInstance(_options.clientOptions.svcCtx);
    _uri = makeMongocxxUri(_options.clientOptions.uri);
    _client =
        std::make_unique<mongocxx::client>(*_uri, _options.clientOptions.toMongoCxxClientOptions());
    if (!_options.dbExpr) {
        _database = std::make_unique<mongocxx::database>(
            _client->database(*_options.clientOptions.database));
    }

    mongocxx::write_concern writeConcern;
    writeConcern.journal(true);
    writeConcern.acknowledge_level(mongocxx::write_concern::level::k_majority);
    writeConcern.majority(/*timeout*/ stdx::chrono::milliseconds(60 * 1000));
    _insertOptions = mongocxx::options::insert().write_concern(std::move(writeConcern));
    if (_options.dbExpr || _options.collExpr) {
        _errorPrefix = "Time series $emit failed";
    } else {
        _errorPrefix = fmt::format("Time series $emit to {}.{} failed",
                                   *_options.clientOptions.database,
                                   *_options.clientOptions.collection);
    }
    _stats.connectionType = ConnectionTypeEnum::Atlas;
}

OperatorStats TimeseriesEmitOperator::processDataMsg(StreamDataMsg dataMsg) {
    if (_options.collExpr || _options.dbExpr) {
        return processStreamDocs(dataMsg, /* startIdx */ 0, /* maxDocCount */ 1);
    }
    return processStreamDocs(dataMsg, /* startIdx */ 0, kDataMsgMaxDocSize);
}

void TimeseriesEmitOperator::validateConnection() {
    if (_options.dbExpr || _options.collExpr) {
        return;
    }
    ErrorCodes::Error genericErrorCode{74452};
    tassert(9777901, "_database should be set", _database);

    auto validateFunc = [this]() {
        mongocxx::collection collection;
        auto collectionExists = _database->has_collection(*_options.clientOptions.collection);
        auto tsOptions = getTimeseriesOptionsFromDb();
        if (collectionExists) {
            uassert(ErrorCodes::StreamProcessorInvalidOptions,
                    str::stream() << "$emit can only be used with a time series collection. "
                                  << *_options.clientOptions.collection
                                  << " is not a time series collection.",
                    tsOptions);
        }
        if (_options.timeseriesSinkOptions.getTimeseries()) {
            // The field $emit.timeseries is present.
            // Check if the collection exist.
            if (collectionExists) {
                uassert(
                    ErrorCodes::StreamProcessorInvalidOptions,
                    str::stream() << "Found a Time Series collection "
                                  << *_options.clientOptions.collection << " with a timeField "
                                  << tsOptions->getTimeField().toString()
                                  << " that doesn't match the $emit.timeField "
                                  << _options.timeseriesSinkOptions.getTimeseries()
                                         ->getTimeField()
                                         .toString(),
                    tsOptions->getTimeField().toString() ==
                        _options.timeseriesSinkOptions.getTimeseries()->getTimeField().toString());

                // Found a valid timeseries collection.
                collection = _database->collection(*_options.clientOptions.collection);
            } else {
                // Create a new timeseries collection.
                auto opts = bsoncxx::builder::basic::document{};
                opts.append(kvp(
                    "timeseries",
                    toBsoncxxValue((_options.timeseriesSinkOptions.getTimeseries())->toBSON())));
                collection =
                    _database->create_collection(*_options.clientOptions.collection, opts.view());
            }
        } else {
            // The field $emit.timeseries is missing.
            uassert(ErrorCodes::StreamProcessorInvalidOptions,
                    str::stream() << "$emit.timeSeries must be specified when the collection does "
                                     "not already exist "
                                  << *_options.clientOptions.collection,
                    collectionExists);
            _options.timeseriesSinkOptions.setTimeseries(std::move(*tsOptions));
            collection = _database->collection(*_options.clientOptions.collection);
        }
        _collection = std::make_unique<mongocxx::collection>(std::move(collection));
    };

    auto status = runMongocxxNoThrow(
        std::move(validateFunc), _context, genericErrorCode, _errorPrefix, *_uri);
    spassert(status, status.isOK());
}

void TimeseriesEmitOperator::processDbAndCollExpressions(const StreamDocument& streamDoc) {
    // TODO(SERVER-98021): Handle collection validation with expressions.
    if (_options.dbExpr) {
        // TODO(SERVER-98021): handle expression failures with DLQ messages
        Value val = _options.dbExpr->evaluate(streamDoc.doc, &_context->expCtx->variables);
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Failed to parse database expresssion",
                val.getType() == String);
        std::string db = val.getString();
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Expected database name but got none",
                !db.empty());
        _database = std::make_unique<mongocxx::database>(_client->database(std::move(db)));
    }
    std::string coll;
    if (_options.clientOptions.collection) {
        coll = *_options.clientOptions.collection;
    } else {
        tassert(9777900, "_options.collExpr should be set", _options.collExpr);
        // TODO(SERVER-98021): handle expression failures with DLQ messages
        Value val = _options.collExpr->evaluate(streamDoc.doc, &_context->expCtx->variables);
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Failed to parse collection expresssion",
                val.getType() == String);
        std::string collName = val.getString();
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Expected collection name but got none",
                !collName.empty());
        coll = collName;
    }

    // TODO(SERVER-98021): Perform collection validations and cache the mongocxx::collection in a
    // LRU cache
    mongocxx::collection collection;
    auto collectionExists = _database->has_collection(coll);
    // TODO(SERVER-98021): handle the case where the collection doesn't exist but user has not
    // specified _options.timeseriesSinkOptions
    if (collectionExists) {
        collection = _database->collection(coll);
    } else {
        auto opts = bsoncxx::builder::basic::document{};
        opts.append(
            kvp("timeseries",
                toBsoncxxValue((_options.timeseriesSinkOptions.getTimeseries())->toBSON())));
        collection = _database->create_collection(coll, opts.view());
    }
    _collection = std::make_unique<mongocxx::collection>(std::move(collection));
}

OperatorStats TimeseriesEmitOperator::processStreamDocs(StreamDataMsg dataMsg,
                                                        size_t startIdx,
                                                        size_t maxDocCount) {
    OperatorStats stats;
    // The max document size limit for a time series collectio in 4MB
    // https://www.mongodb.com/docs/v5.0/core/timeseries/timeseries-limitations/#constraints
    const auto maxBatchObjectSizeBytes = BSONObjMaxUserSize / 4;
    bool samplersPresent = samplersExist();
    int32_t curBatchByteSize{0};
    size_t curIdx{startIdx};
    std::vector<bsoncxx::document::value> docBatch;

    while (curIdx < dataMsg.docs.size()) {
        const auto& streamDoc = dataMsg.docs[curIdx++];
        if (_options.collExpr || _options.dbExpr) {
            processDbAndCollExpressions(streamDoc);
        }
        auto docSize = streamDoc.doc.memUsageForSorter();
        // Send the document to dlq if it is larger than the size limit.
        if (docSize > maxBatchObjectSizeBytes) {
            std::string error = str::stream()
                << "Input document for a Time Series collection is too large, " << docSize << " > "
                << maxBatchObjectSizeBytes;
            stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
            ++stats.numDlqDocs;
        } else {
            // The sink (Time Series collection) will reject the document with a missing timeField
            // Check for the timeField to avoid write failures
            const auto& timeField = _options.timeseriesSinkOptions.getTimeseries()->getTimeField();
            const auto& timeValue = streamDoc.doc.getField(timeField);
            if (timeValue.missing() || timeValue.getType() != BSONType::Date) {
                // Send the document to dlq if timeField is missing or the format is not BSON UTC
                std::string error = str::stream()
                    << "timeField '" << timeField
                    << "' must be present and contain a valid BSON UTC datetime value";
                stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                    _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
                ++stats.numDlqDocs;
            } else {
                docBatch.push_back(toBsoncxxValue(std::move(streamDoc).doc.toBson()));
                curBatchByteSize += docSize;
            }
        }

        // insert the documents to the collection, if either
        // - all the documents are processed and we have something to insert, or
        // - maxDocCount number of documents are processes, or
        // - curBatchByteSize is more than the dataMsh max byte size
        if ((curIdx == dataMsg.docs.size() && !docBatch.empty()) ||
            docBatch.size() == maxDocCount || curBatchByteSize >= kSinkDataMsgMaxDocSize) {
            try {
                int64_t curBatchSize = docBatch.size();
                auto start = stdx::chrono::steady_clock::now();
                auto res = _collection->insert_many(std::move(docBatch), _insertOptions);
                if (!res || res->inserted_count() < curBatchSize) {
                    uasserted(74450,
                              fmt::format("Error encountered in {} while writing to a Time Series "
                                          "collection: {} and db: {}",
                                          getName(),
                                          _collection->name().to_string(),
                                          _database->name().to_string()));
                }
                auto elapsed = stdx::chrono::steady_clock::now() - start;
                _writeLatencyMs->increment(
                    stdx::chrono::duration_cast<stdx::chrono::milliseconds>(elapsed).count());
                stats.numOutputDocs += curBatchSize;
                stats.numOutputBytes += curBatchByteSize;
                if (samplersPresent) {
                    StreamDataMsg msg;
                    msg.docs.reserve(curBatchSize);
                    for (int i = 0; i < curBatchSize; i++) {
                        msg.docs.push_back(dataMsg.docs[startIdx + i]);
                    }
                    sendOutputToSamplers(std::move(msg));
                }
            } catch (const mongocxx::operation_exception& ex) {
                auto writeError = getWriteErrorFromRawServerError(ex);
                if (!writeError) {
                    LOGV2_INFO(74787,
                               "Error encountered while writing to target in timeseries $emit",
                               "db"_attr = _database->name().to_string(),
                               "coll"_attr = _collection->name().to_string(),
                               "context"_attr = _context,
                               "exception"_attr = ex.what(),
                               "code"_attr = int(ex.code().value()));
                    spasserted(mongocxxExceptionToStatus(ex, *_uri, _errorPrefix));
                }

                // The writeErrors field exist, find which document cause the write error.
                size_t writeErrorIndex = writeError->getIndex();
                stats.numOutputDocs += writeErrorIndex;
                StreamDataMsg msg;
                if (samplersPresent && writeErrorIndex > 0) {
                    msg.docs.reserve(writeErrorIndex);
                }
                for (size_t i = 0; i < writeErrorIndex; i++) {
                    stats.numOutputBytes += dataMsg.docs[startIdx + i].doc.memUsageForSorter();
                    if (samplersPresent) {
                        msg.docs.push_back(dataMsg.docs[startIdx + i]);
                    }
                }
                if (samplersPresent) {
                    sendOutputToSamplers(std::move(msg));
                }
                invariant(startIdx + writeErrorIndex < curIdx);

                // Add the document with the write error to the dlq
                const auto& streamDoc = dataMsg.docs[startIdx + writeErrorIndex];
                std::string error = str::stream()
                    << "Failed to process an input document in the current batch in " << getName()
                    << " with error: code = " << writeError->getStatus().codeString()
                    << ", reason = " << writeError->getStatus().reason();
                stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                    _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
                ++stats.numDlqDocs;

                // Reprocess the remaining documents in the current batch individually.
                for (size_t i = startIdx + writeErrorIndex + 1; i < curIdx; ++i) {
                    stats += processStreamDocs(dataMsg, i, /*maxDocCount*/ 1);
                }
            }

            // reset the current batch size
            curBatchByteSize = 0;
            docBatch.clear();
        }
    }
    stats.timeSpent = dataMsg.creationTimer->elapsed();
    return stats;
}

boost::optional<TimeseriesOptions> TimeseriesEmitOperator::getTimeseriesOptionsFromDb() {
    boost::optional<TimeseriesOptions> tsOptions;
    // Create a filter on the name and type (timeseries) of the collection.

    // TODO(SERVER-98021): refactor this method because as of now it assumes the database and
    // collection are already evaluated
    tassert(9777902,
            "don't run this method if db or coll are expressions",
            !_options.dbExpr && !_options.collExpr);
    auto filter = BSON("type"
                       << "timeseries"
                       << "name" << *_options.clientOptions.collection);
    mongocxx::cursor tsCollectionCursor =
        _database->list_collections(toBsoncxxView(std::move(filter)));

    // tsCollectionCursor will have either one entry (for the timeseries collection) or none, if the
    // collection is not of type timeseries.
    auto tsCollectionItr = tsCollectionCursor.begin();
    while (tsCollectionItr != tsCollectionCursor.end()) {
        BSONObj tsCollectionObj = fromBsoncxxDocument(*tsCollectionItr);
        if (tsCollectionObj["name"].String() == *_options.clientOptions.collection) {
            BSONObj tsCollectionOptions = tsCollectionObj["options"].Obj();
            tsOptions = TimeseriesOptions::parseOwned(
                IDLParserContext("TimeseriesOptions"),
                std::move(tsCollectionOptions)["timeseries"].Obj().getOwned());
            break;
        }
        ++tsCollectionItr;
    }
    return tsOptions;
}
}  // namespace streams
