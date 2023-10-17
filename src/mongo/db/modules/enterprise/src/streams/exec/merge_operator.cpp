/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/merge_operator.h"

#include <boost/optional.hpp>
#include <exception>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/exception.hpp>
#include <set>
#include <string>
#include <utility>

#include "mongo/bson/json.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/merge_processor.h"
#include "mongo/db/pipeline/process_interface/mongo_process_interface.h"
#include "mongo/logv2/log.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/namespace_string_util.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/log_util.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

namespace {

static constexpr char kWriteErrorsField[] = "writeErrors";

// Max size of the queue (in bytes) until `push()` starts blocking.
static constexpr int64_t kQueueMaxSizeBytes = 128 * 1024 * 1024;  // 128 MB

};  // namespace

// Returns the single index in the writeErrors field in the given exception returned by mongocxx.
size_t getWriteErrorIndexFromRawServerError(const bsoncxx::document::value& rawServerError) {
    // Here is the expected schema of 'rawServerError':
    // https://github.com/mongodb/specifications/blob/master/source/driver-bulk-update.rst#merging-write-errors
    auto rawServerErrorObj = fromBsonCxxDocument(rawServerError);

    // Extract write error indexes.
    auto writeErrorsVec = rawServerErrorObj["writeErrors"].Array();
    std::set<size_t> writeErrorIndexes;
    for (auto& writeError : writeErrorsVec) {
        writeErrorIndexes.insert(writeError["index"].Int());
    }
    uassert(ErrorCodes::InternalError,
            "bulk_write_exception::raw_server_error() contains duplicate entries in the "
            "'writeErrors' field",
            writeErrorIndexes.size() == writeErrorsVec.size());

    // Since we apply the writes in ordered manner there should only be 1 failed write and all the
    // writes before it should have succeeded.
    uassert(ErrorCodes::InternalError,
            str::stream() << "bulk_write_exception::raw_server_error() contains unexpected ("
                          << writeErrorIndexes.size() << ") number of write error",
            writeErrorIndexes.size() == 1);

    // Extract upserted indexes.
    auto upserted = rawServerErrorObj["upserted"];
    std::set<size_t> upsertedIndexes;
    if (!upserted.eoo()) {
        auto upsertedVec = upserted.Array();
        for (auto& upsertedItem : upsertedVec) {
            upsertedIndexes.insert(upsertedItem["index"].Int());
        }
        uassert(ErrorCodes::InternalError,
                "bulk_write_exception::raw_server_error() contains duplicate entries in the "
                "'upserted' field",
                upsertedIndexes.size() == upsertedVec.size());
        uassert(ErrorCodes::InternalError,
                str::stream() << "unexpected number of upserted indexes (" << upsertedIndexes.size()
                              << " vs " << writeErrorIndexes.size() << ")",
                upsertedIndexes.size() == *writeErrorIndexes.begin());
        size_t i = 0;
        for (auto idx : upsertedIndexes) {
            uassert(ErrorCodes::InternalError,
                    str::stream() << "unexpected upserted index value (" << idx << " vs " << i
                                  << ")",
                    idx == i);
            ++i;
        }
    }
    return *writeErrorIndexes.begin();
}

MergeOperator::MergeOperator(Context* context, Options options)
    : SinkOperator(context, 1 /* numInputs */),
      _options(std::move(options)),
      _processor(_options.documentSource->getMergeProcessor()),
      _queue(decltype(_queue)::Options{.maxQueueDepth = kQueueMaxSizeBytes}) {

    _queueSize = _context->metricManager->registerCallbackGauge(
        "merge_operator_queue_bytesize",
        /* description */ "Total bytes currently buffered in the queue",
        /*labels*/ getDefaultMetricLabels(_context),
        [this]() { return _queue.getStats().queueDepth; });
}

void MergeOperator::doStart() {
    stdx::lock_guard<Latch> lock(_consumerMutex);
    dassert(!_consumerThread.joinable());
    dassert(!_consumerThreadRunning);
    _consumerThread = stdx::thread([this]() { consumeLoop(); });
    _consumerThreadRunning = true;
}

void MergeOperator::doStop() {
    // This will close the queue which will make the consumer thread exit as well
    // because this will trigger a `ProducerConsumerQueueConsumed` exception in the
    // consumer thread.
    _queue.closeConsumerEnd();
    if (_consumerThread.joinable()) {
        _consumerThread.join();
    }
}

void MergeOperator::doFlush() {
    stdx::unique_lock<Latch> lock(_consumerMutex);

    dassert(!_pendingFlush);
    _pendingFlush = true;
    _queue.push(Message{.flushSignal = true});
    _flushedCv.wait(lock, [this]() -> bool { return !_consumerThreadRunning || !_pendingFlush; });

    // Make sure that an error wasn't encountered in the background consumer thread while
    // waiting for the flushed condvar to be notified.
    uassert(75386,
            str::stream() << "unable to flush merge operator with error: "
                          << _consumerError.value_or("unknown"),
            !_consumerError && !_pendingFlush);
}

boost::optional<std::string> MergeOperator::doGetError() {
    stdx::lock_guard<Latch> lock(_consumerMutex);
    return _consumerError;
}

void MergeOperator::doSinkOnDataMsg(int32_t inputIdx,
                                    StreamDataMsg dataMsg,
                                    boost::optional<StreamControlMsg> controlMsg) {
    _queue.push(Message{.data = std::move(dataMsg)});
}

auto MergeOperator::partitionDocsByTargets(const StreamDataMsg& dataMsg) -> DocPartitions {
    auto getNsKey = [this](const StreamDocument& streamDoc) -> boost::optional<NsKey> {
        const auto& doc = streamDoc.doc;
        try {
            return std::make_pair(_options.db.evaluate(_context->expCtx.get(), doc),
                                  _options.coll.evaluate(_context->expCtx.get(), doc));
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to evaluate target namespace in "
                                              << getName() << " with error: " << e.what();
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));
            incOperatorStats({.numDlqDocs = 1});
            return boost::none;
        }
    };

    DocPartitions docPartitions;
    for (size_t docIdx = 0; docIdx < dataMsg.docs.size(); ++docIdx) {
        auto nsKey = getNsKey(dataMsg.docs[docIdx]);
        if (!nsKey) {
            continue;
        }
        auto [it, inserted] = docPartitions.try_emplace(*nsKey, std::vector<size_t>{});
        if (inserted) {
            it->second.reserve(dataMsg.docs.size());
        }
        it->second.push_back(docIdx);
    }

    return docPartitions;
}

void MergeOperator::consumeLoop() {
    bool done{false};
    boost::optional<std::string> error;

    while (!done) {
        try {
            auto msg = _queue.pop();
            if (msg.flushSignal) {
                stdx::lock_guard<Latch> lock(_consumerMutex);
                _pendingFlush = false;
                _flushedCv.notify_all();
            } else {
                const StreamDataMsg& dataMsg = *msg.data;
                // Partitions the docs in 'dataMsg' based on their target namespaces.
                auto docPartitions = partitionDocsByTargets(dataMsg);

                // Process each document partition.
                for (const auto& [nsKey, docIndices] : docPartitions) {
                    auto outputNs =
                        getNamespaceString(/*dbStr*/ nsKey.first, /*collStr*/ nsKey.second);
                    processStreamDocs(dataMsg, outputNs, docIndices, kDataMsgMaxDocSize);
                }
            }
        } catch (const ExceptionFor<ErrorCodes::ProducerConsumerQueueEndClosed>&) {
            // Closed naturally from `stop()`.
            done = true;
        } catch (const DBException& e) {
            // TODO Figure out a way to transfer the inner error code directly. The toString()
            // result has the error code information in the form of "Location1234500: error
            // message".
            error = e.toString();
            done = true;
        } catch (const std::exception& e) {
            error = e.what();
            done = true;
        }
    }

    // Wake up the executor thread if its waiting on a flush. If we're exiting the consume
    // loop because of an exception, then the flush in the executor thread will fail after
    // it receives the flushed condvar signal.
    stdx::lock_guard<Latch> lock(_consumerMutex);
    _consumerError = std::move(error);
    _consumerThreadRunning = false;
    _flushedCv.notify_all();
}

void MergeOperator::processStreamDocs(const StreamDataMsg& dataMsg,
                                      const NamespaceString& outputNs,
                                      const DocIndices& docIndices,
                                      size_t maxBatchDocSize) {
    const auto maxBatchObjectSizeBytes = BSONObjMaxUserSize / 2;
    int32_t curBatchByteSize{0};
    // Create batches honoring the maxBatchDocSize and kDataMsgMaxByteSize size limits.
    size_t startIdx = 0;
    while (startIdx < docIndices.size()) {
        MongoProcessInterface::BatchedObjects curBatch;

        // [startIdx, curIdx) range determines the current batch.
        size_t curIdx{startIdx};
        stdx::unordered_set<size_t> badDocIndexes;
        while (curIdx < docIndices.size()) {
            const auto& streamDoc = dataMsg.docs[docIndices[curIdx++]];
            try {
                auto docSize = streamDoc.doc.getCurrentApproximateSize();
                uassert(ErrorCodes::InternalError,
                        str::stream()
                            << "Output document is too large (" << (docSize / 1024) << "KB)",
                        docSize <= maxBatchObjectSizeBytes);

                auto batchObject = _processor->makeBatchObject(streamDoc.doc);
                curBatch.push_back(std::move(batchObject));
                curBatchByteSize += docSize;
                if (curBatch.size() == maxBatchDocSize || curBatchByteSize >= kDataMsgMaxByteSize) {
                    // Current batch is ready to flush.
                    break;
                }
            } catch (const DBException& e) {
                invariant(curIdx >= startIdx);
                badDocIndexes.insert(docIndices[curIdx - 1]);
                std::string error = str::stream() << "Failed to process input document in "
                                                  << getName() << " with error: " << e.what();
                _context->dlq->addMessage(
                    toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));
                incOperatorStats({.numDlqDocs = 1});
            }
        }

        /*
         * All following code assumes that the 'curIdx' points to the next element to process.
         */

        if (!curBatch.empty()) {
            try {
                auto batchedCommandReq =
                    _processor->getMergeStrategyDescriptor().batchedCommandGenerator(
                        _context->expCtx, outputNs);
                _processor->flush(outputNs, std::move(batchedCommandReq), std::move(curBatch));
            } catch (const mongocxx::bulk_write_exception& ex) {
                // TODO(SERVER-81325): Use the exception details to determine whether this is a
                // network error or error coming from the data. For now we simply check if the
                // "writeErrors" field exists. If it does not exist in the response, we error out
                // the streamProcessor.
                const auto& rawServerError = ex.raw_server_error();
                if (!rawServerError ||
                    rawServerError->find(kWriteErrorsField) == rawServerError->end()) {
                    LOGV2_INFO(74781,
                               "Error encountered while writing to target in MergeOperator",
                               "ns"_attr = outputNs,
                               "exception"_attr = ex.what());
                    uasserted(
                        74780,
                        fmt::format("Error encountered in {} while writing to target db: {} and "
                                    "collection: {}",
                                    getName(),
                                    outputNs.dbName().toStringForErrorMsg(),
                                    outputNs.coll()));
                }

                // The writeErrors field exists so we use it to determine which specific documents
                // caused the write error.
                int32_t writeErrorIndex = getWriteErrorIndexFromRawServerError(*rawServerError);
                invariant(startIdx + writeErrorIndex < curIdx);

                // Add the doc that encountered a write error to the dlq.
                const auto& streamDoc = dataMsg.docs[docIndices[startIdx + writeErrorIndex]];
                std::string error = str::stream()
                    << "Failed to process an input document in the current batch in " << getName()
                    << " with error: " << ex.what();
                _context->dlq->addMessage(
                    toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));
                incOperatorStats({.numDlqDocs = 1});

                // Now reprocess the remaining docs in the current batch individually.
                for (size_t i = startIdx + writeErrorIndex + 1; i < curIdx; ++i) {
                    if (badDocIndexes.contains(docIndices[i])) {
                        continue;
                    }
                    processStreamDocs(dataMsg,
                                      outputNs,
                                      {docIndices[i]},
                                      /*maxBatchDocSize*/ 1);
                }
            }
        }

        // Process the remaining docs in 'dataMsg'.
        startIdx = curIdx;
    }
}

}  // namespace streams
