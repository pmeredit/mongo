/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/merge_operator.h"

#include <exception>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/exception.hpp>

#include "mongo/bson/json.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/merge_processor.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/stdx/unordered_set.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

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

    // Since we apply the writes in ordered manner there should only be 1 failed write and
    // all the writes before it should have succeeded.
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
      _processor(_options.documentSource->getMergeProcessor()) {}

void MergeOperator::doSinkOnDataMsg(int32_t inputIdx,
                                    StreamDataMsg dataMsg,
                                    boost::optional<StreamControlMsg> controlMsg) {
    processStreamDocs(dataMsg, /*startIdx*/ 0, /*endIdx*/ dataMsg.docs.size(), kDataMsgMaxDocSize);
}

void MergeOperator::processStreamDocs(const StreamDataMsg& dataMsg,
                                      size_t startIdx,
                                      size_t endIdx,
                                      size_t maxBatchDocSize) {
    invariant(endIdx <= dataMsg.docs.size());

    const auto maxBatchObjectSizeBytes = BSONObjMaxUserSize / 2;
    int32_t curBatchByteSize{0};
    // Create batches honoring the maxBatchDocSize and kDataMsgMaxByteSize size limits.
    while (startIdx < endIdx) {
        MongoProcessInterface::BatchedObjects curBatch;
        // [startIdx, curIdx) range determines the current batch.
        size_t curIdx{startIdx};
        stdx::unordered_set<size_t> badDocIndexes;
        while (curIdx < endIdx) {
            const auto& streamDoc = dataMsg.docs[curIdx++];
            try {
                auto docSize = streamDoc.doc.getCurrentApproximateSize();
                uassert(ErrorCodes::InternalError,
                        str::stream()
                            << "Output document is too large (" << (docSize / 1024) << "KB)",
                        docSize <= maxBatchObjectSizeBytes);

                auto batchObject = _processor->makeBatchObject(std::move(streamDoc.doc));
                curBatch.push_back(std::move(batchObject));
                curBatchByteSize += docSize;
                if (curBatch.size() == maxBatchDocSize || curBatchByteSize >= kDataMsgMaxByteSize) {
                    // Current batch is ready to flush.
                    break;
                }
            } catch (const DBException& e) {
                invariant(curIdx > startIdx);
                badDocIndexes.insert(curIdx - 1);
                std::string error = str::stream() << "Failed to process input document in "
                                                  << getName() << " with error: " << e.what();
                _context->dlq->addMessage(
                    toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));
            }
        }

        if (!curBatch.empty())
            try {
                auto batchedCommandReq =
                    _processor->getMergeStrategyDescriptor().batchedCommandGenerator(
                        _context->expCtx, _processor->getOutputNs());
                _processor->flush(std::move(batchedCommandReq), std::move(curBatch));
            } catch (const mongocxx::bulk_write_exception& ex) {
                const auto& rawServerError = ex.raw_server_error();
                uassert(ErrorCodes::InternalError,
                        "bulk_write_exception does not contain a raw_server_error",
                        rawServerError);

                int32_t writeErrorIndex = getWriteErrorIndexFromRawServerError(*rawServerError);
                invariant(startIdx + writeErrorIndex < curIdx);

                // Add the doc that encountered a write error to the dlq.
                const auto& streamDoc = dataMsg.docs[startIdx + writeErrorIndex];
                std::string error = str::stream()
                    << "Failed to process an input document in the current batch in " << getName()
                    << " with error: " << ex.what();
                _context->dlq->addMessage(
                    toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));

                // Now reprocess the remaining docs in the current batch individually.
                for (size_t i = startIdx + writeErrorIndex + 1; i < curIdx; ++i) {
                    if (badDocIndexes.contains(i)) {
                        continue;
                    }
                    processStreamDocs(
                        dataMsg, /*startIdx*/ i, /*endIdx*/ i + 1, /*maxBatchDocSize*/ 1);
                }
            }

        // Process the remaining docs in 'dataMsg'.
        startIdx = curIdx;
    }
}

}  // namespace streams
