/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/merge_operator.h"

#include "streams/exec/message.h"
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
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/namespace_string_util.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/log_util.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

MergeOperator::MergeOperator(Context* context, Options options)
    : QueuedSinkOperator(context, 1 /* numInputs */),
      _options(std::move(options)),
      _processor(_options.documentSource->getMergeProcessor()) {}

OperatorStats MergeOperator::processDataMsg(StreamDataMsg dataMsg) {
    // Partitions the docs in 'dataMsg' based on their target namespaces.
    OperatorStats stats;
    auto [docPartitions, partitionStats] = partitionDocsByTargets(dataMsg);
    stats += partitionStats;

    // Process each document partition.
    auto mongoProcessInterface =
        dynamic_cast<MongoDBProcessInterface*>(_options.mergeExpCtx->mongoProcessInterface.get());
    invariant(mongoProcessInterface);

    for (const auto& [nsKey, docIndices] : docPartitions) {
        auto outputNs = getNamespaceString(/*dbStr*/ nsKey.first, /*collStr*/ nsKey.second);
        // Create necessary collection instances first so that we need not do that in
        // MongoDBProcessInterface::ensureFieldsUniqueOrResolveDocumentKey() which is
        // declared const.
        mongoProcessInterface->ensureCollectionExists(outputNs);
        stats += processStreamDocs(dataMsg, outputNs, docIndices, kDataMsgMaxDocSize);
    }

    return stats;
}

auto MergeOperator::partitionDocsByTargets(const StreamDataMsg& dataMsg)
    -> std::tuple<DocPartitions, OperatorStats> {
    OperatorStats stats;
    auto getNsKey = [&](const StreamDocument& streamDoc) -> boost::optional<NsKey> {
        const auto& doc = streamDoc.doc;
        try {
            return std::make_pair(_options.db.evaluate(_options.mergeExpCtx.get(), doc),
                                  _options.coll.evaluate(_options.mergeExpCtx.get(), doc));
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to evaluate target namespace in "
                                              << getName() << " with error: " << e.what();
            stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc.streamMeta, std::move(error)));
            ++stats.numDlqDocs;
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

    return {docPartitions, stats};
}

void MergeOperator::validateConnection() {
    if (_options.coll.isLiteral() && _options.db.isLiteral()) {
        auto mongoProcessInterface = dynamic_cast<MongoDBProcessInterface*>(
            _options.mergeExpCtx->mongoProcessInterface.get());

        // If the target is literal, validate that the connection works.
        auto outputNs = getNamespaceString(_options.db.getLiteral(), _options.coll.getLiteral());
        auto validateFunc = [this, outputNs, mongoProcessInterface]() {
            // Test the connection to the target.
            mongoProcessInterface->testConnection(outputNs);

            if (_options.onFieldPaths) {
                // If the $merge.on field is specified, validate that the on fields have unique
                // indexes.
                mongoProcessInterface->fetchCollection(outputNs);
                auto result = mongoProcessInterface->ensureFieldsUniqueOrResolveDocumentKey(
                    _options.mergeExpCtx,
                    _options.onFieldPaths,
                    /*targetCollectionPlacementVersion*/ boost::none,
                    outputNs);
                _literalMergeOnFieldPaths = std::move(result.first);
            }
        };

        auto status = runMongocxxNoThrow(
            std::move(validateFunc),
            _context,
            ErrorCodes::Error{8619002},
            fmt::format("Failed to connect to $merge to {}", outputNs.toStringForErrorMsg()),
            mongoProcessInterface->uri());

        if (!status.isOK()) {
            uasserted(status.code(), status.reason());
        }
    }
}

OperatorStats MergeOperator::processStreamDocs(const StreamDataMsg& dataMsg,
                                               const NamespaceString& outputNs,
                                               const DocIndices& docIndices,
                                               size_t maxBatchDocSize) {
    OperatorStats stats;
    auto mongoProcessInterface =
        dynamic_cast<MongoDBProcessInterface*>(_options.mergeExpCtx->mongoProcessInterface.get());

    std::set<FieldPath> dynamicMergeOnFieldPaths;
    if (!_literalMergeOnFieldPaths) {
        try {
            // For the given 'on' field paths, retrieve the list of field paths that can be used to
            // uniquely identify the doc.
            dynamicMergeOnFieldPaths = mongoProcessInterface
                                           ->ensureFieldsUniqueOrResolveDocumentKey(
                                               _options.mergeExpCtx,
                                               _options.onFieldPaths,
                                               /*targetCollectionPlacementVersion*/ boost::none,
                                               outputNs)
                                           .first;
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            // Add all the docs to the dlq.
            for (size_t docIdx : docIndices) {
                const auto& streamDoc = dataMsg.docs[docIdx];
                stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                    _context->streamMetaFieldName, streamDoc.streamMeta, error));
                ++stats.numDlqDocs;
            }
            return stats;
        }
    }

    auto& mergeOnFieldPaths =
        _literalMergeOnFieldPaths ? *_literalMergeOnFieldPaths : dynamicMergeOnFieldPaths;
    bool mergeOnFieldPathsIncludeId{mergeOnFieldPaths.contains(kIdFieldName)};
    const auto maxBatchObjectSizeBytes = BSONObjMaxUserSize;
    int32_t curBatchByteSize{0};
    // Create batches honoring the maxBatchDocSize and kDataMsgMaxByteSize size limits.
    size_t startIdx = 0;
    bool samplersPresent = samplersExist();
    while (startIdx < docIndices.size()) {
        MongoProcessInterface::BatchedObjects curBatch;

        // [startIdx, curIdx) range determines the current batch.
        size_t curIdx{startIdx};
        stdx::unordered_set<size_t> badDocIndexes;
        while (curIdx < docIndices.size()) {
            const auto& streamDoc = dataMsg.docs[docIndices[curIdx++]];
            try {
                auto docSize =
                    streamDoc.doc.memUsageForSorter();  // looks like getApproximateCurrentSize
                                                        // can be incorrect for documents unwound
                uassert(ErrorCodes::InternalError,
                        str::stream()
                            << "Output document is too large (" << (docSize / 1024) << "KB)",
                        docSize <= maxBatchObjectSizeBytes);

                auto batchObject = _processor->makeBatchObject(
                    streamDoc.doc, mergeOnFieldPaths, mergeOnFieldPathsIncludeId);
                curBatch.push_back(std::move(batchObject));
                curBatchByteSize += docSize;
                if (curBatch.size() == maxBatchDocSize || curBatchByteSize >= kDataMsgMaxByteSize) {
                    // Current batch is ready to flush.
                    break;
                }
            } catch (const DBException& e) {
                invariant(curIdx >= startIdx);
                badDocIndexes.insert(docIndices[curIdx - 1]);
                std::string error = str::stream()
                    << "Failed to process input document in " << getName()
                    << " with error: code = " << e.codeString() << ", reason = " << e.reason();
                stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                    _context->streamMetaFieldName, streamDoc.streamMeta, std::move(error)));
                ++stats.numDlqDocs;
            }
        }

        /*
         * All following code assumes that the 'curIdx' points to the next element to process.
         */

        if (!curBatch.empty()) {
            try {
                int64_t curBatchSize = curBatch.size();
                auto batchedCommandReq =
                    _processor->getMergeStrategyDescriptor().batchedCommandGenerator(
                        _options.mergeExpCtx, outputNs);

                auto now = stdx::chrono::steady_clock::now();
                _processor->flush(outputNs, std::move(batchedCommandReq), std::move(curBatch));
                stats.numOutputDocs += curBatchSize;
                stats.numOutputBytes += curBatchByteSize;
                auto elapsed = stdx::chrono::steady_clock::now() - now;
                _writeLatencyMs->increment(
                    stdx::chrono::duration_cast<stdx::chrono::milliseconds>(elapsed).count());
                if (samplersPresent) {
                    StreamDataMsg msg;
                    msg.docs.reserve(curBatchSize);
                    for (int i = 0; i < curBatchSize; i++) {
                        msg.docs.push_back(dataMsg.docs[docIndices[startIdx + i]]);
                    }
                    sendOutputToSamplers(std::move(msg));
                }
            } catch (const mongocxx::operation_exception& ex) {
                // TODO(SERVER-81325): Use the exception details to determine whether this is a
                // network error or error coming from the data. For now we simply check if the
                // "writeErrors" field exists. If it does not exist in the response, we error out
                // the streamProcessor.
                const auto& rawServerError = ex.raw_server_error();
                if (!rawServerError ||
                    rawServerError->find(kWriteErrorsFieldName) == rawServerError->end()) {
                    LOGV2_INFO(74781,
                               "Error encountered while writing to target in MergeOperator",
                               "ns"_attr = outputNs,
                               "context"_attr = _context,
                               "exception"_attr = ex.what());
                    uasserted(
                        ex.code().value(),
                        fmt::format(
                            "Error encountered in {} while writing to target db: {} and "
                            "collection: {}: {}",
                            getName(),
                            outputNs.dbName().toStringForErrorMsg(),
                            outputNs.coll(),
                            sanitizeMongocxxErrorMsg(ex.what(), mongoProcessInterface->uri())));
                }

                // The writeErrors field exists so we use it to determine which specific documents
                // caused the write error.
                auto writeError = getWriteErrorIndexFromRawServerError(*rawServerError);
                size_t writeErrorIndex = writeError.getIndex();
                stats.numOutputDocs += writeErrorIndex;
                StreamDataMsg msg;
                if (samplersPresent && writeErrorIndex > 0) {
                    msg.docs.reserve(writeErrorIndex);
                }
                for (size_t i = 0; i < writeErrorIndex; i++) {
                    stats.numOutputBytes +=
                        dataMsg.docs[docIndices[startIdx + i]].doc.memUsageForSorter();
                    if (samplersPresent) {
                        msg.docs.push_back(dataMsg.docs[docIndices[startIdx + i]]);
                    }
                }
                if (samplersPresent) {
                    sendOutputToSamplers(std::move(msg));
                }
                invariant(startIdx + writeErrorIndex < curIdx);

                // Add the doc that encountered a write error to the dlq.
                const auto& streamDoc = dataMsg.docs[docIndices[startIdx + writeErrorIndex]];
                std::string error = str::stream()
                    << "Failed to process an input document in the current batch in " << getName()
                    << " with error: code = " << writeError.getStatus().codeString()
                    << ", reason = " << writeError.getStatus().reason();
                stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                    _context->streamMetaFieldName, streamDoc.streamMeta, std::move(error)));
                ++stats.numDlqDocs;

                // Now reprocess the remaining docs in the current batch individually.
                for (size_t i = startIdx + writeErrorIndex + 1; i < curIdx; ++i) {
                    if (badDocIndexes.contains(docIndices[i])) {
                        continue;
                    }
                    stats += processStreamDocs(dataMsg,
                                               outputNs,
                                               {docIndices[i]},
                                               /*maxBatchDocSize*/ 1);
                }
            }
        }

        // Process the remaining docs in 'dataMsg'.
        startIdx = curIdx;
        curBatchByteSize = 0;
    }

    return stats;
}

}  // namespace streams
