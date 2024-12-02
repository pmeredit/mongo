/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/merge_operator.h"

#include "mongo/util/duration.h"
#include "streams/exec/message.h"
#include "streams/util/exception.h"
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
      _processor(_options.documentSource->getMergeProcessor()) {
    _stats.connectionType = ConnectionTypeEnum::Atlas;
}

OperatorStats MergeOperator::processDataMsg(StreamDataMsg dataMsg) {
    // Partitions the docs in 'dataMsg' based on their target namespaces.
    OperatorStats stats;
    auto [docPartitions, partitionStats] = partitionDocsByTargets(dataMsg);
    stats += partitionStats;

    // Process each document partition.
    auto mongoProcessInterface = dynamic_cast<MongoDBProcessInterface*>(
        _options.mergeExpCtx->getMongoProcessInterface().get());
    invariant(mongoProcessInterface);

    for (const auto& [nsKey, docIndices] : docPartitions) {
        auto outputNs = getNamespaceString(/*dbStr*/ nsKey.first, /*collStr*/ nsKey.second);
        // Create necessary collection instances first so that we need not do that in
        // MongoDBProcessInterface::ensureFieldsUniqueOrResolveDocumentKey() which is
        // declared const.
        try {
            // This might fail due to auth or connection reasons.
            // This won't throw an exception if the collection doesn't exist.
            mongoProcessInterface->ensureCollectionExists(outputNs);
        } catch (const mongocxx::exception& e) {
            errorOut(e, outputNs);
        }
        stats += processStreamDocs(dataMsg, outputNs, docIndices, kSinkDataMsgMaxDocSize);
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
                _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
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
            _options.mergeExpCtx->getMongoProcessInterface().get());

        // If the target is literal, validate that the connection works.
        auto outputNs = getNamespaceString(_options.db.getLiteral(), _options.coll.getLiteral());
        auto validateFunc = [this, outputNs, mongoProcessInterface]() {
            // Test the connection to the target.
            mongoProcessInterface->testConnection(outputNs);

            if (_options.onFieldPaths) {
                MongoDBProcessInterface::DocumentKeyResolutionMetadata result;
                try {
                    // If the $merge.on field is specified, validate that the on fields have unique
                    // indexes.
                    result = mongoProcessInterface->ensureFieldsUniqueOrResolveDocumentKey(
                        _options.mergeExpCtx,
                        _options.onFieldPaths,
                        /*targetCollectionPlacementVersion*/ boost::none,
                        outputNs);
                } catch (DBException& e) {
                    e.addContext("Error occured while validating $merge.on");
                    throw;
                }
                _literalMergeOnFieldPaths = std::move(std::get<0>(result));
            }
        };

        auto status = runMongocxxNoThrow(std::move(validateFunc),
                                         _context,
                                         ErrorCodes::Error{8619002},
                                         getErrorPrefix(outputNs),
                                         mongoProcessInterface->uri());
        spassert(status, status.isOK());
    }
}

void MergeOperator::errorOut(const mongocxx::exception& e, const mongo::NamespaceString& outputNs) {
    auto code = ErrorCodes::Error{e.code().value()};
    LOGV2_INFO(74781,
               "Error encountered in MergeOperator",
               "ns"_attr = outputNs,
               "context"_attr = _context,
               "exception"_attr = e.what(),
               "code"_attr = int(code));
    auto mongoProcessInterface = dynamic_cast<MongoDBProcessInterface*>(
        _options.mergeExpCtx->getMongoProcessInterface().get());
    invariant(mongoProcessInterface);
    SPStatus status =
        mongocxxExceptionToStatus(e, mongoProcessInterface->uri(), getErrorPrefix(outputNs));
    spasserted(status);
}

// Returns an error message prefix for the output namespace.
std::string MergeOperator::getErrorPrefix(const mongo::NamespaceString& outputNs) {
    return fmt::format("$merge to {} failed", outputNs.toStringForErrorMsg());
}

OperatorStats MergeOperator::processStreamDocs(const StreamDataMsg& dataMsg,
                                               const NamespaceString& outputNs,
                                               const DocIndices& docIndices,
                                               size_t maxBatchDocSize) {
    OperatorStats stats;
    auto mongoProcessInterface = dynamic_cast<MongoDBProcessInterface*>(
        _options.mergeExpCtx->getMongoProcessInterface().get());

    std::set<FieldPath> dynamicMergeOnFieldPaths;
    if (!_literalMergeOnFieldPaths) {
        try {
            // For the given 'on' field paths, retrieve the list of field paths that can be used to
            // uniquely identify the doc.
            dynamicMergeOnFieldPaths =
                std::get<0>(mongoProcessInterface->ensureFieldsUniqueOrResolveDocumentKey(
                    _options.mergeExpCtx,
                    _options.onFieldPaths,
                    /*targetCollectionPlacementVersion*/ boost::none,
                    outputNs));
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            // Add all the docs to the dlq.
            for (size_t docIdx : docIndices) {
                const auto& streamDoc = dataMsg.docs[docIdx];
                stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                    _context->streamMetaFieldName, streamDoc, getName(), error));
                ++stats.numDlqDocs;
            }
            return stats;
        }
    }

    auto& mergeOnFieldPaths =
        _literalMergeOnFieldPaths ? *_literalMergeOnFieldPaths : dynamicMergeOnFieldPaths;
    bool mergeOnFieldPathsIncludeId{mergeOnFieldPaths.contains(kIdFieldName)};
    const auto maxBatchSizeBytes = (3 * BSONObjMaxUserSize) / 4;
    int32_t curBatchByteSize{0};
    // Create batches honoring the maxBatchSizeBytes and kDataMsgMaxByteSize size limits.
    size_t startIdx = 0;
    bool samplersPresent = samplersExist();
    while (startIdx < docIndices.size()) {
        MongoProcessInterface::BatchedObjects curBatch;

        // [startIdx, curIdx) range determines the current batch.
        size_t curIdx{startIdx};
        stdx::unordered_set<size_t> badDocIndexes;
        for (; curIdx < docIndices.size(); curIdx++) {
            const auto& streamDoc = dataMsg.docs[docIndices[curIdx]];
            try {
                auto docSize =
                    streamDoc.doc.memUsageForSorter();  // looks like getApproximateCurrentSize
                                                        // can be incorrect for documents unwound
                // Check if there is any space left in the batch for the current document.
                if (curBatch.size() == maxBatchDocSize ||
                    ((docSize + curBatchByteSize) > maxBatchSizeBytes && curBatchByteSize > 0)) {
                    break;
                }

                // MergeProcessor::makeBatchObject validates the size of document.
                // Any document which is larger than the BSONObj max size limit,
                // will throw an exception.
                auto batchObject = _processor->makeBatchObject(
                    streamDoc.doc, mergeOnFieldPaths, mergeOnFieldPathsIncludeId);
                curBatch.push_back(std::move(batchObject));
                curBatchByteSize += docSize;
            } catch (const DBException& e) {
                invariant(curIdx >= startIdx);
                badDocIndexes.insert(docIndices[curIdx]);
                std::string error = str::stream()
                    << "Failed to process input document in " << getName()
                    << " with error: code = " << e.codeString() << ", reason = " << e.reason();
                stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                    _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
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
                auto writeError = getWriteErrorFromRawServerError(ex);
                if (!writeError) {
                    errorOut(ex, outputNs);
                }

                // The writeErrors field exists so we use it to determine which specific documents
                // caused the write error.
                size_t writeErrorIndex = writeError->getIndex();
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
                    << " with error: code = " << writeError->getStatus().codeString()
                    << ", reason = " << writeError->getStatus().reason();
                stats.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                    _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
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
            } catch (const mongocxx::exception& e) {
                // Data errors due to output (index violations etc.), will all get caught in the
                // block above. If we're here, something went wrong with the connection.
                errorOut(e, outputNs);
            }
        }

        // Process the remaining docs in 'dataMsg'.
        startIdx = curIdx;
        curBatchByteSize = 0;
    }

    stats.timeSpent = dataMsg.creationTimer->elapsed();
    return stats;
}

}  // namespace streams
