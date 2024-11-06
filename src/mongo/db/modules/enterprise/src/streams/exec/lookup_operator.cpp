/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/lookup_operator.h"

#include "mongo/util/duration.h"
#include <fmt/format.h>
#include <mongocxx/exception/exception.hpp>

#include "mongo/db/pipeline/document_source_lookup.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_source_remote_db_cursor.h"
#include "streams/exec/log_util.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

LookUpOperator::LookUpOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _asField(_options.documentSource->getAsField()) {
    _fromExpCtx = context->expCtx->copyForSubPipeline(
        _options.foreignNs.value_or(_options.documentSource->getFromNs()));
    const auto& unwindSource = _options.documentSource->getUnwindSource();
    if (unwindSource) {
        _shouldUnwind = true;
        _unwindIndexPath = unwindSource->indexPath();
        _unwindPreservesNullAndEmptyArrays = unwindSource->preserveNullAndEmptyArrays();
    }
    _stats.connectionType = ConnectionTypeEnum::Atlas;  // only supports Atlas
}

void LookUpOperator::registerMetrics(MetricManager* metricManager) {
    _lookupLatencyMs = metricManager->registerHistogram(
        "foreign_mongodb_lookup_latency_ms",
        /* description */ "Latency for lookup request.",
        /* labels */ getDefaultMetricLabels(_context),
        /* buckets */
        makeExponentialDurationBuckets(
            /* start */ stdx::chrono::milliseconds(5), /* factor */ 5, /* count */ 6));
}

void LookUpOperator::doOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) {
    using namespace fmt::literals;

    int32_t curDataMsgByteSize{0};
    auto newStreamDataMsg = [&]() {
        StreamDataMsg outputMsg;
        outputMsg.docs.reserve(dataMsg.docs.size());
        curDataMsgByteSize = 0;
        _memoryUsageHandle.set(0);
        return outputMsg;
    };

    int32_t curInputDocIdx{-1};
    StreamDataMsg outputMsg = newStreamDataMsg();
    while (true) {
        if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
            curDataMsgByteSize >= kDataMsgMaxByteSize) {
            // Make sure to not wrap sendDataMsg() calls with a try/catch block.
            incOperatorStats({.timeSpent = dataMsg.creationTimer->elapsed()});
            sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
            outputMsg = newStreamDataMsg();
        }

        try {
            if (_pipeline) {
                // Resets the '_pipeline' if it's exhausted or an error occurred.
                ScopeGuard resetPipeline{[&] {
                    _unwindCurIndex = 0;
                    _pipeline->dispose(_context->expCtx->opCtx);
                    _pipeline.reset();
                }};

                invariant(_shouldUnwind);
                auto& streamDoc = dataMsg.docs[curInputDocIdx];
                if (auto foreignDoc = getNextDocFromPipeline(dataMsg.docs[curInputDocIdx])) {
                    auto& inputDoc = dataMsg.docs[curInputDocIdx].doc;
                    auto outputDoc = produceJoinedDoc(inputDoc, std::move(*foreignDoc));
                    curDataMsgByteSize += outputDoc.getApproximateSize();
                    ++_unwindCurIndex;
                    StreamDocument outputStreamDoc(std::move(outputDoc));
                    outputStreamDoc.copyDocumentMetadata(streamDoc);
                    outputMsg.docs.emplace_back(std::move(outputStreamDoc));
                    // Should not reset the '_pipeline' until it's exhausted.
                    resetPipeline.dismiss();
                } else if (_unwindCurIndex == 0 && _unwindPreservesNullAndEmptyArrays) {
                    auto& inputDoc = dataMsg.docs[curInputDocIdx].doc;
                    auto outputDoc = produceJoinedDoc(std::move(inputDoc), Value());
                    curDataMsgByteSize += outputDoc.getApproximateSize();
                    StreamDocument outputStreamDoc(std::move(outputDoc));
                    outputStreamDoc.copyDocumentMetadata(streamDoc);
                    outputMsg.docs.emplace_back(std::move(outputStreamDoc));
                }
                continue;
            }

            // Process the next input doc.
            ++curInputDocIdx;
            if (curInputDocIdx >= int32_t(dataMsg.docs.size())) {
                break;
            }

            auto& streamDoc = dataMsg.docs[curInputDocIdx];
            if (auto pipeline = buildPipeline(streamDoc)) {
                if (_shouldUnwind) {
                    _pipeline = std::move(pipeline);
                } else {
                    auto results = getAllDocsFromPipeline(streamDoc, std::move(pipeline));
                    if (results) {
                        auto& inputDoc = streamDoc.doc;
                        auto outputDoc =
                            produceJoinedDoc(std::move(inputDoc), Value(std::move(*results)));
                        curDataMsgByteSize += outputDoc.getApproximateSize();
                        StreamDocument outputStreamDoc(std::move(outputDoc));
                        outputStreamDoc.copyDocumentMetadata(streamDoc);
                        outputMsg.docs.emplace_back(std::move(outputStreamDoc));
                    }
                }
            }
        } catch (const DBException& ex) {
            auto& streamDoc = dataMsg.docs[curInputDocIdx];
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << ex.what();
            auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
            incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
        }
    }

    // Make sure to not wrap sendDataMsg() calls with a try/catch block.
    if (!outputMsg.docs.empty()) {
        incOperatorStats({.timeSpent = dataMsg.creationTimer->elapsed()});
        sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
    } else if (controlMsg) {
        doOnControlMsg(inputIdx, std::move(*controlMsg));
    }
}

void LookUpOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // This operator just passes through any control messages it sees.
    sendControlMsg(inputIdx, std::move(controlMsg));
}

PipelinePtr LookUpOperator::buildPipeline(const StreamDocument& streamDoc) {
    try {
        auto pipeline = _options.documentSource->buildPipeline<true /*isStreamsEngine*/>(
            _fromExpCtx, streamDoc.doc);
        if (_options.foreignMongoDBClient) {
            return _options.foreignMongoDBClient->preparePipelineForExecution(pipeline.get());
        }
        return pipeline;
    } catch (const mongocxx::exception& ex) {
        std::string error = str::stream()
            << "Failed to process input document in " << getName() << " with error: " << ex.what();
        auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
            _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
        incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
        return nullptr;
    }
}

boost::optional<std::vector<Value>> LookUpOperator::getAllDocsFromPipeline(
    const StreamDocument& streamDoc, PipelinePtr pipeline) {
    try {
        std::vector<Value> results;
        auto now = stdx::chrono::steady_clock::now();
        while (auto result = pipeline->getNext()) {
            _memoryUsageHandle.add(result->getApproximateSize());
            results.emplace_back(std::move(*result));
        }
        auto elapsed = stdx::chrono::steady_clock::now() - now;
        _lookupLatencyMs->increment(
            stdx::chrono::duration_cast<stdx::chrono::milliseconds>(elapsed).count());
        return results;
    } catch (const mongocxx::exception& ex) {
        std::string error = str::stream()
            << "Failed to process input document in " << getName() << " with error: " << ex.what();
        auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
            _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
        incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
        return boost::none;
    }
}

boost::optional<Value> LookUpOperator::getNextDocFromPipeline(const StreamDocument& streamDoc) {
    try {
        auto result = _pipeline->getNext();
        if (!result) {
            return boost::none;
        }
        return Value(*result);
    } catch (const mongocxx::exception& ex) {
        std::string error = str::stream()
            << "Failed to process input document in " << getName() << " with error: " << ex.what();
        auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
            _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
        incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
        return boost::none;
    }
}

Document LookUpOperator::produceJoinedDoc(Document inputDoc, Value asFieldValue) {
    MutableDocument output(std::move(inputDoc));
    if (_unwindIndexPath) {
        if (asFieldValue.missing()) {
            invariant(_unwindPreservesNullAndEmptyArrays);
            invariant(_unwindCurIndex == 0);
            output.setNestedField(*_unwindIndexPath, Value(BSONNULL));
        } else {
            output.setNestedField(*_unwindIndexPath, Value(_unwindCurIndex));
        }
    }
    output.setNestedField(_asField, std::move(asFieldValue));
    return output.freeze();
}

}  // namespace streams
