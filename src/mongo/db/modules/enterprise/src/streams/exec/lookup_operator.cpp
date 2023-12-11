/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/lookup_operator.h"

#include <mongocxx/exception/exception.hpp>

#include "mongo/db/pipeline/document_source_lookup.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

LookUpOperator::LookUpOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _localField(*_options.documentSource->getLocalField()),
      _foreignField(*_options.documentSource->getForeignField()),
      _asField(_options.documentSource->getAsField()),
      _memoryUsageHandle(context->memoryAggregator->createUsageHandle()) {
    const auto& unwindSource = _options.documentSource->getUnwindSource();
    if (unwindSource) {
        _shouldUnwind = true;
        _unwindIndexPath = unwindSource->indexPath();
        _unwindPreservesNullAndEmptyArrays = unwindSource->preserveNullAndEmptyArrays();
    }
    const auto& additionalFilter = _options.documentSource->getAdditionalFilter();
    if (additionalFilter) {
        invariant(_shouldUnwind);
        _additionalFilter = *additionalFilter;
    }
}

void LookUpOperator::doOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) {
    int32_t curDataMsgByteSize{0};
    auto newStreamDataMsg = [&]() {
        StreamDataMsg outputMsg;
        outputMsg.docs.reserve(dataMsg.docs.size());
        curDataMsgByteSize = 0;
        _memoryUsageHandle.set(0);
        return outputMsg;
    };

    // Resets any state tracked for the previous doc.
    auto resetPreviousCursor = [&]() {
        _unwindCurIndex = 0;
        _previousCursorIter.reset();
        _previousCursor.reset();
    };

    int32_t curInputDocIdx{-1};
    StreamDataMsg outputMsg = newStreamDataMsg();
    while (true) {
        if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
            curDataMsgByteSize >= kDataMsgMaxByteSize) {
            // Make sure to not wrap sendDataMsg() calls with a try/catch block.
            sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
            outputMsg = newStreamDataMsg();
        }

        try {
            if (_previousCursorIter) {
                // Get the next doc from '_previousCursorIter'.
                invariant(_shouldUnwind);
                auto& streamDoc = dataMsg.docs[curInputDocIdx];
                if (*_previousCursorIter != _previousCursor->end()) {
                    auto& inputDoc = dataMsg.docs[curInputDocIdx].doc;
                    auto foreignDoc =
                        getNextDocFromPreviousCursorIter(dataMsg.docs[curInputDocIdx]);
                    if (foreignDoc) {
                        auto outputDoc = produceJoinedDoc(inputDoc, std::move(*foreignDoc));
                        curDataMsgByteSize += outputDoc.getApproximateSize();
                        ++_unwindCurIndex;
                        StreamDocument outputStreamDoc(std::move(outputDoc));
                        outputStreamDoc.copyDocumentMetadata(streamDoc);
                        outputMsg.docs.emplace_back(std::move(outputStreamDoc));
                    } else {
                        // Encountered an error, reset the cursor and the iterator so that we
                        // process the next doc in the next iteration.
                        resetPreviousCursor();
                    }
                } else if (_unwindCurIndex == 0 && _unwindPreservesNullAndEmptyArrays) {
                    auto& inputDoc = dataMsg.docs[curInputDocIdx].doc;
                    auto outputDoc = produceJoinedDoc(std::move(inputDoc), Value());
                    curDataMsgByteSize += outputDoc.getApproximateSize();
                    StreamDocument outputStreamDoc(std::move(outputDoc));
                    outputStreamDoc.copyDocumentMetadata(streamDoc);
                    outputMsg.docs.emplace_back(std::move(outputStreamDoc));
                    resetPreviousCursor();
                } else {
                    resetPreviousCursor();
                }
                continue;
            }

            // Process the next input doc.
            ++curInputDocIdx;
            if (curInputDocIdx >= int32_t(dataMsg.docs.size())) {
                break;
            }

            auto& streamDoc = dataMsg.docs[curInputDocIdx];
            auto cursor = createCursor(streamDoc);
            if (cursor) {
                if (_shouldUnwind) {
                    _previousCursor = std::move(*cursor);
                    _previousCursorIter = _previousCursor->begin();
                } else {
                    auto results = getAllDocsFromCursor(streamDoc, std::move(*cursor));
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
            auto numDlqBytes =
                _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc, std::move(error)));
            incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
        }
    }

    // Make sure to not wrap sendDataMsg() calls with a try/catch block.
    if (!outputMsg.docs.empty()) {
        sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
    } else if (controlMsg) {
        doOnControlMsg(inputIdx, std::move(*controlMsg));
    }
}

void LookUpOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // This operator just passes through any control messages it sees.
    sendControlMsg(inputIdx, std::move(controlMsg));
}

boost::optional<mongocxx::cursor> LookUpOperator::createCursor(const StreamDocument& streamDoc) {
    try {
        auto matchStage = DocumentSourceLookUp::makeMatchStageFromInput(
            streamDoc.doc, _localField, _foreignField.fullPath(), _additionalFilter);
        auto filter = matchStage.firstElement().Obj();
        return _options.foreignMongoDBClient->query(_context->expCtx, _options.foreignNs, filter);
    } catch (const mongocxx::exception& ex) {
        std::string error = str::stream()
            << "Failed to process input document in " << getName() << " with error: " << ex.what();
        auto numDlqBytes =
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc, std::move(error)));
        incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
        return boost::none;
    }
}

boost::optional<std::vector<Value>> LookUpOperator::getAllDocsFromCursor(
    const StreamDocument& streamDoc, mongocxx::cursor cursor) {
    try {
        std::vector<Value> results;
        for (const auto& doc : cursor) {
            BSONObj obj = fromBsoncxxDocument(doc);
            _memoryUsageHandle.add(obj.objsize());
            results.emplace_back(std::move(obj));
        }
        return results;
    } catch (const mongocxx::exception& ex) {
        std::string error = str::stream()
            << "Failed to process input document in " << getName() << " with error: " << ex.what();
        auto numDlqBytes =
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc, std::move(error)));
        incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
        return boost::none;
    }
}

boost::optional<Value> LookUpOperator::getNextDocFromPreviousCursorIter(
    const StreamDocument& streamDoc) {
    try {
        auto foreignDoc = Value(fromBsoncxxDocument(**_previousCursorIter));
        ++(*_previousCursorIter);
        return foreignDoc;
    } catch (const mongocxx::exception& ex) {
        std::string error = str::stream()
            << "Failed to process input document in " << getName() << " with error: " << ex.what();
        auto numDlqBytes =
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc, std::move(error)));
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
