/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/generated_data_source_operator.h"

#include "streams/exec/context.h"
#include "streams/exec/source_buffer_manager.h"
#include "streams/exec/util.h"

namespace streams {

using namespace mongo;

GeneratedDataSourceOperator::GeneratedDataSourceOperator(Context* context, int32_t numOutputs)
    : SourceOperator(context, numOutputs) {}

void GeneratedDataSourceOperator::doStart() {
    const auto& opts = getOptions();
    if (opts.useWatermarks) {
        _watermarkGenerator =
            std::make_unique<DelayedWatermarkGenerator>(0 /* inputIdx */, nullptr /* combiner */);
    }
}

int64_t GeneratedDataSourceOperator::doRunOnce() {
    int64_t numDocsFlushed{0};

    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto msgs = getMessages(lock);
    bool emptyBatch = msgs.empty();
    for (auto& msg : msgs) {
        if (msg.dataMsg) {
            auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
            dataMsg.docs.reserve(msg.dataMsg->docs.size());

            int64_t numInputDocs = msg.dataMsg->docs.size();
            int64_t numInputBytes{0};
            int64_t numDlqDocs{0};
            for (auto& doc : msg.dataMsg->docs) {
                auto mutatedDoc = processDocument(std::move(doc));
                if (!mutatedDoc) {
                    ++numDlqDocs;
                    continue;
                }

                numInputBytes += mutatedDoc->doc.getCurrentApproximateSize();
                dataMsg.docs.push_back(std::move(*mutatedDoc));
            }

            boost::optional<StreamControlMsg> controlMsg;
            if (_watermarkGenerator) {
                auto nextControlMsg = StreamControlMsg{_watermarkGenerator->getWatermarkMsg()};
                if (nextControlMsg != _lastControlMsg) {
                    _lastControlMsg = nextControlMsg;
                    controlMsg = std::move(nextControlMsg);
                }
            }

            incOperatorStats(OperatorStats{.numInputDocs = numInputDocs,
                                           .numInputBytes = numInputBytes,
                                           .numDlqDocs = numDlqDocs,
                                           .timeSpent = dataMsg.creationTimer->elapsed()});

            if (_watermarkGenerator) {
                _stats.watermark = _watermarkGenerator->getWatermarkMsg().eventTimeWatermarkMs;
            }

            numDocsFlushed += dataMsg.docs.size();
            sendDataMsg(
                /*outputIdx*/ 0, std::move(dataMsg), std::move(controlMsg));
        }

        if (msg.controlMsg) {
            sendControlMsg(/*outputIdx*/ 0, std::move(msg.controlMsg.get()));
        }
    }

    // Report current memory usage to SourceBufferManager.
    _context->sourceBufferManager->allocPages(
        _sourceBufferHandle.get(), _stats.memoryUsageBytes /* curSize */, 0 /* numPages */);

    if (getOptions().sendIdleMessages && emptyBatch) {
        // If _options.sendIdleMessages is set, always send a kIdle watermark when
        // there are no docs in the batch.
        StreamControlMsg msg{.watermarkMsg =
                                 WatermarkControlMsg{.watermarkStatus = WatermarkStatus::kIdle}};
        _lastControlMsg = msg;
        sendControlMsg(0, std::move(msg));
    }

    return numDocsFlushed;
}

boost::optional<StreamDocument> GeneratedDataSourceOperator::processDocument(StreamDocument doc) {
    Date_t timestamp;
    int64_t timestampMs{0};

    try {
        timestamp = getTimestamp(doc);
        timestampMs = timestamp.toMillisSinceEpoch();
    } catch (const DBException& ex) {
        auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
            _context->streamMetaFieldName, doc, getName(), std::string(ex.what())));
        incOperatorStats({.numDlqBytes = numDlqBytes});
        return boost::none;
    }

    if (_watermarkGenerator) {
        _watermarkGenerator->onEvent(timestampMs);
    }

    MutableDocument mutableDoc(std::move(doc.doc));
    mutableDoc[getOptions().timestampOutputFieldName] = Value(timestamp);
    if (!doc.streamMeta.getSource()) {
        StreamMetaSource streamMetaSource;
        doc.streamMeta.setSource(streamMetaSource);
    }
    doc.streamMeta.getSource()->setType(StreamMetaSourceTypeEnum::Generated);
    if (_context->shouldProjectStreamMetaPriorToSinkStage()) {
        auto newStreamMeta = updateStreamMeta(
            mutableDoc.peek().getField(*_context->streamMetaFieldName), doc.streamMeta);
        mutableDoc.setField(*_context->streamMetaFieldName, Value(std::move(newStreamMeta)));
    }

    doc.doc = mutableDoc.freeze();
    doc.minProcessingTimeMs = curTimeMillis64();
    doc.minEventTimestampMs = timestampMs;
    doc.maxEventTimestampMs = timestampMs;

    return doc;
}

Date_t GeneratedDataSourceOperator::getTimestamp(const StreamDocument& doc) const {
    if (doc.minEventTimestampMs >= 0) {
        return Date_t::fromMillisSinceEpoch(doc.minEventTimestampMs);
    }

    const auto& opts = getOptions();
    if (opts.timestampExtractor) {
        return opts.timestampExtractor->extractTimestamp(doc.doc);
    } else {
        // Fallback to server timestamp if theres no timestamp extractor set
        // for the stream processor.
        return Date_t::now();
    }
}

}  // namespace streams
