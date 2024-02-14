/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/window_pipeline.h"

#include <boost/intrusive_ptr.hpp>
#include <boost/optional.hpp>
#include <boost/proto/matches.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "mongo/util/assert_util.h"
#include "streams/exec/collect_operator.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/message.h"
#include "streams/exec/util.h"

namespace streams {

using namespace mongo;

namespace {

Date_t toDate(int64_t ms) {
    return Date_t::fromMillisSinceEpoch(ms);
}

}  // namespace

WindowPipeline::WindowPipeline(Context* context, Options options)
    : _context(context), _options(std::move(options)) {
    dassert(_options.endMs > _options.startMs);

    invariant(!_options.operators.empty());
    invariant(dynamic_cast<CollectOperator*>(_options.operators.back().get()));

    // Like done in OperatorDag, start the operators in reverse order.
    for (auto iter = _options.operators.rbegin(); iter != _options.operators.rend(); ++iter) {
        (*iter)->start();
    }
}

void WindowPipeline::process(StreamDataMsg dataMsg) {
    if (_error) {
        // Processing for this window already ran into an error, skip processing any more
        // documents for this window.
        return;
    }

    for (auto& doc : dataMsg.docs) {
        if (!_streamMetaTemplate) {
            _streamMetaTemplate = doc.streamMeta;
        }
        doc.streamMeta.setWindowStartTimestamp(toDate(_options.startMs));
        doc.streamMeta.setWindowEndTimestamp(toDate(_options.endMs));
        _minObservedEventTimeMs = std::min(_minObservedEventTimeMs, doc.minEventTimestampMs);
        _maxObservedEventTimeMs = std::max(_maxObservedEventTimeMs, doc.minEventTimestampMs);
        _minObservedProcessingTime = std::min(_minObservedProcessingTime, doc.minProcessingTimeMs);
    }

    try {
        // Send the docs through the pipeline.
        _options.operators.front()->onDataMsg(/*inputIdx*/ 0, std::move(dataMsg));
    } catch (const DBException& e) {
        _error = str::stream() << "Failed to process input document in window"
                               << " with error: " << e.what();
    }
}

OperatorStats WindowPipeline::close() {
    // Close the operators and retrieve stats
    OperatorStats opStats;
    for (auto& oper : _options.operators) {
        opStats += oper->getStats();
        oper->stop();
    }

    return opStats;
}

bool WindowPipeline::isEof() const {
    return _reachedEof;
}

std::queue<StreamDataMsg> WindowPipeline::getNextOutputDataMsgs(bool eof) {
    if (_reachedEof) {
        return {};
    }

    auto collectOp = dynamic_cast<CollectOperator*>(_options.operators.back().get());
    std::deque<StreamMsgUnion> msgs = collectOp->getMessages();

    // Keep sending EOF signal until there are messages available in the sink collect
    // operator. Each EOF signal sent will trigger the receiving operator to push only
    // a single batch to the next operator. That former operator will then send a EOF
    // signal to the next operator after it's pushed all documents. Then this will
    // keep going until the last operator in the pipeline emits a EOF signal to the
    // collect operator, which signals that all output documents have been drained from
    // this window pipeline.
    while (msgs.empty() && eof) {
        StreamControlMsg controlMsg{.eofSignal = true};

        try {
            _options.operators.front()->onControlMsg(/*inputIdx*/ 0, std::move(controlMsg));
        } catch (const DBException& e) {
            _error = str::stream() << "Failed to process an input document for this window"
                                   << " with error: " << e.what();
        }

        msgs = collectOp->getMessages();
    }

    std::queue<StreamDataMsg> out;
    for (auto& msg : msgs) {
        if (msg.dataMsg) {
            for (size_t i = 0; i < msg.dataMsg->docs.size(); ++i) {
                auto& streamDoc = msg.dataMsg->docs[i];
                msg.dataMsg->docs[i] = toOutputDocument(std::move(streamDoc));
            }

            out.push(std::move(*msg.dataMsg));
        }

        if (msg.controlMsg && msg.controlMsg->eofSignal) {
            _reachedEof = true;
        }
    }

    return out;
}

mongo::BSONObjBuilder WindowPipeline::getDeadLetterQueueMsg() const {
    dassert(_error);
    dassert(_streamMetaTemplate);
    StreamMeta streamMeta;
    streamMeta.setSourceType(_streamMetaTemplate->getSourceType());
    streamMeta.setWindowStartTimestamp(toDate(_options.startMs));
    streamMeta.setWindowEndTimestamp(toDate(_options.endMs));
    return streams::toDeadLetterQueueMsg(
        _context->streamMetaFieldName, std::move(streamMeta), _error);
}

StreamDocument WindowPipeline::toOutputDocument(StreamDocument streamDoc) {
    dassert(_streamMetaTemplate);
    streamDoc.streamMeta.setSourceType(_streamMetaTemplate->getSourceType());
    streamDoc.streamMeta.setWindowStartTimestamp(toDate(_options.startMs));
    streamDoc.streamMeta.setWindowEndTimestamp(toDate(_options.endMs));
    streamDoc.minProcessingTimeMs = _minObservedProcessingTime;
    streamDoc.minEventTimestampMs = _minObservedEventTimeMs;
    streamDoc.maxEventTimestampMs = _maxObservedEventTimeMs;
    return streamDoc;
}

OperatorStats WindowPipeline::getStats() const {
    OperatorStats out;
    for (const auto& oper : _options.operators) {
        out += oper->getStats();
    }
    return out;
}

}  // namespace streams
