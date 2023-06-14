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

const int kResultsBufferSize = 1024;

void addToResults(StreamDocument streamDoc, std::queue<StreamDataMsg>* results) {
    if (results->empty() || results->back().docs.size() == kResultsBufferSize) {
        results->emplace(StreamDataMsg{});
        results->back().docs.reserve(kResultsBufferSize);
    }
    results->back().docs.emplace_back(std::move(streamDoc));
}

}  // namespace

WindowPipeline::WindowPipeline(Context* context, Options options)
    : _context(context), _options(std::move(options)) {
    dassert(_options.endMs > _options.startMs);

    // Add a CollectOperator at the end of the operator chain to collect the documents
    // emitted at the end of the pipeline.
    dassert(!_options.operators.empty());
    auto sinkOperator = std::make_unique<CollectOperator>(_context, /*numInputs*/ 1);
    _options.operators.back()->addOutput(sinkOperator.get(), 0);
    _options.operators.push_back(std::move(sinkOperator));
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

    // Send the docs through the pipeline.
    _options.operators.front()->onDataMsg(/*inputIdx*/ 0, std::move(dataMsg));
}

std::queue<StreamDataMsg> WindowPipeline::close() {
    if (_error) {
        // Processing for this window already ran into an error, skip processing any more
        // documents for this window.
        return {};
    }

    // Send EOF signal to the pipeline.
    StreamControlMsg controlMsg{.pushDocumentSourceEofSignal = true};
    _options.operators.front()->onControlMsg(/*inputIdx*/ 0, std::move(controlMsg));

    auto sinkOperator = dynamic_cast<CollectOperator*>(_options.operators.back().get());
    auto messages = sinkOperator->getMessages();

    // Close the operators.
    for (auto& oper : _options.operators) {
        oper->stop();
    }

    std::queue<StreamDataMsg> results;
    while (!messages.empty()) {
        StreamMsgUnion msg = std::move(messages.front());
        messages.pop();
        if (msg.dataMsg) {
            for (auto& streamDoc : msg.dataMsg->docs) {
                addToResults(toOutputDocument(std::move(streamDoc)), &results);
            }
        }
    }
    return results;
}

mongo::BSONObjBuilder WindowPipeline::getDeadLetterQueueMsg() const {
    dassert(_error);
    dassert(_streamMetaTemplate);
    StreamMeta streamMeta;
    streamMeta.setSourceType(_streamMetaTemplate->getSourceType());
    streamMeta.setWindowStartTimestamp(toDate(_options.startMs));
    streamMeta.setWindowEndTimestamp(toDate(_options.endMs));
    return streams::toDeadLetterQueueMsg(std::move(streamMeta), _error);
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

}  // namespace streams
