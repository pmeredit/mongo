/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <boost/intrusive_ptr.hpp>
#include <boost/optional.hpp>
#include <boost/proto/matches.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "mongo/util/assert_util.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/message.h"
#include "streams/exec/window_pipeline.h"

namespace streams {

using namespace mongo;

namespace {

Date_t toDate(int64_t ms) {
    return Date_t::fromMillisSinceEpoch(ms);
}

const int kResultsBufferSize = 1024;

void addToResults(StreamDocument doc, std::queue<StreamDataMsg>* results) {
    if (results->empty() || results->back().docs.size() == kResultsBufferSize) {
        results->emplace(StreamDataMsg{});
        results->back().docs.reserve(kResultsBufferSize);
    }
    results->back().docs.emplace_back(std::move(doc));
}

}  // namespace

WindowPipeline::WindowPipeline(int64_t start,
                               int64_t end,
                               std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
                               boost::intrusive_ptr<mongo::ExpressionContext> expCtx)
    : _startMs(start),
      _endMs(end),
      _pipeline(std::move(pipeline)),
      _feeder(new DocumentSourceFeeder(expCtx)) {
    dassert(_endMs > _startMs);
    _pipeline->getSources().front()->setSource(_feeder.get());
}

StreamDocument WindowPipeline::toOutputDocument(Document doc) {
    dassert(_streamMetaTemplate);
    StreamDocument streamDoc(std::move(doc));
    streamDoc.streamMeta.setSourceType(_streamMetaTemplate->getSourceType());
    streamDoc.streamMeta.setWindowStartTimestamp(toDate(_startMs));
    streamDoc.streamMeta.setWindowEndTimestamp(toDate(_endMs));
    streamDoc.minProcessingTimeMs = _minObservedProcessingTime;
    streamDoc.minEventTimestampMs = _minObservedEventTimeMs;
    streamDoc.maxEventTimestampMs = _maxObservedEventTimeMs;
    return streamDoc;
}

void WindowPipeline::process(StreamDocument doc) {
    if (!_streamMetaTemplate) {
        _streamMetaTemplate = doc.streamMeta;
    }
    _minObservedEventTimeMs = std::min(_minObservedEventTimeMs, doc.minEventTimestampMs);
    _maxObservedEventTimeMs = std::max(_maxObservedEventTimeMs, doc.minEventTimestampMs);
    _minObservedProcessingTime = std::min(_minObservedProcessingTime, doc.minProcessingTimeMs);

    _feeder->addDocument(std::move(doc.doc));

    auto result = _pipeline->getSources().back()->getNext();
    // For a window with an inner pipeline like [$group], we won't get any
    // results back here. This is to handle window's with inner pipelines like
    // [$match].
    while (result.isAdvanced()) {
        _earlyResults.emplace_back(result.releaseDocument());
        result = _pipeline->getSources().back()->getNext();
    }
    dassert(result.isPaused(), str::stream() << "Expected pause, got: " << int(result.getStatus()));
}

std::queue<StreamDataMsg> WindowPipeline::close() {
    std::queue<StreamDataMsg> results;

    // If there's anything in the "_earlyResults", add that to the output.
    // This only happens for inner pipelines that don't have a blocking stage.
    for (auto& result : _earlyResults) {
        addToResults(toOutputDocument(std::move(result)), &results);
    }

    _feeder->setEndOfBufferSignal(DocumentSource::GetNextResult::makeEOF());
    auto result = _pipeline->getSources().back()->getNext();
    while (result.isAdvanced()) {
        addToResults(toOutputDocument(result.releaseDocument()), &results);
        result = _pipeline->getSources().back()->getNext();
    }
    dassert(result.isEOF(), str::stream() << "Expected EOF, got: " << int(result.getStatus()));

    return results;
}

}  // namespace streams
