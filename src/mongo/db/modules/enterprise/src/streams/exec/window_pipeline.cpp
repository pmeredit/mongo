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
    MutableDocument mutableDoc{std::move(doc)};
    mutableDoc.addField(
        "windowMeta",
        Value(BSON("windowOpen" << toDate(_startMs) << "windowClose" << toDate(_endMs))));
    StreamDocument streamDoc{mutableDoc.freeze()};
    // TODO(SERVER-75593): Use the actual min and max event timestamps.
    streamDoc.minEventTimestampMs = _startMs;
    streamDoc.maxEventTimestampMs = _endMs;
    return streamDoc;
}

void WindowPipeline::process(Document doc, int64_t time) {
    _feeder->addDocument(std::move(doc));

    auto result = _pipeline->getSources().back()->getNext();
    // For a window with an inner pipeline like [$group], we won't get any
    // results back here. This is to handle window's with inner pipelines like
    // [$match].
    while (result.isAdvanced()) {
        _earlyResults.emplace_back(toOutputDocument(result.releaseDocument()));
        result = _pipeline->getSources().back()->getNext();
    }
    dassert(result.isPaused(), str::stream() << "Expected pause, got: " << int(result.getStatus()));
}

StreamDataMsg WindowPipeline::close() {
    StreamDataMsg msg;

    // If there's anything in the "_earlyResults", add that to the output.
    // This only happens for inner pipelines that don't have a blocking stage.
    for (auto& result : _earlyResults) {
        msg.docs.emplace_back(toOutputDocument(std::move(result.doc)));
    }

    // TODO(SERVER-75593): Create a queue of vectors with fixed capacity to avoid
    // giant DataMsg and vector resizing
    _feeder->setEndOfBufferSignal(DocumentSource::GetNextResult::makeEOF());
    auto result = _pipeline->getSources().back()->getNext();
    while (result.isAdvanced()) {
        msg.docs.emplace_back(toOutputDocument(result.releaseDocument()));
        result = _pipeline->getSources().back()->getNext();
    }
    dassert(result.isEOF(), str::stream() << "Expected EOF, got: " << int(result.getStatus()));
    return msg;
}

}  // namespace streams
