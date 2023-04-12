/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/window_operator.h"
#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/document_source_window_stub.h"
#include "streams/exec/message.h"
#include "streams/exec/parser.h"
#include "streams/exec/window_stage_gen.h"
#include <chrono>

using namespace mongo;

namespace streams {

namespace {

WindowOperator::Options makeOptions(BSONObj bsonOptions,
                                    const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    auto options = TumblingWindow::parse(IDLParserContext("tumblingWindow"), bsonOptions);
    auto interval = options.getInterval();
    const auto& pipeline = options.getPipeline();
    auto size = interval.getSize();
    return {pipeline, expCtx, size, interval.getUnit(), size, interval.getUnit()};
}

}  // namespace

WindowOperator::WindowOperator(Options options)
    : Operator(1, 1),
      _options(options),
      _windowSizeMs(toMillis(options.sizeUnit, options.size)),
      _windowSlideMs(toMillis(options.slideUnit, options.slide)) {
    // Only tumbling windows currently supported.
    dassert(isTumblingWindow());
    _innerPipelineTemplate = Pipeline::parse(_options.pipeline, _options.expCtx);
    _innerPipelineTemplate->optimizePipeline();
}

WindowOperator::WindowOperator(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                               mongo::BSONObj bsonOptions)
    : WindowOperator(makeOptions(std::move(bsonOptions), expCtx)) {}

bool WindowOperator::windowContains(int64_t start, int64_t end, int64_t timestamp) {
    return timestamp >= start && timestamp < end;
}

bool WindowOperator::shouldCloseWindow(int64_t windowEnd, int64_t watermarkTime) {
    return watermarkTime >= windowEnd;
}

auto WindowOperator::addWindow(int64_t start, int64_t end) {
    auto pipeline = _innerPipelineTemplate->clone();
    WindowPipeline windowPipeline(start, end, std::move(pipeline), _options.expCtx);
    return _openWindows.emplace(std::make_pair(start, std::move(windowPipeline))).first;
}

void WindowOperator::doOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) {
    // TODO(STREAMS-220)-PrivatePreview: Modify this implementation for $hoppingWindow.
    for (auto& doc : dataMsg.docs) {
        auto docTime = doc.minEventTimestampMs;
        auto start = toOldestWindowStartTime(docTime);
        auto end = start + _windowSizeMs;
        dassert(windowContains(start, end, docTime));
        auto window = _openWindows.find(start);
        if (window == _openWindows.end()) {
            window = addWindow(start, end);
        }
        window->second.process(std::move(doc.doc), docTime);
    }

    if (controlMsg) {
        doOnControlMsg(inputIdx, *controlMsg);
    }
}

/**
 * Like flink, we align our tumbling window boundaries to the epoch.
 * Suppose the pipeline has a 1 hour window and we see our first event at
 * 11:05:32.000 on the first day.
 * For this event we will open a window for: [11:00:00.000, 12:00:00.000).
 * Here are a few examples for tumbling windows.
 *
 * Support docTime is 555 and the window size is 100 (slide is also 100).
 * 555 - 100 + 100 - (555 % 100) = 555 - (555 % 100) = 500
 *
 * Support docTime is 999 and the window size is 25 (slide is also 25).
 * 999 - 25 + 25 - (999 % 25) = 999 - (999 % 25) = 999 - 24 = 975
 *
 * For tumblingWindows, _windowSize == _windowSlide.
 * Thus, docTime - _windowSize + _windowSlide - (docTime % _windowSlide)
 * reduces to:
 * docTime - _windowSize + _windowSize - (docTime % _windowSize)
 * docTime - (docTime % _windowSize)
 */
int64_t WindowOperator::toOldestWindowStartTime(int64_t docTime) {
    return docTime - _windowSizeMs + _windowSlideMs - (docTime % _windowSlideMs);
}

// TODO(STREAMS-220)-PrivatePreview: Especially with units of day and year,
// this logic probably leads to incorrect for daylights savings time and leap years.
// In future work we can rely more on std::chrono for the time math here, for now
// we're just converting the size and slide to milliseconds for simplicity.
int64_t WindowOperator::toMillis(mongo::TimeWindowUnitEnum unit, int count) {
    switch (unit) {
        case TimeWindowUnitEnum::Millisecond:
            return stdx::chrono::milliseconds(count).count();
        case TimeWindowUnitEnum::Second:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::seconds(count))
                .count();
        case TimeWindowUnitEnum::Minute:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::minutes(count))
                .count();
        case TimeWindowUnitEnum::Hour:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::hours(count))
                .count();
        // For Day, using the C++20 ratios from
        // https://en.cppreference.com/w/cpp/header/chrono
        case TimeWindowUnitEnum::Day:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::seconds(86400 * count))
                .count();
        default:
            MONGO_UNREACHABLE;
    }
}

void WindowOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    if (controlMsg.watermarkMsg) {
        // TODO(SERVER-75593): If we want to use an unordered_map for the container, we need
        // to add some extra logic here to close windows in order. We can choose a starting point in
        // time and iterate using options.slide, like in doOnDataMessage.
        // The starting point in time here could be
        // min(EarliestOpenWindowStart, watermarkTime aligned to its closest End boundary).
        auto watermarkTime = controlMsg.watermarkMsg->eventTimeWatermarkMs;
        for (auto it = _openWindows.begin(); it != _openWindows.end();) {
            if (shouldCloseWindow(it->second.getEnd(), watermarkTime)) {
                // TODO(STREAMS-220)-PrivatePreview: chunk up the output,
                // right now we're just build one (potentially giant) StreamDataMsg.
                auto dataMsg = it->second.close();
                sendDataMsg(0, std::move(dataMsg), boost::none);
                _openWindows.erase(it++);
            } else {
                break;
            }
        }
    }

    sendControlMsg(0, std::move(controlMsg));
}

}  // namespace streams
