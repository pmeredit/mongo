/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/window_operator.h"

#include <chrono>

#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/collect_operator.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_source_window_stub.h"
#include "streams/exec/message.h"
#include "streams/exec/util.h"
#include "streams/exec/window_operator.h"
#include "streams/util/metric_manager.h"

using namespace mongo;

namespace streams {

WindowOperator::WindowOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _windowSizeMs(toMillis(options.sizeUnit, options.size)),
      _windowSlideMs(toMillis(options.slideUnit, options.slide)) {
    dassert(_options.size > 0);
    dassert(_options.slide > 0);
    dassert(_windowSizeMs > 0);
    dassert(_windowSlideMs > 0);
    _innerPipelineTemplate = Pipeline::parse(_options.pipeline, _context->expCtx);
    // TODO(SERVER-78478): Remove this once we're passing an optimized representation of the
    // pipeline.
    _innerPipelineTemplate->optimizePipeline();

    Parser::Options parserOptions;
    parserOptions.planMainPipeline = false;
    _parser = std::make_unique<Parser>(_context, std::move(parserOptions));

    MetricManager::LabelsVec labels;
    labels.push_back(std::make_pair(kTenantIdLabelKey, _context->tenantId));
    labels.push_back(std::make_pair(kProcessorIdLabelKey, _context->streamProcessorId));
    _numOpenWindowsGauge =
        _context->metricManager->registerGauge("num_open_windows", std::move(labels));
}

bool WindowOperator::windowContains(int64_t start, int64_t end, int64_t timestamp) {
    return timestamp >= start && timestamp < end;
}

bool WindowOperator::shouldCloseWindow(int64_t windowEnd, int64_t watermarkTime) {
    return watermarkTime >= windowEnd;
}

std::map<int64_t, WindowPipeline>::iterator WindowOperator::addWindow(int64_t start, int64_t end) {
    auto pipeline = _innerPipelineTemplate->clone();
    auto operators = _parser->fromPipeline(*pipeline, /* minOperatorId */ _operatorId + 1);

    // Add a CollectOperator at the end of the operator chain to collect the documents
    // emitted at the end of the pipeline.
    auto collectOperator = std::make_unique<CollectOperator>(_context, /*numInputs*/ 1);
    auto& lastOp = operators.back();
    // TODO(SERVER-78481): We may need to increment by getNumInnerOperators here when $facet is
    // supported.
    invariant(lastOp->getNumInnerOperators() == 0);
    OperatorId collectOperatorId = lastOp->getOperatorId() + 1;
    collectOperator->setOperatorId(collectOperatorId);
    invariant(collectOperator->getNumInnerOperators() == 0);
    operators.back()->addOutput(collectOperator.get(), 0);
    operators.push_back(std::move(collectOperator));

    WindowPipeline::Options options;
    options.startMs = start;
    options.endMs = end;
    options.pipeline = std::move(pipeline->getSources());
    options.operators = std::move(operators);
    WindowPipeline windowPipeline(_context, std::move(options));
    auto result = _openWindows.emplace(std::make_pair(start, std::move(windowPipeline)));
    dassert(result.second);
    return std::move(result.first);
}

void WindowOperator::doOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) {
    for (auto& doc : dataMsg.docs) {
        auto docTime = doc.minEventTimestampMs;
        // Create and/or look up windows from the oldest window until we exceed 'docTime'.
        for (auto start = toOldestWindowStartTime(docTime); start <= docTime;
             start += _windowSlideMs) {
            auto end = start + _windowSizeMs;
            dassert(windowContains(start, end, docTime));
            auto window = _openWindows.find(start);
            if (window == _openWindows.end()) {
                window = addWindow(start, end);
            }
            auto& windowPipeline = window->second;
            try {
                // TODO: Avoid copying the doc.
                StreamDataMsg dataMsg{{doc}};
                windowPipeline.process(std::move(dataMsg));
            } catch (const DBException& e) {
                windowPipeline.setError(str::stream() << "Failed to process input document in "
                                                      << getName() << " with error: " << e.what());
            }
        }
    }
    if (controlMsg) {
        doOnControlMsg(inputIdx, *controlMsg);
    }
    _numOpenWindowsGauge->set(_openWindows.size());
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
    return std::max(docTime - _windowSizeMs + _windowSlideMs - (docTime % _windowSlideMs),
                    int64_t{0});
}

void WindowOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    if (controlMsg.watermarkMsg) {
        // TODO(SERVER-76722): If we want to use an unordered_map for the container, we need
        // to add some extra logic here to close windows in order. We can choose a starting
        // point in time and iterate using options.slide, like in doOnDataMessage. The starting
        // point in time here could be min(EarliestOpenWindowStart, watermarkTime aligned to its
        // closest End boundary).
        auto watermarkTime = controlMsg.watermarkMsg->eventTimeWatermarkMs;
        for (auto it = _openWindows.begin(); it != _openWindows.end();) {
            auto& windowPipeline = it->second;

            if (shouldCloseWindow(windowPipeline.getEnd(), watermarkTime)) {
                std::queue<StreamDataMsg> results;
                try {
                    results = windowPipeline.close();
                } catch (const DBException& e) {
                    windowPipeline.setError(
                        str::stream() << "Failed to process an input document for this window in "
                                      << getName() << " with error: " << e.what());
                }

                if (windowPipeline.getError()) {
                    _context->dlq->addMessage(windowPipeline.getDeadLetterQueueMsg());
                } else {
                    while (!results.empty()) {
                        sendDataMsg(0, std::move(results.front()));
                        results.pop();
                    }
                }
                _openWindows.erase(it++);
            } else {
                break;
            }
        }
    }

    sendControlMsg(0, std::move(controlMsg));
}

int32_t WindowOperator::getNumInnerOperators() const {
    // The size of the inner pipeline, plus 1 for the CollectOperator.
    return _innerPipelineTemplate->getSources().size() + 1;
}

}  // namespace streams
