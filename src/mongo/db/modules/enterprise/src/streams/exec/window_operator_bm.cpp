/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <benchmark/benchmark.h>
#include <string>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "streams/exec/context.h"
#include "streams/exec/message.h"
#include "streams/exec/noop_sink_operator.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/window_aware_operator.h"
#include "streams/util/metric_manager.h"

namespace streams {

using namespace mongo;

class WindowOperatorBMFixture : public benchmark::Fixture {
public:
    WindowOperatorBMFixture() {}

    void SetUp(benchmark::State& state) override {
        if (state.thread_index == 0) {
            auto service = ServiceContext::make();
            setGlobalServiceContext(std::move(service));
            _metricManager = std::make_unique<MetricManager>();
            _context = std::get<0>(getTestContext(/*svcCtx*/ nullptr));
            _context->connections = testInMemoryConnectionRegistry();
        }

        _noopSink = std::make_unique<NoOpSinkOperator>(_context.get());
        _noopSink->start();

        auto p = fromjson("{pipeline: " + pipeline + "}");
        ASSERT_EQUALS(BSONType::Array, p["pipeline"].type());
        _pipeline = parsePipelineFromBSON(p["pipeline"]);

        _dataMsg.docs.reserve(kNumDocsPerDataMsg);
        for (int j = 0; j < kNumDocsPerDataMsg; ++j) {
            StreamDocument doc(makeDocument(j, j));
            _dataMsg.docs.push_back(std::move(doc));
        }
    }

    void TearDown(benchmark::State& state) override {
        _dataMsg.docs.clear();
        _noopSink->stop();
        if (state.thread_index == 0) {
            _metricManager.reset();
            _noopSink.reset();
            _dag.reset();
            _planner.reset();
            _context.reset();
            setGlobalServiceContext({});
        }
    }

protected:
    static constexpr int64_t kNumDocsPerDataMsg = 1000;
    static constexpr int64_t kNumDataMsgs = 100;
    const std::string pipeline = R"([
        { $group: {
            _id: "$id",
            sum: { $sum: "$value" },
            avg: { $avg: "$value" },
            min: { $min: "$value" },
            max: { $max: "$value" }
        }}
    ])";

    void checkOpenWindowCount(WindowAwareOperator* window, int64_t expectedOpenWindowCount) {
        ASSERT_EQ(window->_windows.size(), expectedOpenWindowCount);
    }

    Document makeDocument(int64_t id, int64_t value) const {
        BSONObjBuilder builder;
        builder.append("id", id);
        builder.append("value", value);
        return Document(builder.obj());
    }

    WindowAwareOperator* makeWindowOperator(int size, int slide) {
        // Setting this so a sink stage is not enforced.
        _context->isEphemeral = true;
        _planner = std::make_unique<Planner>(_context.get(), Planner::Options{});
        _dag = _planner->plan(std::vector<BSONObj>{
            BSON("$source" << BSON("connectionName" << kTestMemoryConnectionName)),
            BSON("$hoppingWindow" << BSON("interval" << BSON("unit"
                                                             << "ms"
                                                             << "size" << size)
                                                     << "hopSize"
                                                     << BSON("unit"
                                                             << "ms"
                                                             << "size" << slide)
                                                     << "pipeline" << _pipeline))});
        return dynamic_cast<WindowAwareOperator*>(_dag->operators()[1].get());
    }

    // Generates `kNumDataMsgs` batches of documents, each batch being of size
    // `kNumDocsPerDataMsg`, guaranteeing that there are only `numWindows` and
    // that each window get `(kNumDataMsgs * kNumDocsPerDataMsg) / numWindows`
    // number of documents. `windowTimestampSparsityMs` controls the gap (in
    // milliseconds) between each open window, which is primarily to ensure that
    // there is no regression when there are sparse open windows.
    std::vector<StreamDataMsg> makeDataMsgs(int64_t windowTimestampSparsityMs,
                                            int64_t numWindows,
                                            int64_t windowSizeMs,
                                            int64_t windowSlideMs) const {
        std::vector<StreamDataMsg> dataMsgs(kNumDataMsgs, _dataMsg);
        int64_t cur{windowSizeMs};
        int64_t maxTimestampMs = numWindows;
        for (auto& dataMsg : dataMsgs) {
            for (auto& doc : dataMsg.docs) {
                doc.minDocTimestampMs = (cur % maxTimestampMs) * windowTimestampSparsityMs;
                doc.maxDocTimestampMs = doc.minDocTimestampMs;
                cur += windowSlideMs;
            }
        }

        return dataMsgs;
    }

    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
    std::unique_ptr<Planner> _planner;
    std::unique_ptr<OperatorDag> _dag;
    std::vector<BSONObj> _pipeline;
    std::unique_ptr<NoOpSinkOperator> _noopSink;
    StreamDataMsg _dataMsg;
};

// Benchmarks writing data messages to a window operator. This is only benchmarking the insert
// performance, which is `onDataMsg` and not the flushing/closing of windows.
BENCHMARK_DEFINE_F(WindowOperatorBMFixture, BM_WindowOperator_Insert)(benchmark::State& state) {
    int64_t windowSizeMs = state.range(0);
    int64_t windowSlideMs = state.range(1);
    int64_t windowTimestampSparsityMs = state.range(2);
    int64_t numWindows = state.range(3);
    for (auto _ : state) {
        state.PauseTiming();
        auto window = makeWindowOperator(windowSizeMs, windowSlideMs);
        auto dataMsgs =
            makeDataMsgs(windowTimestampSparsityMs, numWindows, windowSizeMs, windowSlideMs);
        state.ResumeTiming();

        for (auto& dataMsg : dataMsgs) {
            window->onDataMsg(0, std::move(dataMsg));
        }
        checkOpenWindowCount(window, numWindows);
    }

    state.SetItemsProcessed(kNumDocsPerDataMsg * kNumDataMsgs * state.iterations());
}

// 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
BENCHMARK_REGISTER_F(WindowOperatorBMFixture, BM_WindowOperator_Insert)
    ->ArgNames({"window_size_ms", "window_slide_ms", "window_timestamp_sparsity_ms", "num_windows"})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 100k (100%), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 100'000})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 10k (10%), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 10'000})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 100 (every one of the 100 window timestamps will have 10 documents within
    // a single batch of 1k docs), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 100})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 100k (100%), Gap between each open window: 1000ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1000,
            /* num_windows */ 100'000})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 10k (10%), Gap between each open window: 1000ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1000,
            /* num_windows */ 10'000})
    // Hopping window with interval size of 5ms and slide size of 1ms, input documents: 100k (in
    // batches of 1k docs), number of windows: 100k (100%), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 4,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 100'000})
    // Hopping window with interval size of 4ms and slide size of 1ms, input documents: 100k (in
    // batches of 1k docs), number of windows: 10k (10%), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 4,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 10'000})
    // Hopping window with interval size of 4ms and slide size of 1ms, input documents: 100k (in
    // batches of 1k docs), number of windows: 100k (10%), Gap between each hopping window batch:
    // 1000ms
    ->Args({/* window_size_ms */ 4,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1000,
            /* num_windows */ 400'000});

// Benchmarks flushing open windows in a window operator. This is only benchmarking the flush
// performance, which is `onControlMsg` and not the insertion of data messages.
BENCHMARK_DEFINE_F(WindowOperatorBMFixture, BM_WindowOperator_Flush)(benchmark::State& state) {
    int64_t windowSizeMs = state.range(0);
    int64_t windowSlideMs = state.range(1);
    int64_t windowTimestampSparsityMs = state.range(2);
    int64_t numWindows = state.range(3);
    for (auto _ : state) {
        state.PauseTiming();
        auto window = makeWindowOperator(windowSizeMs, windowSlideMs);
        auto dataMsgs =
            makeDataMsgs(windowTimestampSparsityMs, numWindows, windowSizeMs, windowSlideMs);
        for (auto& dataMsg : dataMsgs) {
            window->onDataMsg(0, std::move(dataMsg));
        }
        checkOpenWindowCount(window, numWindows);
        state.ResumeTiming();

        window->onControlMsg(
            0,
            StreamControlMsg{.watermarkMsg = WatermarkControlMsg{
                                 .watermarkTimestampMs = std::numeric_limits<int64_t>::max()}});
    }

    state.SetItemsProcessed(kNumDocsPerDataMsg * kNumDataMsgs * state.iterations());
}

BENCHMARK_REGISTER_F(WindowOperatorBMFixture, BM_WindowOperator_Flush)
    ->ArgNames({"window_size_ms", "window_slide_ms", "window_timestamp_sparsity_ms", "num_windows"})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 100k (100%), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 100'000})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 10k (10%), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 10'000})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 100k (100%), Gap between each open window: 1000ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1000,
            /* num_windows */ 100'000})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 10k (10%), Gap between each open window: 1000ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1000,
            /* num_windows */ 10'000})
    // Tumbling window with interval size of 1ms, input documents: 100k (in batches of 1k docs),
    // number of windows: 100 (every one of the 100 window timestamps will have 10 documents within
    // a single batch of 1k docs), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 1,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 100})
    // Hopping window with interval size of 4ms and slide size of 1ms, input documents: 100k (in
    // batches of 1k docs), number of windows: 100k (100%), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 4,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 100'000})
    // Hopping window with interval size of 4ms and slide size of 1ms, input documents: 100k (in
    // batches of 1k docs), number of windows: 10k (10%), Gap between each open window: 1ms
    ->Args({/* window_size_ms */ 4,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1,
            /* num_windows */ 10'000})
    // Hopping window with interval size of 4ms and slide size of 1ms, input documents: 100k (in
    // batches of 1k docs), number of windows: 100k (100%), Gap between each hopping window batch:
    // 1000ms
    ->Args({/* window_size_ms */ 4,
            /* window_slide_ms */ 1,
            /* window_timestamp_sparsity_ms */ 1000,
            /* num_windows */ 400'000});

};  // namespace streams
