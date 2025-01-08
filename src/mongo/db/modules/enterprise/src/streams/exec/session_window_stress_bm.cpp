/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <algorithm>
#include <benchmark/benchmark.h>
#include <random>
#include <string>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"
#include "streams/exec/context.h"
#include "streams/exec/group_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/noop_sink_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/window_assigner.h"
#include "streams/exec/window_aware_operator.h"

namespace streams {

using namespace mongo;

class SessionWindowStressBMFixture : public benchmark::Fixture {
public:
    SessionWindowStressBMFixture() {}

    void SetUp(benchmark::State& state) override {
        if (state.thread_index == 0) {
            auto service = ServiceContext::make();
            setGlobalServiceContext(std::move(service));
            _context = std::get<0>(getTestContext(/*svcCtx*/ nullptr));
        }
    }

    void TearDown(benchmark::State& state) override {
        if (state.thread_index == 0) {
            _noopSink.reset();
            _dag->stop();
            _dag.reset();
            _context.reset();
            setGlobalServiceContext({});
        }
    }

protected:
    constexpr static auto kPartitionFieldName = "partition"_sd;

    Document makeDocument(int64_t id, int64_t value, int64_t partition) const {
        BSONObjBuilder builder;
        builder.append("id", id);
        builder.append("value", value);
        builder.append(kPartitionFieldName, partition);
        return Document(builder.obj());
    }

    // Creates input according to the parameters.
    // In the input there {numPartitions} partitions. Each partition has 1 session
    // with {docsPerSession} in it. There are {secondsBetweenDocsInSession} seconds in between
    // each doc in the session.
    // The returned input is sorted by timestamp (not partition).
    // The batch size use is the same as real sources use.
    auto makeSortedInput(int numPartitions,
                         int docsPerSession,
                         Seconds secondsBetweenDocsInSession,
                         Date_t startTime,
                         Milliseconds perPartitionMs,
                         Minutes gap,
                         const uint32_t docLim = kDataMsgMaxDocSize) {
        Date_t sessionStartTime{startTime};
        int idx{0};
        std::vector<StreamDocument> docs;
        for (int partition = 0; partition < numPartitions; ++partition) {
            // Create a session for each partition.
            for (int docNum = 0; docNum < docsPerSession; ++docNum) {
                StreamDocument streamDoc(makeDocument(idx, idx, partition));
                streamDoc.minDocTimestampMs =
                    (sessionStartTime + secondsBetweenDocsInSession * docNum).asInt64();
                docs.push_back(std::move(streamDoc));
                ++idx;
            }
            sessionStartTime += perPartitionMs;
        }
        // Sort by timestamps.
        std::sort(docs.begin(), docs.end(), [](const auto& left, const auto& right) {
            return left.minDocTimestampMs < right.minDocTimestampMs;
        });

        // Organize into batches.
        std::vector<StreamDataMsg> msgs;
        for (size_t docIdx = 0; docIdx < docs.size();) {
            std::vector<StreamDocument> batch;
            int64_t batchSize{0};
            while (docIdx < docs.size() && batch.size() < docLim &&
                   batchSize < kDataMsgMaxByteSize) {
                batch.push_back(std::move(docs[docIdx++]));
            }
            msgs.push_back(StreamDataMsg{std::move(batch)});
        }

        // Get the max timestamp per partition and overall max timestamp.
        int64_t overallMaxTs{0};
        ValueUnorderedMap<int64_t> maxTsPerPartition =
            mongo::ValueComparator::kInstance.makeUnorderedValueMap<int64_t>();
        for (const auto& msg : msgs) {
            for (const auto& doc : msg.docs) {
                auto partition = doc.doc["partition"];
                auto ts = doc.minDocTimestampMs;
                if (ts > maxTsPerPartition[partition]) {
                    maxTsPerPartition[partition] = ts;
                }
                if (ts > overallMaxTs) {
                    overallMaxTs = ts;
                }
            }
        }

        // Based on the final watermark, check how many partitions should have closed
        // their sessions with a 1 hour gap
        int64_t expectedNumClosedWindows{0};
        for (const auto& [partition, maxTs] : maxTsPerPartition) {
            int64_t finalWatermark = overallMaxTs - 1;
            if (finalWatermark >= (Date_t::fromMillisSinceEpoch(maxTs) + gap).asInt64()) {
                ++expectedNumClosedWindows;
            }
        }

        return std::make_tuple(
            std::move(msgs), std::move(maxTsPerPartition), expectedNumClosedWindows);
    }

    // Create an OperatorDag: [$sessionWindow[$group], no-op sink];
    GroupOperator* makeSessionWindow10MinuteGap() {
        // TODO(SERVER-91881): Change this to use the planner.

        auto bson = fromjson(_innerPipeline);
        auto spec = bson.firstElement().Obj();
        auto specElem = spec.firstElement();
        boost::intrusive_ptr<DocumentSource> groupStage =
            DocumentSourceGroup::createFromBson(specElem, _context->expCtx);

        auto partitionBy = ExpressionFieldPath::parse(_context->expCtx.get(),
                                                      "$" + kPartitionFieldName,
                                                      _context->expCtx->variablesParseState);

        SessionWindowAssigner::Options windowingOptions(WindowAssigner::Options{});
        windowingOptions.gapSize = 10;
        windowingOptions.gapUnit = mongo::StreamTimeUnitEnum::Minute;
        windowingOptions.partitionBy = std::move(partitionBy);

        auto windowAssigner = std::make_unique<SessionWindowAssigner>(windowingOptions);

        GroupOperator::Options options{WindowAwareOperator::Options{
            .windowAssigner = std::move(windowAssigner), .sendWindowSignals = true}};
        options.documentSource = dynamic_cast<DocumentSourceGroup*>(groupStage.get());
        options.isSessionWindow = true;
        auto groupOperator = std::make_unique<GroupOperator>(_context.get(), std::move(options));
        OperatorDag::OperatorContainer ops;
        ops.push_back(std::move(groupOperator));
        ops.push_back(std::make_unique<NoOpSinkOperator>(_context.get()));
        ops[0]->addOutput(ops[1].get(), 0);

        _dag = std::make_unique<OperatorDag>(
            OperatorDag::Options{
                .pipeline = Pipeline::SourceContainer{std::move(groupStage)},
            },
            std::move(ops));
        _dag->start();

        return dynamic_cast<GroupOperator*>(_dag->operators()[0].get());
    }

    auto makeDocsTinyMode() {
        Date_t startTime = dateFromISOString("2024-01-01T00:00:00Z").getValue();
        int numPartitions{500000};
        int docsPerSession{20};
        Seconds secondsBetweenDocsInSession{1};
        std::vector<StreamDataMsg> input;
        Milliseconds timeSeperationBetweenPartitions{6};
        return makeSortedInput(numPartitions,
                               docsPerSession,
                               secondsBetweenDocsInSession,
                               startTime,
                               timeSeperationBetweenPartitions,
                               Minutes{10},
                               1000);
    }

    void mediumBatchBenchmark(benchmark::State& state) {
        for (auto _ : state) {
            state.PauseTiming();
            int64_t watermark{0};
            auto window = makeSessionWindow10MinuteGap();
            auto [dataMsgs, maxTsPerPartition, expectedNumClosedWindows] = makeDocsTinyMode();
            state.ResumeTiming();

            for (auto& dataMsg : dataMsgs) {
                // This assumes the order is sorted.
                const auto& lastDoc = dataMsg.docs[dataMsg.docs.size() - 1];
                auto maxTs = lastDoc.minDocTimestampMs;
                watermark = maxTs - 1;
                window->onDataMsg(0,
                                  std::move(dataMsg),
                                  StreamControlMsg{WatermarkControlMsg{
                                      .watermarkStatus = WatermarkStatus::kActive,
                                      .watermarkTimestampMs = watermark}});
            }

            state.PauseTiming();

            auto stats = window->getStats();

            // There is one output doc per closed window.
            auto actualClosedWindows = window->getStats().numOutputDocs;
            ASSERT(expectedNumClosedWindows > 0);
            ASSERT_EQ(expectedNumClosedWindows, actualClosedWindows);

            state.ResumeTiming();
        }
    }

    void bigBatchBenchmark(benchmark::State& state) {
        Date_t startTime = dateFromISOString("2024-01-01T00:00:00Z").getValue();
        int numPartitions{500000};
        int docsPerSession{20};
        Seconds secondsBetweenDocsInSession{1};
        std::vector<StreamDataMsg> input;
        Milliseconds timeSeperationBetweenPartitions{100};

        for (auto _ : state) {
            state.PauseTiming();
            int64_t watermark{0};
            auto window = makeSessionWindow10MinuteGap();
            auto [dataMsgs, maxTsPerPartition, expectedNumClosedWindows] =
                makeSortedInput(numPartitions,
                                docsPerSession,
                                secondsBetweenDocsInSession,
                                startTime,
                                timeSeperationBetweenPartitions,
                                Minutes{10});
            state.ResumeTiming();

            for (auto& dataMsg : dataMsgs) {
                // This assumes the order is sorted.
                const auto& lastDoc = dataMsg.docs[dataMsg.docs.size() - 1];
                auto maxTs = lastDoc.minDocTimestampMs;
                watermark = maxTs - 1;
                window->onDataMsg(0,
                                  std::move(dataMsg),
                                  StreamControlMsg{WatermarkControlMsg{
                                      .watermarkStatus = WatermarkStatus::kActive,
                                      .watermarkTimestampMs = watermark}});
            }

            state.PauseTiming();

            auto stats = window->getStats();

            // There is one output doc per closed window.
            auto actualClosedWindows = window->getStats().numOutputDocs;
            ASSERT(expectedNumClosedWindows > 0);
            ASSERT_EQ(expectedNumClosedWindows, actualClosedWindows);

            state.ResumeTiming();
        }
    }

    std::unique_ptr<Context> _context;
    std::unique_ptr<OperatorDag> _dag;
    std::unique_ptr<NoOpSinkOperator> _noopSink;
    std::string _innerPipeline = R"([
        { $group: {
            _id: "$partition",
            sum: { $sum: "$value" }
        }}
    ])";
};

BENCHMARK_F(SessionWindowStressBMFixture, BM_SessionWindowOperator_Insert)
(benchmark::State& state) {
    bigBatchBenchmark(state);
}

BENCHMARK_F(SessionWindowStressBMFixture, BM_SessionWindowOperator_Insert_Tiny)
(benchmark::State& state) {
    mediumBatchBenchmark(state);
}

};  // namespace streams
