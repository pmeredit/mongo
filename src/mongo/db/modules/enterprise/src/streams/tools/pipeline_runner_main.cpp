/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/filesystem/operations.hpp>
#include <boost/program_options.hpp>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/debug_util.h"
#include "streams/exec/context.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/executor.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

using namespace mongo;
using namespace streams;

BSONObj readBsonFromJsonFile(std::string fileName) {
    std::ifstream infile(fileName.c_str());
    std::string data((std::istreambuf_iterator<char>(infile)), std::istreambuf_iterator<char>());
    return fromjson(data);
}

std::unique_ptr<KafkaConsumerOperator> createKafkaConsumerOperator(
    const BSONObj& sourceObj, Context* context, EventDeserializer* deserializer) {
    KafkaConsumerOperator::Options options;
    options.bootstrapServers = sourceObj["bootstrapServers"].String();
    options.topicNames.push_back(sourceObj["topic"].String());
    options.deserializer = deserializer;
    return std::make_unique<KafkaConsumerOperator>(context, std::move(options));
}

class PipelineRunner {
public:
    void runPipelineUsingKafkaConsumerOperator(BSONObj pipelineObj);
};

void PipelineRunner::runPipelineUsingKafkaConsumerOperator(BSONObj pipelineObj) {
    BSONObj sourceObj = pipelineObj[0].Obj()["$source"].Obj();
    BSONObj restObj = pipelineObj.removeField(std::to_string(0));
    std::vector<BSONObj> pipeline;
    pipeline.push_back(getTestSourceSpec());
    for (auto elem : restObj) {
        dassert(elem.type() == BSONType::Object);
        pipeline.push_back(elem.Obj());
    }
    pipeline.push_back(getTestMemorySinkSpec());

    QueryTestServiceContext qtServiceContext;
    auto svcCtx = qtServiceContext.getServiceContext();
    auto metricManager = std::make_unique<MetricManager>();
    auto [context, _] = getTestContext(svcCtx);

    auto deserializer = std::make_unique<JsonEventDeserializer>();
    auto source = createKafkaConsumerOperator(sourceObj, context.get(), deserializer.get());

    Planner planner(context.get(), {});
    std::unique_ptr<OperatorDag> dag(planner.plan(pipeline));

    auto& operators = const_cast<OperatorDag::OperatorContainer&>(dag->operators());

    // Replace InMemorySourceOperator in the operator dag with KafkaConsumerOperator.
    operators[0] = std::move(source);
    operators[0]->addOutput(operators[1].get(), 0);
    auto sink = dynamic_cast<InMemorySinkOperator*>(operators.back().get());
    invariant(sink);

    // Create an Executor and start it.
    Executor::Options executorOptions;
    executorOptions.operatorDag = dag.get();
    executorOptions.metricManager = std::make_unique<MetricManager>();
    auto executor = std::make_unique<Executor>(context.get(), std::move(executorOptions));
    std::ignore = executor->start();

    // Pull the docs from the dag until it becomes idle.
    int32_t numDocs{0};
    int32_t numConsecutiveEmptyRuns{0};
    while (numConsecutiveEmptyRuns < 100) {  // total 5s of extra runtime
        int32_t numDocsInCurRun{0};
        std::deque<StreamMsgUnion> msgs = sink->getMessages();
        while (!msgs.empty()) {
            auto msg = std::move(msgs.front());
            msgs.pop_front();

            if (!msg.dataMsg) {
                continue;
            }
            for (const auto& doc : msg.dataMsg->docs) {
                std::cout << doc.doc << std::endl;
            }
            numDocsInCurRun += msg.dataMsg->docs.size();
        }

        if (numDocsInCurRun > 0) {
            numDocs += numDocsInCurRun;
            numConsecutiveEmptyRuns = 0;
        } else {
            ++numConsecutiveEmptyRuns;
        }
        stdx::this_thread::sleep_for(stdx::chrono::milliseconds(50));
    }
    std::cout << "\n\n\n(pipeline_runner_main) Received " << numDocs
              << " documents from the pipeline" << std::endl;
    executor->stop(StopReason::ExternalStopRequest);

    auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(context->dlq.get());
    auto dlqMsgs = dlq->getMessages();
    std::cout << "(pipeline_runner_main) Found " << dlqMsgs.size() << " documents in the DLQ"
              << std::endl;
    while (!dlqMsgs.empty()) {
        auto msg = std::move(dlqMsgs.front());
        dlqMsgs.pop();
        std::cout << "DLQ: " << msg << std::endl;
    }
}

/**
 * Runs given stream pipeline locally.
 *
 * Sample command:
 * build/install/bin/streams_pipeline_runner_main --run_pipeline=true
 * --pipeline_file="pipeline.json"
 */
int main(int argc, char** argv) {
    using namespace boost::program_options;

    options_description desc{"Options"};
    desc.add_options()(
        "run_pipeline", value<bool>()->default_value(false), "Whether to run a pipeline.")(
        "pipeline_filename", value<std::string>(), "File that contains the stream pipeline.");
    variables_map flags;
    store(parse_command_line(argc, argv, desc), flags);
    notify(flags);

    runGlobalInitializersOrDie(std::vector<std::string>(argv, argv + argc));

    if (flags["run_pipeline"].as<bool>()) {
        dassert(flags.count("pipeline_filename"));
        std::string fileName = flags["pipeline_filename"].as<std::string>();
        boost::filesystem::path directoryPath = boost::filesystem::current_path();
        boost::filesystem::path filePath(
            directoryPath / "src/mongo/db/modules/enterprise/src/streams/tools" / fileName);
        dassert(boost::filesystem::exists(filePath));

        PipelineRunner pipelineRunner;
        auto pipelineObj = readBsonFromJsonFile(filePath.string());
        pipelineRunner.runPipelineUsingKafkaConsumerOperator(std::move(pipelineObj));
    }
    return 0;
}
