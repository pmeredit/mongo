/**
 *    Copyright (C) 2023-present MongoDB, Inc.
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
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/parser.h"
#include "streams/exec/tests/test_utils.h"

using namespace mongo;
using namespace streams;

BSONObj readBsonFromJsonFile(std::string fileName) {
    std::ifstream infile(fileName.c_str());
    std::string data((std::istreambuf_iterator<char>(infile)), std::istreambuf_iterator<char>());
    return fromjson(data);
}

std::unique_ptr<KafkaConsumerOperator> createKafkaConsumerOperator(
    const BSONObj& sourceObj, EventDeserializer* deserializer) {
    KafkaConsumerOperator::Options options;
    options.bootstrapServers = sourceObj["bootstrapServers"].String();
    options.topicName = sourceObj["topicName"].String();
    int32_t numPartitions = sourceObj["numPartitions"].Int();
    for (int32_t partition = 0; partition < numPartitions; ++partition) {
        KafkaConsumerOperator::PartitionOptions partitionOptions;
        partitionOptions.partition = partition;
        options.partitionOptions.push_back(std::move(partitionOptions));
    }
    options.deserializer = deserializer;
    return std::make_unique<KafkaConsumerOperator>(std::move(options));
}

class PipelineRunner {
public:
    PipelineRunner() : _context(getTestContext()) {}

    void runPipelineUsingKafkaConsumerOperator(BSONObj pipelineObj);

private:
    std::unique_ptr<Context> _context;
};

void PipelineRunner::runPipelineUsingKafkaConsumerOperator(BSONObj pipelineObj) {
    BSONObj sourceObj = pipelineObj[0].Obj()["$source"].Obj();
    BSONObj restObj = pipelineObj.removeField(std::to_string(0));
    std::vector<BSONObj> pipeline;
    for (auto elem : restObj) {
        dassert(elem.type() == BSONType::Object);
        pipeline.push_back(elem.Obj());
    }

    const NamespaceString kNss{"pipeline_runner"};
    QueryTestServiceContext serviceContext;
    auto opCtx = serviceContext.makeOperationContext();
    auto expCtx = make_intrusive<ExpressionContextForTest>(opCtx.get(), kNss);

    auto deserializer = std::make_unique<JsonEventDeserializer>();
    auto source = createKafkaConsumerOperator(sourceObj, deserializer.get());
    auto sink = std::make_unique<InMemorySinkOperator>(/*numInputs*/ 1);
    auto sinkPtr = sink.get();

    Parser parser(_context.get(), {});
    std::unique_ptr<OperatorDag> dag(parser.fromBson(pipeline));

    auto& operators = const_cast<OperatorDag::OperatorContainer&>(dag->operators());

    // Add the source to the operator dag.
    source->addOutput(operators.front().get(), 0);
    operators.insert(operators.begin(), std::move(source));

    // Add an in-memory sink to the operator dag.
    operators.back()->addOutput(sink.get(), 0);
    operators.push_back(std::move(sink));

    // Create an Executor and start it.
    Executor::Options executorOptions;
    executorOptions.streamProcessorName = "_pipeline_runner_main";
    executorOptions.operatorDag = dag.get();
    auto executor = std::make_unique<Executor>(std::move(executorOptions));
    executor->start();

    // Pull the docs from the dag until it becomes idle.
    int32_t numDocs{0};
    int32_t numConsecutiveEmptyRuns{0};
    while (numConsecutiveEmptyRuns < 100) {  // total 5s of extra runtime
        int32_t numDocsInCurRun{0};
        std::queue<StreamMsgUnion> msgs = sinkPtr->getMessages();
        while (!msgs.empty()) {
            auto msg = std::move(msgs.front());
            msgs.pop();

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
    executor->stop();
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
