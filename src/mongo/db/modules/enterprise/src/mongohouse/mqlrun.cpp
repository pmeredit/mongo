/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <fstream>
#include <iostream>

#ifndef _WIN32
#include <unistd.h>
#endif

#include "mongo/db/exec/pipeline_proxy.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/json.h"
#include "mongo/db/pipeline/aggregation_request.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/service_context.h"

#include "document_source_bson_file.h"
#include "document_source_stdin.h"

namespace mongo {

enum class OutputType {
    kJSON,
    kBSON,
};

BSONArray createPipelineArray(boost::intrusive_ptr<ExpressionContext> expCtx,
                              const char* pipelineStr) {
    if (strstr(pipelineStr, ".json")) {
        // assume it's a file
        // this is ugly, fix it

        BSONArrayBuilder arr;

        std::ifstream in(pipelineStr);
        while (in.good()) {
            std::string line;
            std::getline(in, line);
            auto tmp = fromjson(line);
            arr << tmp;
        }

        return arr.arr();
    }

    if (strstr(pipelineStr, ".bson")) {
        // assume it's a file
        // this is ugly, fix it

        BSONArrayBuilder arr;

        auto in = DocumentSourceBSONFile::create(expCtx, pipelineStr);
        ON_BLOCK_EXIT([in] { in->dispose(); });

        while (1) {
            auto next = in->getNext();
            if (next.isEOF()) {
                break;
            }

            auto tmp = next.getDocument();
            arr << tmp;
        }

        return arr.arr();
    }

    return (BSONArray)fromjson(pipelineStr);
}

int mqlrunMain(const char* pipelineStr,
               const char* fileName,
               OutputType outputType,
               const char* tempDir) {
    std::function<void(BSONObj)> bsonWriter;
    switch (outputType) {
        case OutputType::kJSON:
            bsonWriter = [](BSONObj obj) { std::cout << obj << std::endl; };
            break;
        case OutputType::kBSON:
            bsonWriter = [](BSONObj obj) {
#ifdef _WIN32
                HANDLE hFile = GetStdHandle(STD_OUTPUT_HANDLE);
                unsigned long bytesWritten;
                invariant(WriteFile(hFile, obj.objdata(), obj.objsize(), &bytesWritten, nullptr));
                invariant(bytesWritten == obj.objsize());
#else
                std::cout.write(obj.objdata(), obj.objsize());
                invariant(std::cout.good());
#endif
            };
            break;
        default:
            MONGO_UNREACHABLE;
    }

    NamespaceString nss("db.data"_sd);
    auto client = getGlobalServiceContext()->makeClient("mqlrun");
    auto opCtx = client->makeOperationContext();
    boost::intrusive_ptr<ExpressionContext> expCtx;
    expCtx.reset(new ExpressionContext(opCtx.get(), nullptr, nss));
    expCtx->allowDiskUse = false;
    if (tempDir) {
        // Spill to disk if needed for a large sort.
        expCtx->allowDiskUse = true;
        expCtx->tempDir = tempDir;
    }

    BSONObj pipelineArray;
    try {
        pipelineArray = BSON("pipeline" << createPipelineArray(expCtx, pipelineStr));
    } catch (AssertionException& e) {
        std::cerr << "Invalid JSON in pipeline: " << e.reason() << std::endl;
        return 1;
    }

    std::vector<BSONObj> pipelineVector;
    try {
        auto statusWithPipeline =
            AggregationRequest::parsePipelineFromBSON(pipelineArray["pipeline"]);
        uassertStatusOK(statusWithPipeline.getStatus());
        pipelineVector = std::move(statusWithPipeline.getValue());
    } catch (AssertionException& e) {
        std::cerr << "Failed to parse pipeline from file: " << e.reason() << std::endl;
        return 1;
    }

    std::unique_ptr<Pipeline, PipelineDeleter> pipeline;
    try {
        pipeline = Pipeline::parse(pipelineVector, expCtx);
    } catch (AssertionException& e) {
        std::cerr << "Failed to parse pipeline: " << e.reason() << std::endl;
        return 1;
    }

    try {
        if (strlen(fileName) == 1 && fileName[0] == '-') {
            auto stdinSource = DocumentSourceStdin::create(expCtx);
            pipeline->addInitialSource(stdinSource);
        } else {
            auto bsonSource = DocumentSourceBSONFile::create(expCtx, fileName);
            pipeline->addInitialSource(bsonSource);
        }
    } catch (AssertionException& e) {
        std::cerr << e.reason() << std::endl;
        return false;
    }

    auto ws = std::make_unique<WorkingSet>();
    auto proxy = std::make_unique<PipelineProxyStage>(expCtx.get(), std::move(pipeline), ws.get());

    auto planExec = PlanExecutor::make(
        expCtx, std::move(ws), std::move(proxy), nullptr, PlanExecutor::NO_YIELD, nss);

    BSONObj outObj;

    while (1) {
        try {
            auto result = planExec.getValue()->getNext(&outObj, nullptr);
            switch (result) {
                case PlanExecutor::ADVANCED:
                    bsonWriter(outObj);
                    break;
                case PlanExecutor::IS_EOF:
                    return 0;  // Success
            }
        } catch (AssertionException& e) {
            std::cerr << "Failure: " << e.reason() << std::endl;
            return 1;
        }
    }
}

}  // namespace mongo

const char* usage =
    "Usage: mqlrun [OPTIONS] -e [AGG EXPRESSION] [INPUT BSON FILE]\n"
    "Options: -b Output BSON (default)\n"
    "         -j Output JSON text\n\n"
    "         -t [DIRECTORY] Path to a temp directory (required for large sorts)\n"
    "         -h Output help text and exit\n"
    "Examples:\n"
    "Write out the contents of a BSON file as JSON.\n"
    "mqlrun -j -e '[]' input.bson\n\n"
    "A simple lookup.\n"
    "mqlrun -e '[{$match: {x: 1}}]' input.bson > output.bson\n\n"
    "A lookup with a projection.\n"
    "mqlrun -e '[{$match: {x: 1}}, {$project: {y: \"$x\"}}]' input.bson > output.bson\n\n"
    "N.B.: When using mqlrun from a shell, single quotes are recommended for the\n"
    "aggregation expression, to prevent the shell from attempting to substitute MQL\n"
    "operators that have $ in their names.\n\n"
    "Supported aggregation stages: $addFields, $bucket, $bucketAuto, $count, $facet,\n"
    "$group, $match, $project, $redact, $replaceRoot, $sample, $skip, $sort,\n"
    "$sortByCount, $unwind\n\n"
    "Unsupported aggregations stages: $collStags, $currentOp, $geoNear,\n"
    "$graphLookup, $indexStats, $listLocalSessions, $lookup, $out\n";

int main(int argc, char* argv[]) {

    mongo::Status status =
        mongo::runGlobalInitializers(std::vector<std::string>(argv, argv + argc));

    if (!status.isOK()) {
        std::cerr << "Failed global initialization: " << status << std::endl;
        return 1;
    }

    mongo::setGlobalServiceContext(mongo::ServiceContext::make());

    if (argc <= 1) {
        std::cout << usage;
        return 0;
    }

    const char* tempDir = nullptr;
    const char* pipelineStr = nullptr;
    auto outputType = mongo::OutputType::kBSON;
    bool explicitOutputType = false;
    int inputFileIndex = -1;
    for (int i = 1; i < argc; i++) {
        if (strlen(argv[i]) != 2 || argv[i][0] != '-') {
            inputFileIndex = i;
            break;
        }
        switch (argv[i][1]) {
            case 'h':
                std::cout << usage;
                return 0;
            case 'b':
                outputType = mongo::OutputType::kBSON;
                explicitOutputType = true;
                break;
            case 'j':
                outputType = mongo::OutputType::kJSON;
                explicitOutputType = true;
                break;
            case 't':
                tempDir = argv[++i];
                break;
            case 'e':
                pipelineStr = argv[++i];
                break;
            case '?':
                return 1;
        }
    }

    if (pipelineStr == nullptr) {
        std::cerr << "Must specify a pipeline expression with -e" << std::endl;
        std::cerr << std::endl;
        std::cerr << usage;
        return 1;
    }

    if (inputFileIndex != argc - 1) {
        std::cerr << "Must specify exactly one input file" << std::endl;
        std::cerr << std::endl;
        std::cerr << usage;
        return 1;
    }

#ifdef _WIN32
    bool outputtingToTerminal = GetFileType(GetStdHandle(STD_OUTPUT_HANDLE)) == FILE_TYPE_CHAR;
#else
    bool outputtingToTerminal = isatty(STDOUT_FILENO);
#endif
    if (outputtingToTerminal && !explicitOutputType && outputType == mongo::OutputType::kBSON) {
        std::cerr
            << "Attempting to write binary output to the terminal. If that's what you want, you"
            << std::endl
            << "can override this error with -b. Also consider -j for JSON text output."
            << std::endl;
        return 1;
    }

    return mongo::mqlrunMain(pipelineStr, argv[inputFileIndex], outputType, tempDir);
}
