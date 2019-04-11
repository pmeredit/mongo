/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
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

#ifdef _WIN32
typedef SSIZE_T ssize_t;
#endif

namespace mongo {

namespace {

ssize_t readStdin(void* buf, size_t count) {
#ifdef _WIN32
    unsigned long bytesRead;
    HANDLE hFile = GetStdHandle(STD_INPUT_HANDLE);
    if (!ReadFile(hFile, buf, static_cast<unsigned long>(count), &bytesRead, nullptr)) {
        return GetLastError() == ERROR_BROKEN_PIPE ? 0 : -1;
    }
    return static_cast<ssize_t>(bytesRead);
#else
    return read(0, buf, count);
#endif
}

class StdinDocumentSource : public DocumentSource {
public:
    StdinDocumentSource(const boost::intrusive_ptr<ExpressionContext>& pCtx)
        : DocumentSource(pCtx) {}
    virtual ~StdinDocumentSource() {}

    virtual GetNextResult getNext() override {
        int size = 0;
        auto n = readStdin(&size, 4);
        if (n == -1) {
            std::cerr << "error reading from stdin" << std::endl;
            return GetNextResult::makeEOF();
        }
        if (n == 0) {
            return GetNextResult::makeEOF();
        }

        auto buf = SharedBuffer::allocate(size);
        invariant(buf.get());

        memcpy(buf.get(), &size, 4);
        int totalRead = 4;
        while ((n = readStdin(buf.get() + totalRead, size - totalRead)) > 0) {
            totalRead += n;
            if (totalRead == size) {
                break;
            }
        }
        if (n == -1) {
            std::cerr << "error reading from stdin" << std::endl;
            return GetNextResult::makeEOF();
        }
        if (n == 0) {
            std::cerr << "unexpected end of document" << std::endl;
            return GetNextResult::makeEOF();
        }
        invariant(totalRead == size);

        BSONObj obj(buf);
        return GetNextResult(Document(obj));
    }

    const char* getSourceName() const override {
        return "StdinDocumentSource";
    }

    Value serialize(boost::optional<ExplainOptions::Verbosity> explain) const override {
        return Value(Document{{getSourceName(), Document()}});
    }

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     HostTypeRequirement::kNone,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kAllowed);

        constraints.requiresInputDocSource = false;
        return constraints;
    }

    boost::intrusive_ptr<DocumentSource> optimize() override {
        return this;
    }

    boost::optional<MergingLogic> mergingLogic() override {
        return boost::none;
    }
};

}  // namespace

enum class OutputType {
    kJSON,
    kBSON,
};

BSONArray createPipelineArray(const char* pipelineStr) {
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
        boost::intrusive_ptr<ExpressionContext> expCtx;

        auto in = DocumentSourceBSONFile::create(expCtx, pipelineStr);

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

    BSONObj pipelineArray;
    try {
        pipelineArray = BSON("pipeline" << createPipelineArray(pipelineStr));
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

    auto client = getGlobalServiceContext()->makeClient("mqlrun");
    auto opCtx = client->makeOperationContext();
    boost::intrusive_ptr<ExpressionContext> expCtx;
    expCtx.reset(new ExpressionContext(opCtx.get(), nullptr));
    expCtx->allowDiskUse = true;  // For large sorts.
    if (tempDir) {
        expCtx->tempDir = tempDir;
    }

    std::unique_ptr<Pipeline, PipelineDeleter> pipeline;
    try {
        auto statusWithPipeline = Pipeline::parse(pipelineVector, expCtx);
        uassertStatusOK(statusWithPipeline.getStatus());
        pipeline = std::move(statusWithPipeline.getValue());
    } catch (AssertionException& e) {
        std::cerr << "Failed to parse pipeline: " << e.reason() << std::endl;
        return 1;
    }

    try {
        if (strlen(fileName) == 1 && fileName[0] == '-') {
            pipeline->addInitialSource(new StdinDocumentSource(expCtx));
        } else {
            auto bsonSource = DocumentSourceBSONFile::create(expCtx, fileName);
            pipeline->addInitialSource(bsonSource);
        }
    } catch (AssertionException& e) {
        std::cerr << e.reason() << std::endl;
        return false;
    }

    auto ws = std::make_unique<WorkingSet>();
    auto proxy = std::make_unique<PipelineProxyStage>(opCtx.get(), std::move(pipeline), ws.get());

    NamespaceString nss("db.data"_sd);
    auto planExec = PlanExecutor::make(
        opCtx.get(), std::move(ws), std::move(proxy), nss, PlanExecutor::NO_YIELD);

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
                case PlanExecutor::FAILURE:
                    std::cerr << "Failure: " << std::endl;
                    return 1;
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

int main(int argc, char* argv[], char** envp) {

    mongo::Status status = mongo::runGlobalInitializers(argc, argv, envp);

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
        return 1;
    }

    if (inputFileIndex != argc - 1) {
        std::cerr << "Must specify exactly one input file" << std::endl;
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
