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
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/debug_util.h"
#include "streams/exec/context.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

using namespace mongo;
using namespace streams;

// This class makes it easy to run a MergeOperator with different configuration of
// whenMatched/whenNotMatched.
class StandaloneMergeOperator {
public:
    StandaloneMergeOperator(Context* context);

    boost::intrusive_ptr<DocumentSourceMerge> createMergeStage(BSONObj spec);

    void runReplaceInsertExperiment();
    void runKeepExistingInsertExperiment();
    void runMergeDiscardExperiment();
    void runMergeInsertExperimentWithOnFields();

private:
    Context* _context{nullptr};
};

StandaloneMergeOperator::StandaloneMergeOperator(Context* context) : _context(context) {}

boost::intrusive_ptr<DocumentSourceMerge> StandaloneMergeOperator::createMergeStage(BSONObj spec) {
    auto specElem = spec.firstElement();
    boost::intrusive_ptr<DocumentSourceMerge> mergeStage = dynamic_cast<DocumentSourceMerge*>(
        DocumentSourceMerge::createFromBson(specElem, _context->expCtx).get());
    return mergeStage;
}

void StandaloneMergeOperator::runReplaceInsertExperiment() {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "replace"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeStage = createMergeStage(std::move(spec));
    dassert(mergeStage);
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context, std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    // Write 5 docs and replace all of them.
    for (int i = 0; i < 5; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(
            fmt::format("{{_id: \"replace_insert_{}\", a: {}, customerId: {}}}", i, i, 100 + i))));
        dataMsg.docs.emplace_back(Document(fromjson(
            fmt::format("{{_id: \"replace_insert_{}\", b: {}, customerId: {}}}", i, i, 100 + i))));
    }
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    // Each of matched_count, modified_count and upserted_count in the bulk_write result
    // are 5 in this case.
}

void StandaloneMergeOperator::runKeepExistingInsertExperiment() {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "keepExisting"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeStage = createMergeStage(std::move(spec));
    dassert(mergeStage);
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context, std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    // Write 5 docs and and try to overwrite all of them.
    for (int i = 0; i < 5; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format(
            "{{_id: \"keepExisting_insert_{}\", a: {}, customerId: {}}}", i, i, 100 + i))));
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format(
            "{{_id: \"keepExisting_insert_{}\", b: {}, customerId: {}}}", i, i, 100 + i))));
    }
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    // Each of matched_count and upserted_count in the bulk_write result
    // are 5, while modified_count is 0.
}

void StandaloneMergeOperator::runMergeDiscardExperiment() {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "merge"
                                      << "whenNotMatched"
                                      << "discard"));
    auto mergeStage = createMergeStage(std::move(spec));
    dassert(mergeStage);
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context, std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    // Overwrite 5 docs written by runKeepExistingInsertExperiment() and try to
    // insert 5 new docs.
    for (int i = 0; i < 5; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format(
            "{{_id: \"keepExisting_insert_{}\", current_experiment_id: \"merge_discard_{}\"}}",
            i,
            i))));
        dataMsg.docs.emplace_back(Document(fromjson(
            fmt::format("{{_id: \"merge_discard_{}\", a: {}, customerId: {}}}", i, i, 100 + i))));
    }
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    // Each of matched_count and modified_count in the bulk_write result
    // are 5, while upserted_count is 0.
}

void StandaloneMergeOperator::runMergeInsertExperimentWithOnFields() {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "merge"
                                      << "whenNotMatched"
                                      << "insert"
                                      << "on" << BSON_ARRAY("current_experiment_id")));
    auto mergeStage = createMergeStage(std::move(spec));
    dassert(mergeStage);
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context, std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    // Overwrite 5 docs written by runMergeDiscardExperiment() and insert 5 new docs.
    for (int i = 0; i < 5; ++i) {
        dataMsg.docs.emplace_back(
            Document(fromjson(fmt::format("{{current_experiment_id: \"merge_discard_{}\", "
                                          "new_experiment_id: \"merge_insert_withOnFields_{}\"}}",
                                          i,
                                          i))));
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format(
            "{{current_experiment_id: \"merge_insert_withOnFields_{}\", a: {}, customerId: {}}}",
            i,
            i,
            100 + i))));
    }
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    // Each of matched_count, modified_count and upserted_count in the bulk_write result
    // are 5 in this case.
}

/**
 * Creates a MergeOperator runs it with different configuration of whenMatched/whenNotMatched.
 *
 * Sample command:
 * build/install/bin/streams_merge_operator_main --mongodb_uri=mongodb://localhost:27017
 * --database=test --collection=movies
 */
int main(int argc, char** argv) {
    using namespace boost::program_options;

    options_description desc{"Options"};
    desc.add_options()("mongodb_uri", value<std::string>(), "MongoDB URI.")(
        "database", value<std::string>(), "Name of the database.")(
        "collection", value<std::string>(), "Name of the collection.");
    variables_map flags;
    store(parse_command_line(argc, argv, desc), flags);
    notify(flags);

    runGlobalInitializersOrDie(std::vector<std::string>(argv, argv + argc));

    streams::MongoCxxClientOptions options;
    options.uri = flags["mongodb_uri"].as<std::string>();
    options.database = flags["database"].as<std::string>();
    options.collection = flags["collection"].as<std::string>();

    QueryTestServiceContext qtServiceContext;
    auto svcCtx = qtServiceContext.getServiceContext();
    auto metricManager = std::make_unique<MetricManager>();
    auto [context, _] = getTestContext(svcCtx);
    context->expCtx->setMongoProcessInterface(
        std::make_shared<MongoDBProcessInterface>(std::move(options)));

    StandaloneMergeOperator mergeOperator(context.get());
    mergeOperator.runReplaceInsertExperiment();
    mergeOperator.runKeepExistingInsertExperiment();
    mergeOperator.runMergeDiscardExperiment();
    mergeOperator.runMergeInsertExperimentWithOnFields();
    return 0;
}
