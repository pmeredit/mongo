/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/tests/test_utils.h"
#include "mongo/db/matcher/parsed_match_expression_for_test.h"
#include "streams/exec/parser.h"
#include "streams/exec/test_constants.h"

using namespace mongo;

namespace streams {

std::unique_ptr<Context> getTestContext() {
    auto context = std::make_unique<Context>();
    context->streamName = "test";
    context->clientName = context->streamName + "-" + UUID::gen().toString();
    context->client = getGlobalServiceContext()->makeClient(context->clientName);
    context->opCtx = getGlobalServiceContext()->makeOperationContext(context->client.get());
    // TODO(STREAMS-219)-PrivatePreview: We should make sure we're constructing the context
    // appropriately here
    context->expCtx = make_intrusive<ExpressionContext>(
        context->opCtx.get(), std::unique_ptr<CollatorInterface>(nullptr), NamespaceString{});
    context->expCtx->allowDiskUse = false;
    // TODO(STREAMS-219)-PrivatePreview: Considering exposing this as a parameter.
    // Or, set a parameter to dis-allow spilling.
    // We're using the same default as in run_aggregate.cpp.
    // This tempDir is used for spill to disk in $sort, $group, etc. stages
    // in window inner pipelines.
    context->expCtx->tempDir = storageGlobalParams.dbpath + "/_tmp";
    return context;
}

BSONObj getTestLogSinkSpec() {
    return BSON(Parser::kEmitStageName << BSON("connectionName" << kTestTypeLogToken));
}

BSONObj getTestMemorySinkSpec() {
    return BSON(Parser::kEmitStageName << BSON("connectionName" << kTestTypeMemoryToken));
}

BSONObj getTestSourceSpec() {
    return BSON(Parser::kSourceStageName << BSON("connectionName" << kTestTypeMemoryToken));
}

};  // namespace streams
