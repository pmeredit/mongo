/**
 *    Copyright (C) 2019 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "document_source_internal_search_beta_mongot_remote.h"

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongot_options.h"

namespace mongo {
namespace {

using InternalSearchBetaMongotRemoteTest = AggregationContextFixture;

TEST_F(InternalSearchBetaMongotRemoteTest, SearchBetaMongotRemoteNotAllowedInTransaction) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    expCtx->inMultiDocumentTransaction = true;
    globalMongotParams.host = "localhost:27027";
    globalMongotParams.enabled = true;

    auto specObj = BSON("$_internalSearchBetaMongotRemote" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the mongotRemote stage.
    auto mongotRemoteStage =
        DocumentSourceInternalSearchBetaMongotRemote::createFromBson(spec, expCtx);
    auto pipeline = Pipeline::create({mongotRemoteStage}, expCtx);
    ASSERT_NOT_OK(pipeline.getStatus());
    ASSERT_EQ(pipeline.getStatus(), ErrorCodes::OperationNotSupportedInTransaction);
}

}  // namespace
}  // namespace mongo
