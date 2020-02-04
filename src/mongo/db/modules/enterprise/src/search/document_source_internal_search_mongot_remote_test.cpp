/**
 *    Copyright (C) 2019 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "document_source_internal_search_mongot_remote.h"

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongot_options.h"

namespace mongo {
namespace {

using InternalSearchMongotRemoteTest = AggregationContextFixture;

TEST_F(InternalSearchMongotRemoteTest, SearchMongotRemoteNotAllowedInTransaction) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    expCtx->inMultiDocumentTransaction = true;
    globalMongotParams.host = "localhost:27027";
    globalMongotParams.enabled = true;

    auto specObj = BSON("$_internalSearchMongotRemote" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the mongotRemote stage.
    auto mongotRemoteStage = DocumentSourceInternalSearchMongotRemote::createFromBson(spec, expCtx);
    auto pipeline = Pipeline::create({mongotRemoteStage}, expCtx);
    ASSERT_NOT_OK(pipeline.getStatus());
    ASSERT_EQ(pipeline.getStatus(), ErrorCodes::OperationNotSupportedInTransaction);
}

TEST_F(InternalSearchMongotRemoteTest, SearchMongotRemoteReturnsEOFWhenCollDoesNotExist) {
    auto expCtx = getExpCtx();
    globalMongotParams.host = "localhost:27027";
    globalMongotParams.enabled = true;

    auto specObj = BSON("$_internalSearchMongotRemote" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the mongotRemote stage.
    auto mongotRemoteStage = DocumentSourceInternalSearchMongotRemote::createFromBson(spec, expCtx);
    ASSERT_TRUE(mongotRemoteStage->getNext().isEOF());
}

}  // namespace
}  // namespace mongo
