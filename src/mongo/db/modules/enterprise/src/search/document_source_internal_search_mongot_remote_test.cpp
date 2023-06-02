/**
 *    Copyright (C) 2019 MongoDB Inc.
 */

#include "document_source_internal_search_mongot_remote.h"

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/unittest/death_test.h"
#include "mongot_options.h"

namespace mongo {
namespace {

using InternalSearchMongotRemoteTest = AggregationContextFixture;

TEST_F(InternalSearchMongotRemoteTest, SearchMongotRemoteNotAllowedInTransaction) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    expCtx->opCtx->setInMultiDocumentTransaction();
    globalMongotParams.host = "localhost:27027";
    globalMongotParams.enabled = true;

    auto specObj = BSON("$_internalSearchMongotRemote"
                        << BSON("mongotQuery" << BSONObj() << "metadataMergeProtocolVersion" << 1));
    auto spec = specObj.firstElement();

    // Set up the mongotRemote stage.
    auto mongotRemoteStage = DocumentSourceInternalSearchMongotRemote::createFromBson(spec, expCtx);
    ASSERT_THROWS_CODE(Pipeline::create({mongotRemoteStage}, expCtx),
                       AssertionException,
                       ErrorCodes::OperationNotSupportedInTransaction);
}

TEST_F(InternalSearchMongotRemoteTest, SearchMongotRemoteReturnsEOFWhenCollDoesNotExist) {
    auto expCtx = getExpCtx();
    globalMongotParams.host = "localhost:27027";
    globalMongotParams.enabled = true;

    auto specObj = BSON("$_internalSearchMongotRemote"
                        << BSON("mongotQuery" << BSONObj() << "metadataMergeProtocolVersion" << 1));
    auto spec = specObj.firstElement();

    // Set up the mongotRemote stage.
    auto mongotRemoteStage = DocumentSourceInternalSearchMongotRemote::createFromBson(spec, expCtx);
    ASSERT_TRUE(mongotRemoteStage->getNext().isEOF());
}

TEST_F(InternalSearchMongotRemoteTest, RedactsCorrectly) {
    auto spec = BSON("$_internalSearchMongotRemote"
                     << BSON("mongotQuery" << BSONObj() << "metadataMergeProtocolVersion" << 1));

    auto mongotRemoteStage =
        DocumentSourceInternalSearchMongotRemote::createFromBson(spec.firstElement(), getExpCtx());

    ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
        R"({
            "$_internalSearchMongotRemote": {
                "mongotQuery": "?object",
                "metadataMergeProtocolVersion": "?number",
                "limit": "?number"
            }
        })",
        redact(*mongotRemoteStage));
}

TEST_F(InternalSearchMongotRemoteTest, RedactsCorrectlyWithMergingPipeline) {
    auto spec = fromjson(R"({
        $_internalSearchMongotRemote: {
            mongotQuery: { },
            metadataMergeProtocolVersion: 1,
            mergingPipeline: [
                {
                    $group: {
                        _id: "$x",
                        count: {
                            "$sum": 1
                        }
                    }
                }
            ]
        }
    })");

    auto mongotRemoteStage =
        DocumentSourceInternalSearchMongotRemote::createFromBson(spec.firstElement(), getExpCtx());

    ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
        R"({
            "$_internalSearchMongotRemote": {
                "mongotQuery": "?object",
                "metadataMergeProtocolVersion": "?number",
                "limit": "?number",
                "mergingPipeline": [
                    {
                        "$group": {
                            "_id": "$HASH<x>",
                            "HASH<count>": {
                                "$sum": "?number"
                            }
                        }
                    }
                ]
            }
        })",
        redact(*mongotRemoteStage));
}

DEATH_TEST_REGEX_F(InternalSearchMongotRemoteTest, InvalidSortSpec, "Tripwire assertion.*7320404") {
    auto expCtx = getExpCtx();
    auto specObj = BSON("$_internalSearchMongotRemote"
                        << BSON("mongotQuery" << BSONObj() << "metadataMergeProtocolVersion" << 1
                                              << "sortSpec" << BSON("$searchSortValues.a.b" << 1)));
    ASSERT_THROWS_CODE(
        DocumentSourceInternalSearchMongotRemote::createFromBson(specObj.firstElement(), expCtx),
        DBException,
        7320404);
}

}  // namespace
}  // namespace mongo
