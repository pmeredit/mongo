/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "vector_search/document_source_vector_search.h"

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/unittest/death_test.h"
#include "search/mongot_options.h"

namespace mongo {
namespace {

using DocumentSourceVectorSearchTest = AggregationContextFixture;

TEST_F(DocumentSourceVectorSearchTest, NotAllowedInTransaction) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    expCtx->opCtx->setInMultiDocumentTransaction();


    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            limit: 100,
            indexName: "x_index"
        }
    })");

    auto vectorStage = DocumentSourceVectorSearch::createFromBson(spec.firstElement(), expCtx);
    ASSERT_THROWS_CODE(Pipeline::create({vectorStage}, expCtx),
                       AssertionException,
                       ErrorCodes::OperationNotSupportedInTransaction);
}

TEST_F(DocumentSourceVectorSearchTest, EOFWhenCollDoesNotExist) {
    auto expCtx = getExpCtx();

    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            limit: 100,
            indexName: "x_index"
        }
    })");

    auto vectorStage = DocumentSourceVectorSearch::createFromBson(spec.firstElement(), expCtx);
    // TODO SERVER-78280 Enable this test.
    // ASSERT_TRUE(vectorStage->getNext().isEOF());
    ASSERT_THROWS_CODE(vectorStage->getNext(), AssertionException, ErrorCodes::NotImplemented);
}

TEST_F(DocumentSourceVectorSearchTest, RedactsCorrectly) {
    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            limit: 100,
            indexName: "x_index",
            filter: {
                x: {
                    "$gt": 0
                }
            }
        }
    })");

    auto vectorStage = DocumentSourceVectorSearch::createFromBson(spec.firstElement(), getExpCtx());

    // TODO SERVER-78279 Enable this test and ensure redaction includes all fields.
    // ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
    //     R"({"$vectorSearch":"?object"})",
    //     redact(*vectorStage));
    ASSERT_THROWS_CODE(redact(*vectorStage), AssertionException, ErrorCodes::NotImplemented);
}

}  // namespace
}  // namespace mongo
