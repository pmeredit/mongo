/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "vector_search/document_source_vector_search.h"

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/unittest/death_test.h"
#include "search/document_source_internal_search_id_lookup.h"
#include "search/mongot_options.h"


namespace mongo {
namespace {

using boost::intrusive_ptr;
using DocumentSourceVectorSearchTest = AggregationContextFixture;

TEST_F(DocumentSourceVectorSearchTest, NotAllowedInTransaction) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    expCtx->opCtx->setInMultiDocumentTransaction();


    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            candidates: 100,
            limit: 10,
            index: "x_index"
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
            candidates: 100,
            limit: 10,
            index: "x_index"
        }
    })");

    auto vectorStage = DocumentSourceVectorSearch::createFromBson(spec.firstElement(), expCtx);
    ASSERT_TRUE(vectorStage.front()->getNext().isEOF());
}

TEST_F(DocumentSourceVectorSearchTest, HasTheCorrectStagesWhenCreated) {
    auto expCtx = getExpCtx();

    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            candidates: 100,
            limit: 10,
            index: "x_index"
        }
    })");

    auto vectorStage = DocumentSourceVectorSearch::createFromBson(spec.firstElement(), expCtx);
    ASSERT_EQUALS(vectorStage.size(), 2UL);

    const auto* vectorSearchStage =
        dynamic_cast<DocumentSourceVectorSearch*>(vectorStage.front().get());
    ASSERT(vectorSearchStage);

    const auto* idLookupStage =
        dynamic_cast<DocumentSourceInternalSearchIdLookUp*>(vectorStage.back().get());
    ASSERT(idLookupStage);
}

TEST_F(DocumentSourceVectorSearchTest, RedactsCorrectly) {
    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            candidates: 100,
            limit: 10,
            index: "x_index",
            filter: {
                x: {
                    "$gt": 0
                }
            }
        }
    })");

    auto vectorStage = DocumentSourceVectorSearch::createFromBson(spec.firstElement(), getExpCtx());

    ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
        R"({
            "$vectorSearch": {
                "queryVector": "?array<?number>",
                "path": "?string",
                "index": "HASH<x_index>",
                "limit": "?number",
                "candidates": "?number",
                "filter": {
                    "HASH<x>": {
                        "$gt": "?number"
                    }
                }
            }
        })",
        redact(*(vectorStage.front())));
}

}  // namespace
}  // namespace mongo
