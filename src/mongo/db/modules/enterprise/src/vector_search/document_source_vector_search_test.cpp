/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "vector_search/document_source_vector_search.h"
#include "vector_search/filter_validator.h"

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
            numCandidates: 100,
            limit: 10
        }
    })");

    auto vectorStage = DocumentSourceVectorSearch::createFromBson(spec.firstElement(), expCtx);
    ASSERT_THROWS_CODE(Pipeline::create({vectorStage}, expCtx),
                       AssertionException,
                       ErrorCodes::OperationNotSupportedInTransaction);
}

TEST_F(DocumentSourceVectorSearchTest, NotAllowedInvalidFilter) {
    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            numCandidates: 100,
            limit: 10,
            filter: {
                x: {
                    "$exists": false
                }
            }
        }
    })");

    ASSERT_THROWS_CODE(DocumentSourceVectorSearch::createFromBson(spec.firstElement(), getExpCtx()),
                       AssertionException,
                       7828300);
}

TEST_F(DocumentSourceVectorSearchTest, EOFWhenCollDoesNotExist) {
    auto expCtx = getExpCtx();

    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            numCandidates: 100,
            limit: 10
        }
    })");

    auto vectorStage = DocumentSourceVectorSearch::createFromBson(spec.firstElement(), expCtx);
    ASSERT_TRUE(vectorStage.front()->getNext().isEOF());
}

TEST_F(DocumentSourceVectorSearchTest, HasTheCorrectStagesWhenCreated) {
    auto expCtx = getExpCtx();
    struct MockMongoInterface final : public StubMongoProcessInterface {
        bool inShardedEnvironment(OperationContext* opCtx) const override {
            return false;
        }

        bool isExpectedToExecuteQueries() override {
            return true;
        }
    };
    expCtx->mongoProcessInterface = std::make_unique<MockMongoInterface>();

    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            numCandidates: 100,
            limit: 10
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
            numCandidates: 100,
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
                "numCandidates": "?number",
                "filter": {
                    "HASH<x>": {
                        "$gt": "?number"
                    }
                }
            }
        })",
        redact(*(vectorStage.front())));
}

TEST_F(DocumentSourceVectorSearchTest, OptionalArgumentsAreNotSpecified) {
    auto spec = fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            limit: 10
        }
    })");

    auto vectorStage = DocumentSourceVectorSearch::createFromBson(spec.firstElement(), getExpCtx());

    ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
        R"({
            "$vectorSearch": {
                "queryVector": "?array<?number>",
                "path": "?string",
                "limit": "?number"
            }
        })",
        redact(*(vectorStage.front())));
}

/**
 * Helper function that parses the $vectorSearch aggregation stage from the input, serializes it
 * to its representative shape, re-parses the representative shape, and compares to the original.
 */
void assertRepresentativeShapeIsStable(auto expCtx,
                                       BSONObj inputStage,
                                       BSONObj expectedRepresentativeStage) {
    auto parsedStage =
        *DocumentSourceVectorSearch::createFromBson(inputStage.firstElement(), expCtx).begin();
    std::vector<Value> serialization;
    auto opts = SerializationOptions{LiteralSerializationPolicy::kToRepresentativeParseableValue};
    parsedStage->serializeToArray(serialization, opts);

    auto serializedStage = serialization[0].getDocument().toBson();
    ASSERT_BSONOBJ_EQ(serializedStage, expectedRepresentativeStage);

    auto roundTripped =
        *DocumentSourceVectorSearch::createFromBson(serializedStage.firstElement(), expCtx).begin();

    std::vector<Value> newSerialization;
    roundTripped->serializeToArray(newSerialization, opts);
    ASSERT_EQ(newSerialization.size(), 1UL);
    ASSERT_VALUE_EQ(newSerialization[0], serialization[0]);
}

TEST_F(DocumentSourceVectorSearchTest, RoundTripSerialization) {
    assertRepresentativeShapeIsStable(getExpCtx(),
                                      fromjson(R"({
        $vectorSearch: {
            queryVector: [1.0, 2.0],
            path: "x",
            numCandidates: 100,
            limit: 10,
            index: "x_index",
            filter: {
                x: {
                    "$gt": 0
                }
            }
        }
    })"),
                                      fromjson(R"({
            "$vectorSearch": {
                "queryVector": [1],
                "path": "?",
                "index": "x_index",
                "limit": 1,
                "numCandidates": 1,
                "filter": {
                    "x": {
                        "$gt": 1
                    }
                }
            }
        })"));
}

}  // namespace
}  // namespace mongo
