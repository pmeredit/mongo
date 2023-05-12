/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include <boost/intrusive_ptr.hpp>

#include "document_source_list_search_indexes.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
namespace mongo {

namespace {

using boost::intrusive_ptr;

using ListSearchIndexesTest = AggregationContextFixture;

TEST_F(ListSearchIndexesTest, ShouldParseWithIdNameOrEmptyObject) {
    auto expCtx = getExpCtx();

    // Test parsing with an 'id' field
    auto specObj = BSON("$listSearchIndexes" << BSON("id"
                                                     << "indexID"));
    intrusive_ptr<DocumentSource> result =
        DocumentSourceListSearchIndexes::createFromBson(specObj.firstElement(), expCtx);
    ASSERT(dynamic_cast<DocumentSourceListSearchIndexes*>(result.get()));

    // Test parsing with an 'name' field.
    specObj = BSON("$listSearchIndexes" << BSON("name"
                                                << "indexName"));
    result = DocumentSourceListSearchIndexes::createFromBson(specObj.firstElement(), expCtx);
    ASSERT(dynamic_cast<DocumentSourceListSearchIndexes*>(result.get()));

    // Test parsing with no fields.
    specObj = BSON("$listSearchIndexes" << BSONObj());
    result = DocumentSourceListSearchIndexes::createFromBson(specObj.firstElement(), expCtx);
    ASSERT(dynamic_cast<DocumentSourceListSearchIndexes*>(result.get()));
}

TEST_F(ListSearchIndexesTest, ShouldFailToParse) {
    auto expCtx = getExpCtx();

    // Test parsing with an unknown field.
    auto specObj = BSON("$listSearchIndexes" << BSON("unknown"
                                                     << "unknownValue"));
    ASSERT_THROWS_CODE(
        DocumentSourceListSearchIndexes::createFromBson(specObj.firstElement(), getExpCtx()),
        AssertionException,
        40415 /* IDL unknown field error */);

    // Test parsing with not an object.
    specObj = BSON("$listSearchIndexes" << 1999);
    ASSERT_THROWS_CODE(
        DocumentSourceListSearchIndexes::createFromBson(specObj.firstElement(), getExpCtx()),
        AssertionException,
        ErrorCodes::FailedToParse);
}

TEST_F(ListSearchIndexesTest, RedactsNameFieldCorrectly) {
    auto expCtx = getExpCtx();
    auto specObj = BSON("$listSearchIndexes" << BSON("name"
                                                     << "indexName"));
    auto docSource =
        DocumentSourceListSearchIndexes::createFromBson(specObj.firstElement(), expCtx);
    ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
        R"({
            $listSearchIndexes: {
                name: "HASH<indexName>"
            }
        })",
        redact(*docSource));
}

TEST_F(ListSearchIndexesTest, RedactsIDFieldCorrectly) {
    auto expCtx = getExpCtx();
    auto specObj = BSON("$listSearchIndexes" << BSON("id"
                                                     << "indexID"));
    auto docSource =
        DocumentSourceListSearchIndexes::createFromBson(specObj.firstElement(), expCtx);
    ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
        R"({
            $listSearchIndexes: {
                id: "HASH<indexID>"
            }
        })",
        redact(*docSource));
}

TEST_F(ListSearchIndexesTest, RedactsEmptyObjCorrectly) {
    auto expCtx = getExpCtx();
    auto specObj = BSON("$listSearchIndexes" << BSONObj());
    auto docSource =
        DocumentSourceListSearchIndexes::createFromBson(specObj.firstElement(), expCtx);
    ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
        R"({
            $listSearchIndexes: {}
        })",
        redact(*docSource));
}


}  // namespace
}  // namespace mongo
