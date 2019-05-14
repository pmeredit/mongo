/**
 *    Copyright (C) 2019 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>
#include <deque>
#include <vector>

#include "document_source_internal_search_beta_id_lookup.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_source_mock.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_value_test_util.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/stub_mongo_process_interface_lookup_single_document.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

namespace {

using boost::intrusive_ptr;
using std::deque;
using std::vector;

using MockMongoInterface = StubMongoProcessInterfaceLookupSingleDocument;
using InternalSearchBetaIdLookupTest = AggregationContextFixture;

TEST_F(InternalSearchBetaIdLookupTest, ShouldSkipResultsWhenIdNotFound) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    auto specObj = BSON("$_internalSearchBetaIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the idLookup stage.
    auto idLookupStage = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);

    // Mock its input.
    auto mockLocalSource =
        DocumentSourceMock::createForTest({Document{{"_id", 0}}, Document{{"_id", 1}}});
    idLookupStage->setSource(mockLocalSource.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{Document{{"_id", 0}, {"color", "red"_sd}}};
    expCtx->mongoProcessInterface = stdx::make_unique<MockMongoInterface>(mockDbContents);

    // We should find one document here with _id = 0.
    auto next = idLookupStage->getNext();
    ASSERT_TRUE(next.isAdvanced());
    ASSERT_DOCUMENT_EQ(next.releaseDocument(), (Document{{"_id", 0}, {"color", "red"_sd}}));

    ASSERT_TRUE(idLookupStage->getNext().isEOF());
    ASSERT_TRUE(idLookupStage->getNext().isEOF());
}

TEST_F(InternalSearchBetaIdLookupTest, ShouldNotRemoveMetadata) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();

    // Create a mock data source.
    MutableDocument docOne(Document({{"_id", 0}}));
    docOne.setSearchScore(0.123);
    DocumentSourceMock mockLocalSource({docOne.freeze()}, expCtx);

    // Set up the idLookup stage.
    auto specObj = BSON("$_internalSearchBetaIdLookup" << BSONObj());
    auto spec = specObj.firstElement();
    auto idLookupStage = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);
    idLookupStage->setSource(&mockLocalSource);

    // Set up a project stage that asks for metadata.
    auto projectSpec = fromjson("{$project: {score: {$meta: \"searchScore\"}, _id: 1, color: 1}}");
    auto projectStage = DocumentSourceProject::createFromBson(projectSpec.firstElement(), expCtx);
    projectStage->setSource(idLookupStage.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{
        Document{{"_id", 0}, {"color", "red"_sd}, {"something else", "will be projected out"_sd}}};
    expCtx->mongoProcessInterface = stdx::make_unique<MockMongoInterface>(mockDbContents);

    // We should find one document here with _id = 0.
    auto next = projectStage->getNext();
    ASSERT_TRUE(next.isAdvanced());
    ASSERT_DOCUMENT_EQ(next.releaseDocument(),
                       (Document{{"_id", 0}, {"color", "red"_sd}, {"score", 0.123}}));

    ASSERT_TRUE(idLookupStage->getNext().isEOF());
    ASSERT_TRUE(idLookupStage->getNext().isEOF());
}

TEST_F(InternalSearchBetaIdLookupTest, ShouldParseFromSerialized) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    auto specObj = BSON("$_internalSearchBetaIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the idLookup stage.
    auto idLookupStage = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);

    // Serialize the idLookup stage.
    vector<Value> serialization;
    idLookupStage->serializeToArray(serialization);
    ASSERT_EQ(serialization.size(), 1UL);
    ASSERT_EQ(serialization[0].getType(), BSONType::Object);

    auto serializedBson = serialization[0].getDocument().toBson();
    auto roundTripped = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(
        serializedBson.firstElement(), expCtx);

    // Serialize one more time to make sure we get the same thing.
    vector<Value> newSerialization;
    roundTripped->serializeToArray(newSerialization);

    ASSERT_EQ(newSerialization.size(), 1UL);
    ASSERT_VALUE_EQ(newSerialization[0], serialization[0]);
}

TEST_F(InternalSearchBetaIdLookupTest, ShouldFailParsingWhenSpecNotEmptyObject) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();

    ASSERT_THROWS_CODE(
        DocumentSourceInternalSearchBetaIdLookUp::createFromBson(BSON("$_internalSearchBetaIdLookup"
                                                                      << "string spec")
                                                                     .firstElement(),
                                                                 expCtx),
        AssertionException,
        31016);

    ASSERT_THROWS_CODE(DocumentSourceInternalSearchBetaIdLookUp::createFromBson(
                           BSON("$_internalSearchBetaIdLookup" << 42).firstElement(), expCtx),
                       AssertionException,
                       31016);

    ASSERT_THROWS_CODE(DocumentSourceInternalSearchBetaIdLookUp::createFromBson(
                           BSON("$_internalSearchBetaIdLookup" << BSON("not"
                                                                       << "empty"))
                               .firstElement(),
                           expCtx),
                       AssertionException,
                       31016);

    ASSERT_THROWS_CODE(DocumentSourceInternalSearchBetaIdLookUp::createFromBson(
                           BSON("$_internalSearchBetaIdLookup" << true).firstElement(), expCtx),
                       AssertionException,
                       31016);

    ASSERT_THROWS_CODE(
        DocumentSourceInternalSearchBetaIdLookUp::createFromBson(
            BSON("$_internalSearchBetaIdLookup" << OID("54651022bffebc03098b4567")).firstElement(),
            expCtx),
        AssertionException,
        31016);
}

TEST_F(InternalSearchBetaIdLookupTest, ShouldAllowStringOrObjectIdValues) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    auto specObj = BSON("$_internalSearchBetaIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the idLookup stage.
    auto idLookupStage = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);

    // Mock its input.
    auto mockLocalSource = DocumentSourceMock::createForTest(
        {Document{{"_id", "tango"_sd}},
         Document{{"_id", Document{{"number", 42}, {"irrelevant", "something"_sd}}}}});
    idLookupStage->setSource(mockLocalSource.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{
        Document{{"_id", "tango"_sd}, {"color", "red"_sd}},
        Document{{"_id", Document{{"number", 42}, {"irrelevant", "something"_sd}}}}};
    expCtx->mongoProcessInterface = stdx::make_unique<MockMongoInterface>(mockDbContents);

    // Find documents when _id is a string or document.
    auto next = idLookupStage->getNext();
    ASSERT_TRUE(next.isAdvanced());
    ASSERT_DOCUMENT_EQ(next.releaseDocument(),
                       (Document{{"_id", "tango"_sd}, {"color", "red"_sd}}));

    next = idLookupStage->getNext();
    ASSERT_TRUE(next.isAdvanced());
    ASSERT_DOCUMENT_EQ(
        next.releaseDocument(),
        (Document{{"_id", Document{{"number", 42}, {"irrelevant", "something"_sd}}}}));

    ASSERT_TRUE(idLookupStage->getNext().isEOF());
    ASSERT_TRUE(idLookupStage->getNext().isEOF());
}

TEST_F(InternalSearchBetaIdLookupTest, ShouldNotErrorOnEmptyResult) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    auto specObj = BSON("$_internalSearchBetaIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the idLookup stage.
    auto idLookupStage = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);

    // Mock its input.
    auto mockLocalSource = DocumentSourceMock::createForTest({});
    idLookupStage->setSource(mockLocalSource.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{Document{{"_id", 0}, {"color", "red"_sd}}};
    expCtx->mongoProcessInterface = stdx::make_unique<MockMongoInterface>(mockDbContents);

    ASSERT_TRUE(idLookupStage->getNext().isEOF());
    ASSERT_TRUE(idLookupStage->getNext().isEOF());
}

}  // namespace
}  // namespace mongo
