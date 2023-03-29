/**
 *    Copyright (C) 2019 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>
#include <deque>
#include <vector>

#include "document_source_internal_search_id_lookup.h"
#include "mongo/db/concurrency/locker_noop_service_context_test_fixture.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/document_source_mock.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/process_interface/stub_lookup_single_document_process_interface.h"
#include "mongo/unittest/temp_dir.h"

namespace mongo {

namespace {

using boost::intrusive_ptr;
using std::deque;
using std::vector;

using MockMongoInterface = StubLookupSingleDocumentProcessInterface;
const NamespaceString kTestNss =
    NamespaceString::createNamespaceString_forTest("unittests.pipeline_test");

class InternalSearchIdLookupTest : public LockerNoopServiceContextTest {
public:
    InternalSearchIdLookupTest() : InternalSearchIdLookupTest(NamespaceString(kTestNss)) {}

    InternalSearchIdLookupTest(NamespaceString nss) {
        _expCtx = new ExpressionContext(_opCtx.get(), nullptr, kTestNss);
        _expCtx->ns = std::move(nss);
        unittest::TempDir tempDir("AggregationContextFixture");
        _expCtx->tempDir = tempDir.path();

        _expCtx->mongoProcessInterface =
            std::make_unique<MockMongoInterface>(std::deque<DocumentSource::GetNextResult>());
    }

    boost::intrusive_ptr<ExpressionContext> getExpCtx() {
        return _expCtx.get();
    }

private:
    ServiceContext::UniqueOperationContext _opCtx = makeOperationContext();
    boost::intrusive_ptr<ExpressionContext> _expCtx;
};

TEST_F(InternalSearchIdLookupTest, ShouldSkipResultsWhenIdNotFound) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    auto specObj = BSON("$_internalSearchIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the idLookup stage.
    auto idLookupStage = DocumentSourceInternalSearchIdLookUp::createFromBson(spec, expCtx);

    // Mock its input.
    auto mockLocalSource =
        DocumentSourceMock::createForTest({Document{{"_id", 0}}, Document{{"_id", 1}}}, expCtx);
    idLookupStage->setSource(mockLocalSource.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{Document{{"_id", 0}, {"color", "red"_sd}}};
    expCtx->mongoProcessInterface =
        std::make_unique<StubLookupSingleDocumentProcessInterface>(mockDbContents);

    // We should find one document here with _id = 0.
    auto next = idLookupStage->getNext();
    ASSERT_TRUE(next.isAdvanced());
    ASSERT_DOCUMENT_EQ(next.releaseDocument(), (Document{{"_id", 0}, {"color", "red"_sd}}));

    ASSERT_TRUE(idLookupStage->getNext().isEOF());
    ASSERT_TRUE(idLookupStage->getNext().isEOF());
}

TEST_F(InternalSearchIdLookupTest, ShouldNotRemoveMetadata) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();

    // Create a mock data source.
    MutableDocument docOne(Document({{"_id", 0}}));
    docOne.metadata().setSearchScore(0.123);
    auto searchScoreDetails = BSON("scoreDetails"
                                   << "foo");
    docOne.metadata().setSearchScoreDetails(searchScoreDetails);
    DocumentSourceMock mockLocalSource({docOne.freeze()}, expCtx);

    // Set up the idLookup stage.
    auto specObj = BSON("$_internalSearchIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    auto idLookupStage = DocumentSourceInternalSearchIdLookUp::createFromBson(spec, expCtx);
    idLookupStage->setSource(&mockLocalSource);

    // Set up a project stage that asks for metadata.
    auto projectSpec = fromjson(
        "{$project: {score: {$meta: \"searchScore\"}, "
        "scoreInfo: {$meta: \"searchScoreDetails\"},"
        " _id: 1, color: 1}}");
    auto projectStage = DocumentSourceProject::createFromBson(projectSpec.firstElement(), expCtx);
    projectStage->setSource(idLookupStage.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{
        Document{{"_id", 0}, {"color", "red"_sd}, {"something else", "will be projected out"_sd}}};
    expCtx->mongoProcessInterface = std::make_unique<MockMongoInterface>(mockDbContents);

    // We should find one document here with _id = 0.
    auto next = projectStage->getNext();
    ASSERT_TRUE(next.isAdvanced());
    ASSERT_DOCUMENT_EQ(
        next.releaseDocument(),
        (Document{
            {"_id", 0}, {"color", "red"_sd}, {"score", 0.123}, {"scoreInfo", searchScoreDetails}}));

    ASSERT_TRUE(idLookupStage->getNext().isEOF());
    ASSERT_TRUE(idLookupStage->getNext().isEOF());
}

TEST_F(InternalSearchIdLookupTest, ShouldParseFromSerialized) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();

    DocumentSourceInternalSearchIdLookUp idLookupStage(expCtx);

    // Serialize the idLookup stage, as we would on mongos.
    vector<Value> serialization;
    idLookupStage.serializeToArray(serialization);
    ASSERT_EQ(serialization.size(), 1UL);
    ASSERT_EQ(serialization[0].getType(), BSONType::Object);

    BSONObj spec = BSON("$_internalSearchIdLookup" << BSONObj());
    ASSERT_BSONOBJ_EQ(serialization[0].getDocument().toBson(), spec);

    // On mongod we should be able to re-parse it.
    expCtx->inMongos = false;
    auto idLookupStageMongod =
        DocumentSourceInternalSearchIdLookUp::createFromBson(spec.firstElement(), expCtx);
    ASSERT_EQ(DocumentSourceInternalSearchIdLookUp::kStageName,
              idLookupStageMongod->getSourceName());
}

TEST_F(InternalSearchIdLookupTest, ShouldFailParsingWhenSpecNotEmptyObject) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();

    ASSERT_THROWS_CODE(
        DocumentSourceInternalSearchIdLookUp::createFromBson(BSON("$_internalSearchIdLookup"
                                                                  << "string spec")
                                                                 .firstElement(),
                                                             expCtx),
        AssertionException,
        31016);

    ASSERT_THROWS_CODE(DocumentSourceInternalSearchIdLookUp::createFromBson(
                           BSON("$_internalSearchIdLookup" << 42).firstElement(), expCtx),
                       AssertionException,
                       31016);

    ASSERT_THROWS_CODE(DocumentSourceInternalSearchIdLookUp::createFromBson(
                           BSON("$_internalSearchIdLookup" << BSON("not"
                                                                   << "empty"))
                               .firstElement(),
                           expCtx),
                       AssertionException,
                       31016);

    ASSERT_THROWS_CODE(DocumentSourceInternalSearchIdLookUp::createFromBson(
                           BSON("$_internalSearchIdLookup" << true).firstElement(), expCtx),
                       AssertionException,
                       31016);

    ASSERT_THROWS_CODE(
        DocumentSourceInternalSearchIdLookUp::createFromBson(
            BSON("$_internalSearchIdLookup" << OID("54651022bffebc03098b4567")).firstElement(),
            expCtx),
        AssertionException,
        31016);
}

TEST_F(InternalSearchIdLookupTest, ShouldAllowStringOrObjectIdValues) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    auto specObj = BSON("$_internalSearchIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the idLookup stage.
    auto idLookupStage = DocumentSourceInternalSearchIdLookUp::createFromBson(spec, expCtx);

    // Mock its input.
    auto mockLocalSource = DocumentSourceMock::createForTest(
        {Document{{"_id", "tango"_sd}},
         Document{{"_id", Document{{"number", 42}, {"irrelevant", "something"_sd}}}}},
        expCtx);
    idLookupStage->setSource(mockLocalSource.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{
        Document{{"_id", "tango"_sd}, {"color", "red"_sd}},
        Document{{"_id", Document{{"number", 42}, {"irrelevant", "something"_sd}}}}};
    expCtx->mongoProcessInterface = std::make_unique<MockMongoInterface>(mockDbContents);

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

TEST_F(InternalSearchIdLookupTest, ShouldNotErrorOnEmptyResult) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    auto specObj = BSON("$_internalSearchIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the idLookup stage.
    auto idLookupStage = DocumentSourceInternalSearchIdLookUp::createFromBson(spec, expCtx);

    // Mock its input.
    auto mockLocalSource = DocumentSourceMock::createForTest({}, expCtx);
    idLookupStage->setSource(mockLocalSource.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{Document{{"_id", 0}, {"color", "red"_sd}}};
    expCtx->mongoProcessInterface = std::make_unique<MockMongoInterface>(mockDbContents);

    ASSERT_TRUE(idLookupStage->getNext().isEOF());
    ASSERT_TRUE(idLookupStage->getNext().isEOF());
}

TEST_F(InternalSearchIdLookupTest, RedactsCorrectly) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    auto specObj = BSON("$_internalSearchIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    auto idLookupStage = DocumentSourceInternalSearchIdLookUp::createFromBson(spec, expCtx);

    SerializationOptions opts;
    opts.replacementForLiteralArgs = "?";
    std::vector<Value> vec;
    idLookupStage->serializeToArray(vec, opts);
    ASSERT_BSONOBJ_EQ(vec[0].getDocument().toBson(), specObj);

    vec.clear();
    auto limitedLookup = DocumentSourceInternalSearchIdLookUp(expCtx, 5);
    limitedLookup.serializeToArray(vec, opts);
    ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
        R"({
            "$_internalSearchIdLookup": {"limit": "?"}
        })",
        vec[0].getDocument().toBson());
}

}  // namespace
}  // namespace mongo
