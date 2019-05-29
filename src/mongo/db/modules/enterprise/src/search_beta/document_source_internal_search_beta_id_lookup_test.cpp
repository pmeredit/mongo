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
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
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

class InternalSearchBetaIdLookupTest : public ServiceContextTest {
public:
    InternalSearchBetaIdLookupTest()
        : InternalSearchBetaIdLookupTest(NamespaceString("unittests.pipeline_test")) {}

    InternalSearchBetaIdLookupTest(NamespaceString nss) {
        TimeZoneDatabase::set(getServiceContext(), std::make_unique<TimeZoneDatabase>());
        // Must instantiate ExpressionContext _after_ setting the TZ database on the service
        // context.
        _expCtx = new ExpressionContext(_opCtx.get(), nullptr);
        _expCtx->ns = std::move(nss);
        unittest::TempDir tempDir("AggregationContextFixture");
        _expCtx->tempDir = tempDir.path();

        _expCtx->mongoProcessInterface =
            stdx::make_unique<MockMongoInterface>(std::deque<DocumentSource::GetNextResult>());
    }

    boost::intrusive_ptr<ExpressionContext> getExpCtx() {
        return _expCtx.get();
    }

private:
    ServiceContext::UniqueOperationContext _opCtx = makeOperationContext();
    boost::intrusive_ptr<ExpressionContext> _expCtx;
};

TEST_F(InternalSearchBetaIdLookupTest, ShouldSkipResultsWhenIdNotFound) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    auto specObj = BSON("$_internalSearchBetaIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    // Set up the idLookup stage.
    auto idLookupStages = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);
    auto idLookupStage = idLookupStages.front();

    // Mock its input.
    auto mockLocalSource =
        DocumentSourceMock::createForTest({Document{{"_id", 0}}, Document{{"_id", 1}}});
    idLookupStage->setSource(mockLocalSource.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{Document{{"_id", 0}, {"color", "red"_sd}}};
    expCtx->mongoProcessInterface =
        stdx::make_unique<StubMongoProcessInterfaceLookupSingleDocument>(mockDbContents);

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

    auto idLookupStages = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);
    auto idLookupStage = idLookupStages.front();
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

    DocumentSourceInternalSearchBetaIdLookUp idLookupStage(expCtx);

    // Serialize the idLookup stage, as we would on mongos.
    vector<Value> serialization;
    idLookupStage.serializeToArray(serialization);
    ASSERT_EQ(serialization.size(), 1UL);
    ASSERT_EQ(serialization[0].getType(), BSONType::Object);

    BSONObj spec = BSON("$_internalSearchBetaIdLookup" << BSONObj());
    ASSERT_BSONOBJ_EQ(serialization[0].getDocument().toBson(), spec);

    // On mongod we should be able to re-parse it.
    expCtx->inMongos = false;
    auto idLookupStages =
        DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec.firstElement(), expCtx);

    // Mongod will add the shard filter here. See other tests for more specific coverage for
    // that behavior.
    ASSERT_EQ(idLookupStages.size(), 2u);
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
    auto idLookupStages = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);
    auto idLookupStage = idLookupStages.front();

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
    auto idLookupStages = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);
    auto idLookupStage = idLookupStages.front();

    // Mock its input.
    auto mockLocalSource = DocumentSourceMock::createForTest({});
    idLookupStage->setSource(mockLocalSource.get());

    // Mock documents for this namespace.
    deque<DocumentSource::GetNextResult> mockDbContents{Document{{"_id", 0}, {"color", "red"_sd}}};
    expCtx->mongoProcessInterface = stdx::make_unique<MockMongoInterface>(mockDbContents);

    ASSERT_TRUE(idLookupStage->getNext().isEOF());
    ASSERT_TRUE(idLookupStage->getNext().isEOF());
}

TEST_F(InternalSearchBetaIdLookupTest, IncludesShardFilterOnMongod) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    expCtx->inMongos = false;
    auto specObj = BSON("$_internalSearchBetaIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    // Parsing the _id lookup stage should resolve to an _id lookup stage followed by a shard
    // filter.
    auto idLookupStages = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);
    ASSERT_EQ(idLookupStages.size(), 2u);
    ASSERT(dynamic_cast<DocumentSourceInternalShardFilter*>(idLookupStages.back().get()) !=
           nullptr);
}

TEST_F(InternalSearchBetaIdLookupTest, NoShardFilterOnMongos) {
    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();
    expCtx->inMongos = true;
    auto specObj = BSON("$_internalSearchBetaIdLookup" << BSONObj());
    auto spec = specObj.firstElement();

    expCtx->mongoProcessInterface =
        stdx::make_unique<MockMongoInterface>(std::deque<DocumentSource::GetNextResult>());

    // We should be able to parse this stage on mongos, though no shard filter will be added.
    auto idLookupStages = DocumentSourceInternalSearchBetaIdLookUp::createFromBson(spec, expCtx);
    ASSERT_EQ(idLookupStages.size(), 1u);
}

}  // namespace
}  // namespace mongo
