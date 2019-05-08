/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>
#include <vector>

#include "document_source_internal_search_beta_id_lookup.h"
#include "document_source_internal_search_beta_mongot_remote.h"
#include "document_source_search_beta.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_value_test_util.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongot_options.h"

namespace mongo {

namespace {

using boost::intrusive_ptr;
using std::list;
using std::vector;

using SearchBetaTest = AggregationContextFixture;

TEST_F(SearchBetaTest, ShouldSerializeAndExplainAtQueryPlannerVerbosity) {
    const auto mongotQuery = fromjson("{term: 'asdf'}");
    const auto stageObj = BSON("$searchBeta" << mongotQuery);

    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();

    list<intrusive_ptr<DocumentSource>> results =
        DocumentSourceSearchBeta::createFromBson(stageObj.firstElement(), expCtx);
    ASSERT_EQUALS(results.size(), 2UL);

    const auto* mongotRemoteStage =
        dynamic_cast<DocumentSourceInternalSearchBetaMongotRemote*>(results.front().get());
    ASSERT(mongotRemoteStage);

    const auto* idLookupStage =
        dynamic_cast<DocumentSourceInternalSearchBetaIdLookUp*>(results.back().get());
    ASSERT(idLookupStage);

    auto explain = ExplainOptions::Verbosity::kQueryPlanner;
    vector<Value> explainedStages;
    mongotRemoteStage->serializeToArray(explainedStages, explain);
    idLookupStage->serializeToArray(explainedStages, explain);
    ASSERT_EQUALS(explainedStages.size(), 2UL);

    auto mongotRemoteExplain = explainedStages[0];
    ASSERT_DOCUMENT_EQ(mongotRemoteExplain.getDocument(),
                       Document({{"$_internalSearchBetaMongotRemote", Document(mongotQuery)}}));

    auto idLookupExplain = explainedStages[1];
    ASSERT_DOCUMENT_EQ(idLookupExplain.getDocument(),
                       Document({{"$_internalSearchBetaIdLookup", Document()}}));
}

TEST_F(SearchBetaTest, ShouldSerializeAndExplainAtUnspecifiedVerbosity) {
    const auto mongotQuery = fromjson("{term: 'asdf'}");
    const auto stageObj = BSON("$searchBeta" << mongotQuery);

    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();

    list<intrusive_ptr<DocumentSource>> results =
        DocumentSourceSearchBeta::createFromBson(stageObj.firstElement(), expCtx);
    ASSERT_EQUALS(results.size(), 2UL);

    const auto* mongotRemoteStage =
        dynamic_cast<DocumentSourceInternalSearchBetaMongotRemote*>(results.front().get());
    ASSERT(mongotRemoteStage);

    const auto* idLookupStage =
        dynamic_cast<DocumentSourceInternalSearchBetaIdLookUp*>(results.back().get());
    ASSERT(idLookupStage);

    auto explain = boost::none;
    vector<Value> explainedStages;
    mongotRemoteStage->serializeToArray(explainedStages, explain);
    idLookupStage->serializeToArray(explainedStages, explain);
    ASSERT_EQUALS(explainedStages.size(), 2UL);

    auto mongotRemoteExplain = explainedStages[0];
    ASSERT_DOCUMENT_EQ(mongotRemoteExplain.getDocument(),
                       Document({{"$_internalSearchBetaMongotRemote", Document(mongotQuery)}}));

    auto idLookupExplain = explainedStages[1];
    ASSERT_DOCUMENT_EQ(idLookupExplain.getDocument(),
                       Document({{"$_internalSearchBetaIdLookup", Document()}}));
}

TEST_F(SearchBetaTest, ShouldFailToParseIfSpecIsNotObject) {
    const auto specObj = fromjson("{$searchBeta: 1}");
    ASSERT_THROWS_CODE(
        DocumentSourceSearchBeta::createFromBson(specObj.firstElement(), getExpCtx()),
        AssertionException,
        ErrorCodes::FailedToParse);
}

}  // namespace
}  // namespace mongo
