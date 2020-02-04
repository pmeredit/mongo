/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>
#include <vector>

#include "document_source_internal_search_id_lookup.h"
#include "document_source_internal_search_mongot_remote.h"
#include "document_source_search.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongot_options.h"

namespace mongo {

namespace {

using boost::intrusive_ptr;
using std::list;
using std::vector;

using SearchTest = AggregationContextFixture;

TEST_F(SearchTest, ShouldSerializeAndExplainAtQueryPlannerVerbosity) {
    const auto mongotQuery = fromjson("{term: 'asdf'}");
    const auto stageObj = BSON("$search" << mongotQuery);

    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();

    list<intrusive_ptr<DocumentSource>> results =
        DocumentSourceSearch::createFromBson(stageObj.firstElement(), expCtx);
    ASSERT_EQUALS(results.size(), 2UL);

    const auto* mongotRemoteStage =
        dynamic_cast<DocumentSourceInternalSearchMongotRemote*>(results.front().get());
    ASSERT(mongotRemoteStage);

    const auto* idLookupStage =
        dynamic_cast<DocumentSourceInternalSearchIdLookUp*>(results.back().get());
    ASSERT(idLookupStage);

    auto explain = ExplainOptions::Verbosity::kQueryPlanner;
    vector<Value> explainedStages;
    mongotRemoteStage->serializeToArray(explainedStages, explain);
    idLookupStage->serializeToArray(explainedStages, explain);
    ASSERT_EQUALS(explainedStages.size(), 2UL);

    auto mongotRemoteExplain = explainedStages[0];
    ASSERT_DOCUMENT_EQ(mongotRemoteExplain.getDocument(),
                       Document({{"$_internalSearchMongotRemote", Document(mongotQuery)}}));

    auto idLookupExplain = explainedStages[1];
    ASSERT_DOCUMENT_EQ(idLookupExplain.getDocument(),
                       Document({{"$_internalSearchIdLookup", Document()}}));
}

TEST_F(SearchTest, ShouldSerializeAndExplainAtUnspecifiedVerbosity) {
    const auto mongotQuery = fromjson("{term: 'asdf'}");
    const auto stageObj = BSON("$search" << mongotQuery);

    auto expCtx = getExpCtx();
    expCtx->uuid = UUID::gen();

    list<intrusive_ptr<DocumentSource>> results =
        DocumentSourceSearch::createFromBson(stageObj.firstElement(), expCtx);
    ASSERT_EQUALS(results.size(), 2UL);

    const auto* mongotRemoteStage =
        dynamic_cast<DocumentSourceInternalSearchMongotRemote*>(results.front().get());
    ASSERT(mongotRemoteStage);

    const auto* idLookupStage =
        dynamic_cast<DocumentSourceInternalSearchIdLookUp*>(results.back().get());
    ASSERT(idLookupStage);

    auto explain = boost::none;
    vector<Value> explainedStages;
    mongotRemoteStage->serializeToArray(explainedStages, explain);
    idLookupStage->serializeToArray(explainedStages, explain);
    ASSERT_EQUALS(explainedStages.size(), 2UL);

    auto mongotRemoteExplain = explainedStages[0];
    ASSERT_DOCUMENT_EQ(mongotRemoteExplain.getDocument(),
                       Document({{"$_internalSearchMongotRemote", Document(mongotQuery)}}));

    auto idLookupExplain = explainedStages[1];
    ASSERT_DOCUMENT_EQ(idLookupExplain.getDocument(),
                       Document({{"$_internalSearchIdLookup", Document()}}));
}

TEST_F(SearchTest, ShouldFailToParseIfSpecIsNotObject) {
    const auto specObj = fromjson("{$search: 1}");
    ASSERT_THROWS_CODE(DocumentSourceSearch::createFromBson(specObj.firstElement(), getExpCtx()),
                       AssertionException,
                       ErrorCodes::FailedToParse);
}

}  // namespace
}  // namespace mongo
