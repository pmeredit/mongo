/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>
#include <vector>

#include "document_source_internal_search_beta_id_lookup.h"
#include "document_source_search_beta.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_value_test_util.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

namespace {

using boost::intrusive_ptr;
using std::list;
using std::vector;

using SearchBetaTest = AggregationContextFixture;

TEST_F(SearchBetaTest, ShouldSerializeAndExplainAtQueryPlannerVerbosity) {
    BSONObj spec = fromjson("{$searchBeta: {}}");

    list<intrusive_ptr<DocumentSource>> result =
        DocumentSourceSearchBeta::createFromBson(spec.firstElement(), getExpCtx());

    ASSERT_EQUALS(result.size(), 1UL);

    const auto* idLookupStage =
        dynamic_cast<DocumentSourceInternalSearchBetaIdLookUp*>(result.front().get());
    ASSERT(idLookupStage);

    auto explain = ExplainOptions::Verbosity::kQueryPlanner;
    vector<Value> explainedStages;
    idLookupStage->serializeToArray(explainedStages, explain);
    ASSERT_EQUALS(explainedStages.size(), 1UL);

    Value expectedIdLookupExplain = Value(Document());
    auto idLookupExplain = explainedStages[0];
    ASSERT_VALUE_EQ(idLookupExplain["$_internalSearchBetaIdLookup"], expectedIdLookupExplain);
}

TEST_F(SearchBetaTest, ShouldSerializeAndExplainAtUnspecifiedVerbosity) {
    BSONObj spec = fromjson("{$searchBeta: {}}");

    list<intrusive_ptr<DocumentSource>> result =
        DocumentSourceSearchBeta::createFromBson(spec.firstElement(), getExpCtx());

    ASSERT_EQUALS(result.size(), 1UL);

    const auto* idLookupStage =
        dynamic_cast<DocumentSourceInternalSearchBetaIdLookUp*>(result.front().get());
    ASSERT(idLookupStage);

    auto explain = boost::none;
    vector<Value> explainedStages;
    idLookupStage->serializeToArray(explainedStages, explain);
    ASSERT_EQUALS(explainedStages.size(), 1UL);

    Value expectedIdLookupExplain = Value(Document());
    auto idLookupExplain = explainedStages[0];
    ASSERT_VALUE_EQ(idLookupExplain["$_internalSearchBetaIdLookup"], expectedIdLookupExplain);
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
