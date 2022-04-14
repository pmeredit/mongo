/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include <boost/intrusive_ptr.hpp>
#include <vector>

#include "document_source_internal_search_mongot_remote.h"
#include "document_source_search_meta.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/db/pipeline/document_source_union_with.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongot_options.h"

namespace mongo {

namespace {

using boost::intrusive_ptr;
using std::list;
using std::vector;

using SearchMetaTest = AggregationContextFixture;

struct MockMongoInterface final : public StubMongoProcessInterface {
    bool inShardedEnvironment(OperationContext* opCtx) const override {
        return false;
    }
};

TEST_F(SearchMetaTest, TestParsingOfSearchMeta) {
    const auto mongotQuery = fromjson("{query: 'cakes', path: 'title'}");
    auto specObj = BSON("$searchMeta" << mongotQuery);

    auto expCtx = getExpCtx();
    expCtx->mongoProcessInterface = std::make_unique<MockMongoInterface>();
    auto fromNs = NamespaceString("unittests.$cmd.aggregate");
    expCtx->setResolvedNamespaces(StringMap<ExpressionContext::ResolvedNamespace>{
        {fromNs.coll().toString(), {fromNs, std::vector<BSONObj>()}}});
    list<intrusive_ptr<DocumentSource>> results =
        DocumentSourceSearchMeta::createFromBson(specObj.firstElement(), expCtx);

    ASSERT_EQUALS(results.size(), 1UL);
    ASSERT(dynamic_cast<DocumentSourceSearchMeta*>(results.begin()->get()));

    // $searchMeta argument must be an object.
    specObj = BSON("$searchMeta" << 1000);
    ASSERT_THROWS_CODE(
        DocumentSourceSearchMeta::createFromBson(specObj.firstElement(), getExpCtx()),
        AssertionException,
        ErrorCodes::FailedToParse);
}

}  // namespace
}  // namespace mongo
