/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/document_source_feeder.h"

namespace streams {
namespace {

using namespace mongo;

using DocumentSourceFeederTest = AggregationContextFixture;

TEST_F(DocumentSourceFeederTest, Limit1) {
    boost::intrusive_ptr<DocumentSourceFeeder> source(new DocumentSourceFeeder(getExpCtx()));
    auto limit =
        DocumentSourceLimit::createFromBson(BSON("$limit" << 1).firstElement(), getExpCtx());
    limit->setSource(source.get());
    auto next = limit->getNext();
    ASSERT(next.isPaused());

    source->addDocument(Document(fromjson("{a: 1}")));
    source->addDocument(Document(fromjson("{a: 2}")));
    next = limit->getNext();
    ASSERT(next.isAdvanced());
    ASSERT_VALUE_EQ(Value(1), next.getDocument().getField("a"));
    ASSERT(limit->getNext().isEOF());
    ASSERT_TRUE(source->isDisposed());
}

}  // namespace
}  // namespace streams
