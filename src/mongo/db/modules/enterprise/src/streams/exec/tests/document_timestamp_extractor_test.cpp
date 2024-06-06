/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/document_timestamp_extractor.h"

namespace streams {
namespace {

using namespace mongo;

TEST(DocumentTimestampExtractorTest, Basic) {
    boost::intrusive_ptr<ExpressionContextForTest> expCtx(new ExpressionContextForTest{});

    auto exprObject = fromjson(R"({
$toDate: {
  $multiply: ['$event_time_seconds', 1000]
}
})");
    auto expr = Expression::parseExpression(expCtx.get(), exprObject, expCtx->variablesParseState);
    auto extractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
    ASSERT_EQUALS(
        extractor->extractTimestamp(Document(fromjson("{event_time_seconds: 1677876150}"))),
        Date_t::fromMillisSinceEpoch(1677876150000));

    exprObject = fromjson("{$toDate: '$event_time_ms'}");
    expr = Expression::parseExpression(expCtx.get(), exprObject, expCtx->variablesParseState);
    extractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
    ASSERT_EQUALS(extractor->extractTimestamp(Document(fromjson("{event_time_ms: 1677876150055}"))),
                  Date_t::fromMillisSinceEpoch(1677876150055));

    exprObject = fromjson("{$toDate: '$event_time_date'}");
    expr = Expression::parseExpression(expCtx.get(), exprObject, expCtx->variablesParseState);
    extractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
    ASSERT_EQUALS(
        extractor->extractTimestamp(Document(fromjson("{event_time_date: '2023-03-03'}"))),
        Date_t::fromMillisSinceEpoch(1677801600000));

    exprObject = fromjson("{$toDate: '$event_time_datetime'}");
    expr = Expression::parseExpression(expCtx.get(), exprObject, expCtx->variablesParseState);
    extractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
    ASSERT_EQUALS(extractor->extractTimestamp(
                      Document(fromjson("{event_time_datetime: '2023-03-03T20:42:30.055Z'}"))),
                  Date_t::fromMillisSinceEpoch(1677876150055));

    exprObject = fromjson(R"({
$convert: {
  input: '$event_time_datetime',
  to: 'date'
} 
})");
    expr = Expression::parseExpression(expCtx.get(), exprObject, expCtx->variablesParseState);
    extractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
    ASSERT_EQUALS(extractor->extractTimestamp(
                      Document(fromjson("{event_time_datetime: '2023-03-03T20:42:30.055Z'}"))),
                  Date_t::fromMillisSinceEpoch(1677876150055));

    exprObject = fromjson(R"({
$dateFromParts: {
  'year': 2023,
  'month': 3,
  'day': 3,
  'hour': 20,
  'minute': 42,
  'second': 30,
  'millisecond': 55
}
})");
    expr = Expression::parseExpression(expCtx.get(), exprObject, expCtx->variablesParseState);
    extractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
    ASSERT_EQUALS(extractor->extractTimestamp(Document(fromjson("{a: 1}"))),
                  Date_t::fromMillisSinceEpoch(1677876150055));

    exprObject = fromjson(R"({
$dateAdd: {
  'startDate': {
    $dateFromString: {
      'dateString': '$event_time'
    }
  },
  'unit': 'month',
  'amount': 1
}
})");
    expr = Expression::parseExpression(expCtx.get(), exprObject, expCtx->variablesParseState);
    extractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
    ASSERT_EQUALS(
        extractor->extractTimestamp(Document(fromjson("{event_time: '2023-03-03T20:42:30.055Z'}"))),
        Date_t::fromMillisSinceEpoch(1680554550055));

    exprObject = fromjson(R"({
$dateSubtract: {
  'startDate': {
    $dateFromString: {
      'dateString': '$event_time'
    }
  },
  'unit': 'month',
  'amount': 1
}
})");
    expr = Expression::parseExpression(expCtx.get(), exprObject, expCtx->variablesParseState);
    extractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
    ASSERT_EQUALS(
        extractor->extractTimestamp(Document(fromjson("{event_time: '2023-03-03T20:42:30.055Z'}"))),
        Date_t::fromMillisSinceEpoch(1675456950055));

    exprObject = fromjson(R"({
$dateTrunc: {
  'date': {
    $dateFromString: {
      'dateString': '$event_time'
    }
  },
  'unit': 'month'
}
})");
    expr = Expression::parseExpression(expCtx.get(), exprObject, expCtx->variablesParseState);
    extractor = std::make_unique<DocumentTimestampExtractor>(expCtx, expr);
    ASSERT_EQUALS(
        extractor->extractTimestamp(Document(fromjson("{event_time: '2023-03-03T20:42:30.055Z'}"))),
        Date_t::fromMillisSinceEpoch(1677628800000));
}

}  // namespace
}  // namespace streams
