/**
 * Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "fle2_test_fixture.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/json.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace {

using namespace std::string_literals;
using namespace aggregate_expression_intender;

using TextSearchAggregateExpressionIntender = FLE2TestFixture;

TEST_F(TextSearchAggregateExpressionIntender, EncStrStartsWithMarksElementAsEncrypted) {
    auto exprBson = fromjson("{$encStrStartsWith: {input: \"$prefixField\", prefix:\"test\"}}");
    auto encryptedObj = buildTextSearchEncryptElem(
        "prefixField"_sd, "test", EncryptionPlaceholderContext::kTextPrefixComparison);
    auto serializedExpr = markAggExpressionForTextAndSerialize(exprBson, false, Intention::Marked);
    auto expected = BSON("$encStrStartsWith"
                         << BSON("input"
                                 << "$prefixField"
                                 << "prefix" << BSON("$const" << encryptedObj.firstElement())));

    ASSERT_EQ(Value::compare(Value(expected), serializedExpr, nullptr), 0);
}

TEST_F(TextSearchAggregateExpressionIntender, EncStrStartsWithMarksNestedElementAsEncrypted) {
    const auto exprBson =
        fromjson("{$encStrStartsWith: {input: \"$nested.prefixField\", prefix:\"test\"}}");
    auto encryptedObj = buildTextSearchEncryptElem(
        "nested.prefixField"_sd, "test", EncryptionPlaceholderContext::kTextPrefixComparison);
    auto serializedExpr = markAggExpressionForTextAndSerialize(exprBson, false, Intention::Marked);
    auto expected = BSON("$encStrStartsWith"
                         << BSON("input"
                                 << "$nested.prefixField"
                                 << "prefix" << BSON("$const" << encryptedObj.firstElement())));

    ASSERT_EQ(Value::compare(Value(expected), serializedExpr, nullptr), 0);
}
TEST_F(TextSearchAggregateExpressionIntender, EncStrStartsWithUnencryptedFieldFails) {
    const auto exprBson = fromjson("{$encStrStartsWith: {input: \"$foo\", prefix:\"test\"}}");
    ASSERT_THROWS_CODE(
        markAggExpressionForText(exprBson, false, Intention::Marked), AssertionException, 10112204);
}

}  // namespace
}  // namespace mongo
