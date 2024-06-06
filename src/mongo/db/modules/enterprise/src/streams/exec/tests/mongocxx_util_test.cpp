/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/bson/json.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "streams/exec/mongocxx_utils.h"

namespace streams {
namespace {

using namespace mongo;

TEST(MongoCxxUtilTest, RoundTripFromBsonObjToBsoncxxView) {
    BSONObj obj = fromjson(R"({a: 1, b: 2})");
    auto view = toBsoncxxView(obj);
    ASSERT_TRUE(SimpleBSONObjComparator::kInstance.evaluate(obj == fromBsoncxxDocument(view)))
        << "view: " << fromBsoncxxDocument(view).toString();
}

TEST(MongoCxxUtilTest, RoundTripFromBsonObjToBsoncxxValue) {
    BSONObj obj = fromjson(R"({a: 1, b: 2})");
    auto value = toBsoncxxValue(obj);
    ASSERT_TRUE(SimpleBSONObjComparator::kInstance.evaluate(obj == fromBsoncxxDocument(value)))
        << "value: " << fromBsoncxxDocument(value).toString();
}

}  // namespace
}  // namespace streams
