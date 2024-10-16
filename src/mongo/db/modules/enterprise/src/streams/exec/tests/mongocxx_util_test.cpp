/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/server_error_code.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/bson/json.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/util/exception.h"

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
struct MongocxxExceptionToStatusTestCase {
    const mongocxx::exception& ex;
    const std::string& errorPrefix;
    const SPStatus& expected;
};


TEST(MongoCxxUtilTest, MongocxxExceptionToStatus) {
    auto transientAuthErrorMessage =
        "$merge to foobar failed : command update requires authentication";

    MongocxxExceptionToStatusTestCase tests[] = {
        {
            mongocxx::exception{ErrorCodes::Unauthorized,
                                mongocxx::server_error_category(),
                                transientAuthErrorMessage},
            "errorPrefix",
            SPStatus{Status{ErrorCodes::InternalError,
                            fmt::format("errorPrefix: {} : generic server error ",
                                        transientAuthErrorMessage)},
                     transientAuthErrorMessage},
        },
        {
            mongocxx::exception{ErrorCodes::Error{kAtlasErrorCode},
                                mongocxx::server_error_category(),
                                transientAuthErrorMessage},
            "errorPrefix",
            SPStatus{Status{ErrorCodes::InternalError,
                            fmt::format("errorPrefix: {} : generic server error ",
                                        transientAuthErrorMessage)},
                     transientAuthErrorMessage},
        },
    };

    for (auto tc : tests) {
        auto actual = mongocxxExceptionToStatus(
            tc.ex, mongocxx::uri{"mongodb://localhost:27017"}, tc.errorPrefix);
        ASSERT_EQUALS(tc.expected, actual);
    }
}

}  // namespace
}  // namespace streams
