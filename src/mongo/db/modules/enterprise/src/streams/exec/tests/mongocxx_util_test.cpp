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
    const std::string& uri;
    const std::string& errorPrefix;
    const SPStatus& expected;
};


TEST(MongoCxxUtilTest, MongocxxExceptionToStatus) {
    auto transientAuthErrorMessage =
        "$merge to foobar failed : command update requires authentication";

    auto meshURIErrorMessage =
        "no suitable servers found: [error on 'some-url.mesh.mongodb.net:3333'] [error on: "
        "'some-url.mesh.mongodb.net:3333']";
    auto meshURIErrorMessageWithRedactions =
        "no suitable servers found: [error on 'some-url.mongodb.net'] [error on: "
        "'some-url.mongodb.net']";

    MongocxxExceptionToStatusTestCase tests[] = {
        {
            mongocxx::exception{ErrorCodes::Unauthorized,
                                mongocxx::server_error_category(),
                                transientAuthErrorMessage},
            "mongodb://localhost:27017",
            "errorPrefix",
            SPStatus{Status{ErrorCodes::InternalError,
                            fmt::format("errorPrefix: {}: generic server error",
                                        transientAuthErrorMessage)},
                     fmt::format("{}: generic server error", transientAuthErrorMessage)},
        },
        {
            mongocxx::exception{ErrorCodes::Error{kAtlasErrorCode},
                                mongocxx::server_error_category(),
                                transientAuthErrorMessage},
            "mongodb://localhost:27017",
            "errorPrefix",
            SPStatus{Status{ErrorCodes::InternalError,
                            fmt::format("errorPrefix: {}: generic server error",
                                        transientAuthErrorMessage)},
                     fmt::format("{}: generic server error", transientAuthErrorMessage)},
        },

        {
            mongocxx::exception{ErrorCodes::Error{ErrorCodes::NotWritablePrimary},
                                mongocxx::server_error_category(),
                                meshURIErrorMessage},
            "mongodb://some-url.mesh.mongodb.net:3333",
            "errorPrefix",
            SPStatus{Status{ErrorCodes::StreamProcessorAtlasConnectionError,
                            fmt::format("errorPrefix: {}: generic server error",
                                        meshURIErrorMessageWithRedactions)},
                     fmt::format("{}: generic server error", meshURIErrorMessage)},
        },
    };

    for (auto tc : tests) {
        auto actual = mongocxxExceptionToStatus(tc.ex, mongocxx::uri{tc.uri}, tc.errorPrefix);
        ASSERT_EQUALS(tc.expected, actual);
    }
}

}  // namespace
}  // namespace streams
