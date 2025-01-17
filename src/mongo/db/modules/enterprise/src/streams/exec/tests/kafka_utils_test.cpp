/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "streams/exec/kafka_utils.h"

#include <vector>

using namespace mongo;

namespace streams {

TEST(KafkaUtilsTest, OnlyAllowedConfigurations) {
    mongo::BSONObjBuilder builder;
    builder.append("", "no value");
    builder.append("a", "a value");
    builder.append("b", "b value");
    builder.append("c", "c value");
    builder.append("d", "d value");
    builder.append("e", "e value");
    mongo::BSONObj configurations = builder.obj();

    mongo::stdx::unordered_set<std::string> allowedConfigurations = {"a", "b", "c"};

    std::vector<std::string> results;
    auto setConf = [&results](const std::string& _, std::string confValue) {
        results.emplace_back(confValue);
    };

    setKafkaConnectionConfigurations(configurations, setConf, allowedConfigurations);

    std::vector<std::string> expected = {"a value", "b value", "c value"};

    ASSERT_EQ(results, expected);
}

}  // namespace streams
