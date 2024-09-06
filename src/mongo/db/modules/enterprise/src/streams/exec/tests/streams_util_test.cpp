#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "streams/exec/util.h"
#include <string>

namespace streams {

TEST(UtilTest, ShouldDetermineConfluentBrokerForOneServer) {
    auto isConfluentBrokerRslt = isConfluentBroker("is-a.confluent.cloud:broker");
    ASSERT_TRUE(isConfluentBrokerRslt);
}

TEST(UtilTest, ShouldDetermineConfluentBrokerForMultipleServers) {
    auto isConfluentBrokerRslt =
        isConfluentBroker("is-a.confluent.cloud:broker,is-also-a.confluent.cloud:broker");
    ASSERT_TRUE(isConfluentBrokerRslt);
}

TEST(UtilTest, ShouldDetermineIsNotAConfluentBroker) {
    auto isConfluentBrokerRslt =
        isConfluentBroker("is.not.confluent.broker:3000,is.not.confluent.broker:3001");
    ASSERT_FALSE(isConfluentBrokerRslt);
}

}  // namespace streams
