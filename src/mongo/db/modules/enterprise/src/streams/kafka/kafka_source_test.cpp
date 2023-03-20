/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/unittest/unittest.h"
#include <rdkafkacpp.h>

namespace streams {
namespace {

using namespace mongo;

TEST(KafkaSourceTest, HelloWorld) {
    std::unique_ptr<RdKafka::Conf> conf{RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)};
    ASSERT_TRUE(conf != nullptr);
}

}  // namespace
}  // namespace streams
