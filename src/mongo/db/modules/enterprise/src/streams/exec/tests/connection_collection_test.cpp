/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/stdx/thread.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/context.h"
#include "streams/exec/stages_gen.h"

namespace streams {

namespace {

using namespace mongo;


TEST(ConnectionCollectionTest, ShouldSuccessfullyInstantiate_WithCopyOfConnection) {
    std::unique_ptr<ConnectionCollection> connections;

    {
        auto bsonOptions = BSON("a" << 1);
        auto connection = Connection{"foo", ConnectionTypeEnum::Kafka, bsonOptions};
        ASSERT_FALSE(connection.isOwned());
        connections = std::make_unique<ConnectionCollection>(std::vector<Connection>{connection});
    }

    ASSERT_TRUE(connections->contains("foo"));
    auto fooConnection = connections->at("foo");
    ASSERT_TRUE(fooConnection.isOwned());
    ASSERT_EQ(fooConnection.getType(), ConnectionTypeEnum::Kafka);
    ASSERT_BSONOBJ_EQ(fooConnection.getOptions().getOwned(), BSON("a" << 1));
}

const std::vector<Connection> connectionsVector{
    {Connection{"conn1", ConnectionTypeEnum::Atlas, BSONObj{}},
     Connection{"conn2", ConnectionTypeEnum::Kafka, BSONObj{}},
     Connection{"conn3", ConnectionTypeEnum::HTTPS, BSONObj{}}}};

TEST(ConnectionCollectionTest, ShouldSuccessfullyUpdateConnection_HappyPath) {
    ConnectionCollection connections{connectionsVector};

    auto updatedConnection = Connection{"conn2", ConnectionTypeEnum::Kafka, BSON("a" << 1)};
    connections.update(updatedConnection);

    ASSERT_EQ(connections.at("conn2"), updatedConnection);
}


TEST(ConnectionCollectionTest, ShouldSuccessfullyUpdateConnection_WithCopyOfConnection) {
    ConnectionCollection connections{connectionsVector};

    {
        auto bsonOptions = BSON("a" << 1);
        auto connection = Connection{"conn2", ConnectionTypeEnum::Kafka, bsonOptions};
        ASSERT_FALSE(connection.isOwned());
        connections.update(connection);
    }

    ASSERT_TRUE(connections.contains("conn2"));
    auto updatedConnection = connections.at("conn2");
    ASSERT_TRUE(updatedConnection.isOwned());
    ASSERT_EQ(updatedConnection.getType(), ConnectionTypeEnum::Kafka);
    ASSERT_BSONOBJ_EQ(updatedConnection.getOptions().getOwned(), BSON("a" << 1));
}

TEST(ConnectionCollectionTest, ShouldFailToUpdateConnection_UnknownConnection) {
    ConnectionCollection connections{connectionsVector};

    auto updatedConnection = Connection{"conn4", ConnectionTypeEnum::Kafka, BSONObj{}};
    ASSERT_THROWS_CODE(
        connections.update(updatedConnection), DBException, ErrorCodes::InternalError);
    ASSERT_FALSE(connections.contains(updatedConnection.getName().toString()));
}

TEST(ConnectionCollectionTest, ShouldFailToUpdateConnection_MismatchingConnectionType) {
    Connection targetConnection{"conn2", ConnectionTypeEnum::Kafka, BSONObj{}};
    ConnectionCollection connections{connectionsVector};

    ASSERT_THROWS_CODE(connections.update(Connection{targetConnection.getName().toString(),
                                                     ConnectionTypeEnum::Atlas,
                                                     targetConnection.getOptions()}),
                       DBException,
                       ErrorCodes::InternalError);
    ASSERT_EQ(connections.at(targetConnection.getName().toString()), targetConnection);
}

TEST(ConnectionCollectionTest, ShouldSuccessfullyRetrieveConnection_HappyPath) {
    Connection targetConnection{"conn3", ConnectionTypeEnum::HTTPS, BSONObj{}};
    ConnectionCollection connections{connectionsVector};

    ASSERT_EQ(connections.at(targetConnection.getName().toString()), targetConnection);
}

TEST(ConnectionCollectionTest, ShouldFailToRetrieveConnection_UnknownConnection) {
    ConnectionCollection connections{connectionsVector};

    ASSERT_THROWS_CODE(
        connections.at("unknown-connection"), DBException, ErrorCodes::InternalError);
}

TEST(ConnectionCollectionTest, ShouldSuccessfullyRetrieveConnection_WithoutRaceCondition) {}

TEST(ConnectionCollectionTest, ShouldSuccessfullyFindConnection_HappyPath) {
    Connection targetConnection{"conn3", ConnectionTypeEnum::HTTPS, BSONObj{}};
    ConnectionCollection connections{connectionsVector};

    ASSERT_TRUE(connections.contains(targetConnection.getName().toString()));
}

TEST(ConnectionCollectionTest, ShouldFailToFindConnection_UnknownConnection) {
    ConnectionCollection connections{connectionsVector};

    ASSERT_FALSE(connections.contains("unknown-connection"));
}

TEST(ConnectionCollectionTest, ShouldFailToInit_DuplicateConnectionNames) {
    ASSERT_THROWS_CODE(
        ConnectionCollection({Connection{"conn1", ConnectionTypeEnum::Atlas, BSONObj{}},
                              Connection{"conn2", ConnectionTypeEnum::Kafka, BSONObj{}},
                              Connection{"conn1", ConnectionTypeEnum::HTTPS, BSONObj{}}}),
        DBException,
        ErrorCodes::InternalError);
}

TEST(ConnectionCollectionTest, ShouldFunctionWithoutRaceCondition) {
    constexpr int kIterations = 30;

    ConnectionCollection connections{connectionsVector};

    stdx::thread updaterThread{[&] {
        for (int i = 0; i < kIterations; i++) {
            BSONObjBuilder bsonObjBuilder;
            bsonObjBuilder << "iteration" << i;
            ASSERT_DOES_NOT_THROW(
                connections.update(Connection{"conn2", ConnectionTypeEnum::Kafka, BSON("a" << i)}));
        }
    }};

    stdx::thread getterThread{[&] {
        for (int i = 0; i < kIterations; i++) {
            ASSERT_DOES_NOT_THROW(connections.at("conn2"));
        }
    }};

    stdx::thread finderThread{[&] {
        for (int i = 0; i < kIterations; i++) {
            ASSERT_DOES_NOT_THROW(connections.contains("conn2"));
        }
    }};

    updaterThread.join();
    getterThread.join();
    finderThread.join();
}

}  // namespace
}  // namespace streams
