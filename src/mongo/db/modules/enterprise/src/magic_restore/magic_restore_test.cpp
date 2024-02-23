/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "magic_restore/magic_restore.h"
#include "magic_restore/magic_restore_structs_gen.h"
#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/storage_interface_impl.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include <fstream>
#include <iostream>
#include <sstream>

namespace mongo {
namespace repl {

// Used to mock an input stream for BSONStreamReader. Handles writing BSON object raw data into the
// stream.
class MockStream {
public:
    MockStream() = default;

    MockStream& operator<<(const BSONObj& obj) {
        _stream.write(obj.objdata(), obj.objsize());
        return *this;
    }

    // BSONStreamReader needs access to the underlying std::istream.
    std::stringstream& stream() {
        return _stream;
    }

    void setStreamState(std::ios::iostate state) {
        _stream.setstate(state);
    }

private:
    // std::stringstream derives from std::istream, and is easier to write test data into than an
    // input-specific stream.
    std::stringstream _stream;
};


TEST(MagicRestore, BSONStreamReaderReadOne) {
    auto mock = MockStream();
    auto obj = BSON("_id" << 1);
    mock << obj;

    auto reader = mongo::magic_restore::BSONStreamReader(mock.stream());
    ASSERT(reader.hasNext());
    auto bsonObj = reader.getNext();
    ASSERT_BSONOBJ_EQ(obj, bsonObj);

    ASSERT_EQUALS(reader.getTotalBytesRead(), obj.objsize());
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 1);
    mock.setStreamState(std::ios::eofbit);
    ASSERT(!reader.hasNext());
}

TEST(MagicRestore, BSONStreamReaderReadMultiple) {
    auto obj1 = BSON("_id" << 1);
    auto obj2 = BSON("_id" << 2 << "array" << BSON_ARRAY(BSON("_id" << 3) << BSON("_id" << 4)));
    auto obj3 = BSON("_id" << 5 << "key"
                           << "value");

    auto mock = MockStream();
    // Multiple items in the stream are parsed one by one.
    mock << obj1;
    mock << obj2;

    auto reader = mongo::magic_restore::BSONStreamReader(mock.stream());

    ASSERT(reader.hasNext());
    ASSERT_BSONOBJ_EQ(obj1, reader.getNext());
    ASSERT(reader.hasNext());
    ASSERT_BSONOBJ_EQ(obj2, reader.getNext());

    // An item that is sent to the stream after the BSON reader began reading is parsed correctly.
    mock << obj3;
    ASSERT(reader.hasNext());
    ASSERT_BSONOBJ_EQ(obj3, reader.getNext());

    ASSERT_EQUALS(reader.getTotalBytesRead(), obj1.objsize() + obj2.objsize() + obj3.objsize());
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 3);
    mock.setStreamState(std::ios::eofbit);
    ASSERT(!reader.hasNext());
}

TEST(MagicRestore, BSONStreamReaderEmptyBSON) {
    auto mock = MockStream();
    auto obj = BSONObj();
    mock << obj;
    auto reader = mongo::magic_restore::BSONStreamReader(mock.stream());

    // Empty BSON objects should parse correctly.
    auto readBson = reader.getNext();
    ASSERT_BSONOBJ_EQ(obj, readBson);

    ASSERT_EQUALS(reader.getTotalBytesRead(), obj.objsize());
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 1);
    // Empty BSON objects cannot be parsed into a valid oplog entry.
    ASSERT_THROWS_CODE(OplogEntry(readBson), mongo::DBException, ErrorCodes::IDLFailedToParse);
}

DEATH_TEST(MagicRestore, BSONStreamReaderNegativeSize, "Parsed invalid BSON length") {
    // Insert a negative BSON size by inserting -11 into the first four bytes of the stream.
    char negativeSize[4] = {static_cast<char>((-11 >> 0) & 0xFF),
                            static_cast<char>((-11 >> 8) & 0xFF),
                            static_cast<char>((-11 >> 16) & 0xFF),
                            static_cast<char>((-11 >> 24) & 0xFF)};

    std::stringstream stream(std::string(negativeSize, 4));
    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore, BSONStreamReaderSmallSize, "Parsed invalid BSON length") {
    // Insert a BSON size smaller than the min document size by inserting 4 into the first four
    // bytes of the stream.
    char smallSize[4] = {static_cast<char>((4 >> 0) & 0xFF),
                         static_cast<char>((4 >> 8) & 0xFF),
                         static_cast<char>((4 >> 16) & 0xFF),
                         static_cast<char>((4 >> 24) & 0xFF)};

    std::stringstream stream(std::string(smallSize, 4));
    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore, BSONStreamReaderSizeTooLarge, "Parsed invalid BSON length") {
    // Insert a BSON size greater than the max document size by inserting 16777217 into the first
    // four bytes of the stream.
    char bigSize[4] = {static_cast<char>((16777217 >> 0) & 0xFF),
                       static_cast<char>((16777217 >> 8) & 0xFF),
                       static_cast<char>((16777217 >> 16) & 0xFF),
                       static_cast<char>((16777217 >> 24) & 0xFF)};

    std::stringstream stream(std::string(bigSize, 4));
    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore, BSONStreamReaderStreamFailbit, "Failed to read BSON length") {
    auto mock = MockStream();
    auto obj1 = BSON("_id" << 1);
    mock << obj1;

    auto reader = mongo::magic_restore::BSONStreamReader(mock.stream());
    // With the failbit set, `getNext()` should fassert.
    mock.setStreamState(std::ios::failbit);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore,
           BSONStreamReaderReadLessThanSizeBytes,
           "Failed to read entire BSON object") {
    // Specifying a BSON size of 6 but only including 5 bytes will fail.
    char size[5] = {static_cast<char>((6 >> 0) & 0xFF),
                    static_cast<char>((6 >> 8) & 0xFF),
                    static_cast<char>((6 >> 16) & 0xFF),
                    static_cast<char>((6 >> 24) & 0xFF),
                    static_cast<char>(1)};

    std::stringstream stream(std::string(size, 5));
    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    auto obj = reader.getNext();
}

// This test uses a binary file containing oplog entries generated by Cloud. The file is well-formed
// raw BSON data.
TEST(MagicRestore, BSONStreamReaderReadSampleCloudOplogs) {
    boost::filesystem::path filePath(boost::filesystem::current_path() / "src" / "mongo" / "db" /
                                     "modules" / "enterprise" / "src" / "magic_restore" /
                                     "bson_reader_test_data" / "sample_cloud_oplogs");

    std::ifstream stream(filePath.c_str(), std::ios::binary);
    ASSERT(stream.is_open());

    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    ASSERT(reader.hasNext());
    while (reader.hasNext()) {
        auto bsonObj = reader.getNext();
        // Ensure the parsed BSON object is an oplog entry.
        ASSERT_DOES_NOT_THROW(OplogEntry o(bsonObj));
    }
    // The Cloud file has 12 oplog entries in it.
    ASSERT_EQUALS(reader.getTotalBytesRead(), boost::filesystem::file_size(filePath));
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 12);
}

// A BSONObj with all the required fields parses correctly.
TEST(MagicRestore, RestoreConfigurationValidParsing) {
    auto restoreConfig = BSON("nodeType"
                              << "replicaSet"
                              << "replicaSetConfig"
                              << BSON("_id"
                                      << "rs0"
                                      << "version" << 1 << "term" << 1 << "members"
                                      << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                               << "localhost:12345")))
                              << "maxCheckpointTs" << Timestamp());
    ASSERT_DOES_NOT_THROW(magic_restore::RestoreConfiguration::parse(
        IDLParserContext("RestoreConfiguration"), restoreConfig));
}

// Includes all possible fields in a RestoreConfiguration.
TEST(MagicRestore, RestoreConfigurationValidParsingAllFields) {
    auto restoreConfig = BSON("nodeType"
                              << "shard"
                              << "replicaSetConfig"
                              << BSON("_id"
                                      << "rs0"
                                      << "version" << 1 << "term" << 1 << "members"
                                      << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                               << "localhost:12345")))
                              << "pointInTimeTimestamp" << Timestamp(1, 0)
                              << "restoreToHigherTermThan" << int64_t(10) << "maxCheckpointTs"
                              << Timestamp() << "collectionsToRestore"
                              << BSON_ARRAY(BSON("ns"
                                                 << "testDB"
                                                 << "uuid" << UUID::gen()))
                              << "seedForUuids" << OID() << "shardIdentityDocument"
                              << BSON("shardName"
                                      << "shard1"
                                      << "clusterId" << OID() << "configsvrConnectionString"
                                      << "conn")
                              << "shardingRename"
                              << BSON_ARRAY(BSON("sourceShardName"
                                                 << "source"
                                                 << "destinationShardName"
                                                 << "destination"
                                                 << "destinationShardConnectionString"
                                                 << "connstring"))
                              << "balancerSettings" << BSON("stopped" << false));


    ASSERT_DOES_NOT_THROW(magic_restore::RestoreConfiguration::parse(
        IDLParserContext("RestoreConfiguration"), restoreConfig));
}

// A BSONObj missing required fields will throw an error.
TEST(MagicRestore, RestoreConfigurationMissingRequiredField) {
    auto restoreConfig = BSON("nodeType"
                              << "replicaSet"
                              << "replicaSetConfig"
                              << BSON("_id"
                                      << "rs0"
                                      << "version" << 1 << "term" << 1 << "members"
                                      << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                               << "localhost:12345"))));
    ASSERT_THROWS_CODE_AND_WHAT(
        magic_restore::RestoreConfiguration::parse(IDLParserContext("RestoreConfiguration"),
                                                   restoreConfig),
        mongo::DBException,
        ErrorCodes::IDLFailedToParse,
        "BSON field 'RestoreConfiguration.maxCheckpointTs' is missing but a required field");
}

TEST(MagicRestore, RestoreConfigurationPitGreaterThanMaxTs) {
    auto restoreConfig = BSON("nodeType"
                              << "replicaSet"
                              << "replicaSetConfig"
                              << BSON("_id"
                                      << "rs0"
                                      << "version" << 1 << "term" << 1 << "members"
                                      << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                               << "localhost:12345")))
                              << "maxCheckpointTs" << Timestamp(1, 1) << "pointInTimeTimestamp"
                              << Timestamp(1, 0));
    ASSERT_THROWS_CODE_AND_WHAT(
        magic_restore::RestoreConfiguration::parse(IDLParserContext("RestoreConfiguration"),
                                                   restoreConfig),
        mongo::DBException,
        8290601,
        "The pointInTimeTimestamp must be greater than the maxCheckpointTs.");
}

TEST(MagicRestore, RestoreConfigurationZeroRestoreToTerm) {
    auto restoreConfig = BSON("nodeType"
                              << "replicaSet"
                              << "replicaSetConfig"
                              << BSON("_id"
                                      << "rs0"
                                      << "version" << 1 << "term" << 1 << "members"
                                      << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                               << "localhost:12345")))
                              << "maxCheckpointTs" << Timestamp() << "restoreToHigherTermThan"
                              << int64_t(0));

    ASSERT_THROWS_CODE_AND_WHAT(
        magic_restore::RestoreConfiguration::parse(IDLParserContext("RestoreConfiguration"),
                                                   restoreConfig),
        mongo::DBException,
        ErrorCodes::BadValue,
        "BSON field 'restoreToHigherTermThan' value must be >= 1, actual value '0'");
}

// If any of the sharding fields are set, then the node type must also be sharded.
TEST(MagicRestore, RestoreConfigurationShardingFieldsValidation) {
    std::string errmsg =
        "If the 'shardIdentityDocument', 'shardingRename', or 'balancerSettings' fields exist "
        "in the restore configuration, the node type must be either 'shard', 'configServer', "
        "or 'configShard'.";

    // Node type is `replicaSet` and `shardIdentityDocument` exists.
    auto restoreConfig = BSON("nodeType"
                              << "replicaSet"
                              << "replicaSetConfig"
                              << BSON("_id"
                                      << "rs0"
                                      << "version" << 1 << "term" << 1 << "members"
                                      << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                               << "localhost:12345")))
                              << "maxCheckpointTs" << Timestamp() << "shardIdentityDocument"
                              << BSON("shardName"
                                      << "shard1"
                                      << "clusterId" << OID() << "configsvrConnectionString"
                                      << "conn"));

    ASSERT_THROWS_CODE_AND_WHAT(magic_restore::RestoreConfiguration::parse(
                                    IDLParserContext("RestoreConfiguration"), restoreConfig),
                                mongo::DBException,
                                8290602,
                                errmsg);

    // Node type is `replicaSet` and `shardingRename` exists.
    restoreConfig = BSON("nodeType"
                         << "replicaSet"
                         << "replicaSetConfig"
                         << BSON("_id"
                                 << "rs0"
                                 << "version" << 1 << "term" << 1 << "members"
                                 << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                          << "localhost:12345")))
                         << "maxCheckpointTs" << Timestamp() << "shardingRename"
                         << BSON_ARRAY(BSON("sourceShardName"
                                            << "source"
                                            << "destinationShardName"
                                            << "destination"
                                            << "destinationShardConnectionString"
                                            << "connstring")));

    ASSERT_THROWS_CODE_AND_WHAT(magic_restore::RestoreConfiguration::parse(
                                    IDLParserContext("RestoreConfiguration"), restoreConfig),
                                mongo::DBException,
                                8290602,
                                errmsg);

    // Node type is `replicaSet` and `balancerSettings` exists.
    restoreConfig = BSON("nodeType"
                         << "replicaSet"
                         << "replicaSetConfig"
                         << BSON("_id"
                                 << "rs0"
                                 << "version" << 1 << "term" << 1 << "members"
                                 << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                          << "localhost:12345")))
                         << "maxCheckpointTs" << Timestamp() << "balancerSettings"
                         << BSON("stopped" << false));

    ASSERT_THROWS_CODE_AND_WHAT(magic_restore::RestoreConfiguration::parse(
                                    IDLParserContext("RestoreConfiguration"), restoreConfig),
                                mongo::DBException,
                                8290602,
                                errmsg);

    // Includes all three sharding fields and the node type is 'shard', so will parse correctly.
    restoreConfig = BSON("nodeType"
                         << "shard"
                         << "replicaSetConfig"
                         << BSON("_id"
                                 << "rs0"
                                 << "version" << 1 << "term" << 1 << "members"
                                 << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                          << "localhost:12345")))
                         << "maxCheckpointTs" << Timestamp() << "shardIdentityDocument"
                         << BSON("shardName"
                                 << "shard1"
                                 << "clusterId" << OID() << "configsvrConnectionString"
                                 << "conn")
                         << "shardingRename"
                         << BSON_ARRAY(BSON("sourceShardName"
                                            << "source"
                                            << "destinationShardName"
                                            << "destination"
                                            << "destinationShardConnectionString"
                                            << "connstring"))
                         << "balancerSettings" << BSON("stopped" << false));

    ASSERT_DOES_NOT_THROW(magic_restore::RestoreConfiguration::parse(
        IDLParserContext("RestoreConfiguration"), restoreConfig));
}

TEST(MagicRestore, RestoreConfigurationShardingRenameNoShardIdentity) {
    auto restoreConfig = BSON("nodeType"
                              << "shard"
                              << "replicaSetConfig"
                              << BSON("_id"
                                      << "rs0"
                                      << "version" << 1 << "term" << 1 << "members"
                                      << BSON_ARRAY(BSON("_id" << 0 << "host"
                                                               << "localhost:12345")))
                              << "maxCheckpointTs" << Timestamp() << "shardingRename"
                              << BSON_ARRAY(BSON("sourceShardName"
                                                 << "source"
                                                 << "destinationShardName"
                                                 << "destination"
                                                 << "destinationShardConnectionString"
                                                 << "connstring"))
                              << "balancerSettings" << BSON("stopped" << false));

    ASSERT_THROWS_CODE_AND_WHAT(magic_restore::RestoreConfiguration::parse(
                                    IDLParserContext("RestoreConfiguration"), restoreConfig),
                                mongo::DBException,
                                8290603,
                                "If 'shardingRename' exists in the restore configuration, "
                                "'shardIdentityDocument' must also be passed in.");
}

class MagicRestoreFixture : public ServiceContextMongoDTest {
public:
    explicit MagicRestoreFixture(Options options = {})
        : ServiceContextMongoDTest(std::move(options)) {}

    OperationContext* operationContext() {
        return _opCtx.get();
    }

    repl::StorageInterface* storageInterface() {
        return _storage.get();
    }

protected:
    void setUp() override {
        // Set up mongod.
        ServiceContextMongoDTest::setUp();

        auto service = getServiceContext();
        _opCtx = cc().makeOperationContext();
        _storage = std::make_unique<repl::StorageInterfaceImpl>();

        // Set up ReplicationCoordinator and ensure that we are primary.
        auto replCoord = std::make_unique<repl::ReplicationCoordinatorMock>(service);
        ASSERT_OK(replCoord->setFollowerMode(repl::MemberState::RS_PRIMARY));
        repl::ReplicationCoordinator::set(service, std::move(replCoord));

        // Set up oplog collection.
        repl::createOplog(operationContext());
    }

    void tearDown() override {
        _opCtx.reset();

        // Tear down mongod.
        ServiceContextMongoDTest::tearDown();
    }

private:
    ServiceContext::UniqueOperationContext _opCtx;
    std::unique_ptr<repl::StorageInterface> _storage;
};

TEST_F(MagicRestoreFixture, TruncateLocalDbCollections) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    auto namespaces = std::array{NamespaceString::kSystemReplSetNamespace,
                                 NamespaceString::kDefaultOplogTruncateAfterPointNamespace,
                                 NamespaceString::kDefaultMinValidNamespace,
                                 NamespaceString::kLastVoteNamespace,
                                 NamespaceString::kDefaultInitialSyncIdNamespace};

    for (const auto& nss : namespaces) {
        ASSERT_OK(storage->createCollection(opCtx, nss, CollectionOptions{}));
        auto res = storage->putSingleton(opCtx, nss, {BSON("test" << 1)});
        ASSERT_OK(res);
    }

    magic_restore::truncateLocalDbCollections(opCtx, storage);

    for (const auto& nss : namespaces) {
        auto res = storage->findSingleton(opCtx, nss);
        ASSERT_EQUALS(ErrorCodes::CollectionIsEmpty, res.getStatus());
    }
}

TEST_F(MagicRestoreFixture, SetInvalidMinValid) {
    auto storage = storageInterface();
    auto opCtx = operationContext();
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kDefaultMinValidNamespace, CollectionOptions{}));

    magic_restore::setInvalidMinValid(opCtx, storage);

    auto res = storage->findSingleton(opCtx, NamespaceString::kDefaultMinValidNamespace);
    ASSERT_OK(res.getStatus());
    ASSERT_BSONOBJ_EQ(BSON("_id" << OID() << "t" << -1 << "ts" << Timestamp(0, 1)), res.getValue());
}

}  // namespace repl
}  // namespace mongo
