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
#include "mongo/util/time_support.h"
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

static std::vector<BSONObj> getDocuments(OperationContext* opCtx,
                                         repl::StorageInterface* storage,
                                         const NamespaceString& nss) {
    // Scan the whole collection.
    auto res = storage->findDocuments(opCtx,
                                      nss,
                                      {} /* indexName */,
                                      repl::StorageInterface::ScanDirection::kForward,
                                      {} /* startKey */,
                                      BoundInclusion::kIncludeStartKeyOnly,
                                      -1 /* limit */);
    ASSERT_OK(res.getStatus());
    auto docs = res.getValue();

    std::sort(docs.begin(), docs.end(), [](const BSONObj& lhs, const BSONObj& rhs) {
        return lhs.getStringField("_id") < rhs.getStringField("_id");
    });
    return docs;
}

TEST_F(MagicRestoreFixture, UpdateShardingMetadataConfigShard) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kServerConfigurationNamespace, CollectionOptions{}));

    ASSERT_OK(storage->insertDocuments(opCtx,
                                       NamespaceString::kServerConfigurationNamespace,
                                       {InsertStatement{BSON("_id"
                                                             << "authSchema"
                                                             << "currentVersion" << 5)},
                                        InsertStatement{BSON("_id"
                                                             << "shardIdentity")}}));

    magic_restore::RestoreConfiguration restoreConfig;
    restoreConfig.setNodeType(mongo::magic_restore::NodeTypeEnum::kConfigShard);

    updateShardingMetadata(opCtx, restoreConfig, storage);

    auto docs = getDocuments(opCtx, storage, NamespaceString::kServerConfigurationNamespace);
    ASSERT_EQ(1, docs.size());
    ASSERT_EQ(docs[0].getStringField("_id"), "authSchema");
}

TEST_F(MagicRestoreFixture, UpdateShardNameMetadataConfigShard) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    std::string srcShard0 = "srcShard0";
    std::string dstShard0 = "dstShard0";
    std::string srcConnStr0 = "SourceConnectionString0";
    std::string dstConnStr0 = "DestinationConnectionString0";

    std::string srcShard1 = "srcShard1";
    std::string dstShard1 = "dstShard1";
    std::string srcConnStr1 = "SourceConnectionString1";
    std::string dstConnStr1 = "DestinationConnectionString1";

    std::string srcShard2 = "srcShard2";

    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigDatabasesNamespace, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigsvrShardsNamespace, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigsvrChunksNamespace, CollectionOptions{}));

    // Multiple documents with primary srcShard0 and srcShard1, see mapping below.
    ASSERT_OK(storage->insertDocuments(opCtx,
                                       NamespaceString::kConfigDatabasesNamespace,
                                       {
                                           InsertStatement{BSON("_id"
                                                                << "0"
                                                                << "primary" << srcShard0)},
                                           InsertStatement{BSON("_id"
                                                                << "1"
                                                                << "primary" << srcShard1)},
                                           InsertStatement{BSON("_id"
                                                                << "2"
                                                                << "primary" << srcShard1)},
                                           InsertStatement{BSON("_id"
                                                                << "3"
                                                                << "primary" << srcShard2)},
                                           InsertStatement{BSON("_id"
                                                                << "4"
                                                                << "primary" << srcShard0)},
                                       }));

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kConfigsvrShardsNamespace,
        {
            InsertStatement{BSON("_id" << srcShard0 << "hosts" << srcConnStr0)},
            InsertStatement{BSON("_id" << srcShard1 << "hosts" << srcConnStr1)},
            InsertStatement{BSON("_id" << srcShard2 << "hosts" << srcConnStr0)},
        }));

    BSONArray history0 = BSON_ARRAY(BSON("validAfter" << Timestamp(2) << "shard" << srcShard2)
                                    << BSON("validAfter" << Timestamp(3) << "shard" << srcShard0));
    BSONArray history1 = BSON_ARRAY(BSON("validAfter" << Timestamp(3) << "shard" << srcShard1));
    BSONArray history2 = BSON_ARRAY(BSON("validAfter" << Timestamp(2) << "shard" << srcShard0)
                                    << BSON("validAfter" << Timestamp(3) << "shard" << srcShard2));
    BSONArray history3 = BSON_ARRAY(BSON("validAfter" << Timestamp(2) << "shard" << srcShard2)
                                    << BSON("validAfter" << Timestamp(3) << "shard" << srcShard0));
    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kConfigsvrChunksNamespace,
        {
            InsertStatement{BSON("_id"
                                 << "0"
                                 << "shard" << srcShard0 << "history" << history0
                                 << "onCurrentShardSince" << Timestamp(3))},
            InsertStatement{BSON("_id"
                                 << "1"
                                 << "shard" << srcShard1 << "history" << history1
                                 << "onCurrentShardSince" << Timestamp(3))},
            InsertStatement{BSON("_id"
                                 << "2"
                                 << "shard" << srcShard2 << "history" << history2
                                 << "onCurrentShardSince" << Timestamp(3))},
            InsertStatement{BSON("_id"
                                 << "3"
                                 << "shard" << srcShard0 << "history" << history3
                                 << "onCurrentShardSince" << Timestamp(3))},
        }));

    magic_restore::RestoreConfiguration restoreConfig;
    restoreConfig.setNodeType(magic_restore::NodeTypeEnum::kConfigShard);

    std::vector<magic_restore::ShardRenameMapping> mapping{{srcShard0, dstShard0, dstConnStr0},
                                                           {srcShard1, dstShard1, dstConnStr1}};
    restoreConfig.setShardingRename(mapping);

    updateShardNameMetadata(opCtx, restoreConfig, storage);

    auto docs = getDocuments(opCtx, storage, NamespaceString::kConfigDatabasesNamespace);
    ASSERT_EQ(5, docs.size());

    ASSERT_EQ(docs[0].getStringField("primary"), dstShard0);
    ASSERT_EQ(docs[1].getStringField("primary"), dstShard1);
    ASSERT_EQ(docs[2].getStringField("primary"), dstShard1);
    ASSERT_EQ(docs[3].getStringField("primary"), srcShard2);
    ASSERT_EQ(docs[4].getStringField("primary"), dstShard0);

    docs = getDocuments(opCtx, storage, NamespaceString::kConfigsvrShardsNamespace);
    ASSERT_EQ(3, docs.size());

    ASSERT_EQ(docs[0].getStringField("_id"), dstShard0);
    ASSERT_EQ(docs[1].getStringField("_id"), dstShard1);
    ASSERT_EQ(docs[2].getStringField("_id"), srcShard2);

    ASSERT_EQ(docs[0].getStringField("hosts"), dstConnStr0);
    ASSERT_EQ(docs[1].getStringField("hosts"), dstConnStr1);
    ASSERT_EQ(docs[2].getStringField("hosts"), srcConnStr0);

    docs = getDocuments(opCtx, storage, NamespaceString::kConfigsvrChunksNamespace);
    ASSERT_EQ(4, docs.size());

    BSONArray dstHistory0 =
        BSON_ARRAY(BSON("validAfter" << Timestamp(0, 1) << "shard" << dstShard0));
    BSONArray dstHistory1 =
        BSON_ARRAY(BSON("validAfter" << Timestamp(0, 1) << "shard" << dstShard1));

    ASSERT_EQ(docs[0].getStringField("shard"), dstShard0);
    ASSERT_BSONOBJ_EQ(docs[0].getObjectField("history"), dstHistory0);
    ASSERT_EQ(docs[0].getField("onCurrentShardSince").timestamp(), Timestamp(0, 1));

    ASSERT_EQ(docs[1].getStringField("shard"), dstShard1);
    ASSERT_BSONOBJ_EQ(docs[1].getObjectField("history"), dstHistory1);
    ASSERT_EQ(docs[1].getField("onCurrentShardSince").timestamp(), Timestamp(0, 1));

    ASSERT_EQ(docs[2].getStringField("shard"), srcShard2);
    ASSERT_BSONOBJ_EQ(docs[2].getObjectField("history"), history2);
    ASSERT_EQ(docs[2].getField("onCurrentShardSince").timestamp(), Timestamp(0, 3));

    ASSERT_EQ(docs[3].getStringField("shard"), dstShard0);
    ASSERT_BSONOBJ_EQ(docs[3].getObjectField("history"), dstHistory0);
    ASSERT_EQ(docs[3].getField("onCurrentShardSince").timestamp(), Timestamp(0, 1));
}

void checkNoOpOplogEntry(std::vector<BSONObj>& docs,
                         Timestamp expectedTs,
                         long long expectedTerm,
                         Date_t expectedDate) {
    // These assertions must always be true for the no-op oplog entry.
    ASSERT_EQ(docs.size(), 1);
    ASSERT_EQ(docs[0].getStringField("op"), "n");
    ASSERT_EQ(docs[0].getObjectField("o").getStringField("msg"), "restore incrementing term");

    ASSERT_EQ(docs[0].getField("ts").timestamp(), expectedTs);
    ASSERT_EQ(docs[0].getIntField("t"), expectedTerm);
    ASSERT_EQ(docs[0].getField("wall").Date(), expectedDate);
}

TEST_F(MagicRestoreFixture, insertHigherTermNoOpOplogEntryHighTerm) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    auto now = Date_t::now();
    auto highTerm = 100;
    auto termInEntry = 1;
    // Since the 'highTerm' parameter is greater than the entry's term, we'll use 'highTerm' in the
    // no-op.
    BSONObj lastOplog = BSON("ts" << Timestamp(10, 1) << "t" << termInEntry << "wall" << now);

    ASSERT_DOES_NOT_THROW(magic_restore::insertHigherTermNoOpOplogEntry(
        opCtx, storage, lastOplog, int64_t(highTerm)));
    auto docs = getDocuments(opCtx, storage, NamespaceString::kRsOplogNamespace);
    checkNoOpOplogEntry(docs, Timestamp(11, 1), highTerm + 100, now + Seconds(1));
}

TEST_F(MagicRestoreFixture, insertHigherTermNoOpOplogEntryLastEntryHasHigherTerm) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    auto now = Date_t::now();
    auto highTerm = 10;
    auto termInEntry = 11;
    // Since the 'termInEntry' value in the last oplog entry is greater than the 'highTerm'
    // parameter, we'll use 'termInEntry' in the no-op.
    BSONObj lastOplog = BSON("ts" << Timestamp(10, 1) << "t" << termInEntry << "wall" << now);

    ASSERT_DOES_NOT_THROW(magic_restore::insertHigherTermNoOpOplogEntry(
        opCtx, storage, lastOplog, int64_t(highTerm)));
    auto docs = getDocuments(opCtx, storage, NamespaceString::kRsOplogNamespace);
    checkNoOpOplogEntry(docs, Timestamp(11, 1), termInEntry + 100, now + Seconds(1));
}

TEST_F(MagicRestoreFixture, insertHigherTermNoOpOplogEntryNoTerm) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    auto now = Date_t::now();
    auto highTerm = 10;
    // There is no 't' field in the oplog entry. This only happens in an oplog entry that signals a
    // replica set initiation. In this case, the 'highTerm' should always be used.
    BSONObj lastOplog = BSON("ts" << Timestamp(10, 1) << "wall" << now);

    ASSERT_DOES_NOT_THROW(magic_restore::insertHigherTermNoOpOplogEntry(
        opCtx, storage, lastOplog, int64_t(highTerm)));
    auto docs = getDocuments(opCtx, storage, NamespaceString::kRsOplogNamespace);
    checkNoOpOplogEntry(docs, Timestamp(11, 1), highTerm + 100, now + Seconds(1));
}


}  // namespace repl
}  // namespace mongo
