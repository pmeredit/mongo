/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "magic_restore/magic_restore.h"
#include "magic_restore/magic_restore_structs_gen.h"
#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/auth/authorization_backend_interface.h"
#include "mongo/db/auth/authorization_backend_mock.h"
#include "mongo/db/auth/authorization_client_handle_shard.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_factory_mock.h"
#include "mongo/db/auth/authorization_router_impl.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/storage_interface_impl.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/db/service_entry_point_shard_role.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/time_support.h"
#include <fstream>
#include <iostream>
#include <sstream>

namespace mongo::magic_restore {
namespace {

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

    auto reader = BSONStreamReader(mock.stream());
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

    auto reader = BSONStreamReader(mock.stream());

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
    auto reader = BSONStreamReader(mock.stream());

    // Empty BSON objects should parse correctly.
    auto readBson = reader.getNext();
    ASSERT_BSONOBJ_EQ(obj, readBson);

    ASSERT_EQUALS(reader.getTotalBytesRead(), obj.objsize());
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 1);
    // Empty BSON objects cannot be parsed into a valid oplog entry.
    ASSERT_THROWS_CODE(repl::OplogEntry(readBson), DBException, ErrorCodes::IDLFailedToParse);
}

TEST(MagicRestore, BSONStreamReaderSizeLarge) {
    // Insert a BSON greater than the max external document size.
    // The max internal size (BSONObjMaxInternalSize) is higher so we expect no failure.
    auto mock = MockStream();
    auto obj = BSON("_id" << std::string(BSONObjMaxUserSize + 1, 'x'));
    mock << obj;

    auto reader = BSONStreamReader(mock.stream());
    ASSERT(reader.hasNext());
    auto bsonObj = reader.getNext();
    ASSERT_BSONOBJ_EQ(obj, bsonObj);
    ASSERT_GT(obj.objsize(), BSONObjMaxUserSize + 1);

    ASSERT_EQUALS(reader.getTotalBytesRead(), obj.objsize());
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 1);
    mock.setStreamState(std::ios::eofbit);
    ASSERT(!reader.hasNext());
}

DEATH_TEST(MagicRestore, BSONStreamReaderNegativeSize, "Parsed invalid BSON length") {
    // Insert a negative BSON size by inserting -11 into the first four bytes of the stream.
    char negativeSize[4] = {static_cast<char>((-11 >> 0) & 0xFF),
                            static_cast<char>((-11 >> 8) & 0xFF),
                            static_cast<char>((-11 >> 16) & 0xFF),
                            static_cast<char>((-11 >> 24) & 0xFF)};

    std::stringstream stream(std::string(negativeSize, 4));
    auto reader = BSONStreamReader(stream);
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
    auto reader = BSONStreamReader(stream);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore, BSONStreamReaderSizeTooLarge, "Parsed invalid BSON length") {
    // Insert a BSON size greater than the max internal document size by inserting into the first
    // four bytes of the stream.
    const int size = BSONObjMaxInternalSize + 1;
    char bigSize[4] = {static_cast<char>((size >> 0) & 0xFF),
                       static_cast<char>((size >> 8) & 0xFF),
                       static_cast<char>((size >> 16) & 0xFF),
                       static_cast<char>((size >> 24) & 0xFF)};

    std::stringstream stream(std::string(bigSize, 4));
    auto reader = BSONStreamReader(stream);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore, BSONStreamReaderStreamFailbit, "Failed to read BSON length") {
    auto mock = MockStream();
    auto obj1 = BSON("_id" << 1);
    mock << obj1;

    auto reader = BSONStreamReader(mock.stream());
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
    auto reader = BSONStreamReader(stream);
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

    auto reader = BSONStreamReader(stream);
    ASSERT(reader.hasNext());
    while (reader.hasNext()) {
        auto bsonObj = reader.getNext();
        // Ensure the parsed BSON object is an oplog entry.
        ASSERT_DOES_NOT_THROW(repl::OplogEntry o(bsonObj));
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
        DBException,
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
        DBException,
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
        DBException,
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
                                DBException,
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
                                DBException,
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
                                DBException,
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
                                DBException,
                                8290603,
                                "If 'shardingRename' exists in the restore configuration, "
                                "'shardIdentityDocument' must also be passed in.");
}

class MagicRestoreFixture : public ServiceContextMongoDTest {
public:
    explicit MagicRestoreFixture(Options options = {})
        : ServiceContextMongoDTest(std::move(options)) {
        AuthorizationManager::get(getService())->setAuthEnabled(true);
    }

    OperationContext* operationContext() {
        return _opCtx.get();
    }

    repl::StorageInterface* storageInterface() {
        return _storage.get();
    }

protected:
    void setUp() override {
        storageGlobalParams.magicRestore = true;
        // Set up mongod.
        ServiceContextMongoDTest::setUp();

        auto serviceContext = getServiceContext();
        _opCtx = cc().makeOperationContext();
        _storage = std::make_unique<repl::StorageInterfaceImpl>();

        // Set up ReplicationCoordinator and ensure that we are primary.
        auto replCoord = std::make_unique<repl::ReplicationCoordinatorMock>(serviceContext);
        ASSERT_OK(replCoord->setFollowerMode(repl::MemberState::RS_PRIMARY));
        repl::ReplicationCoordinator::set(serviceContext, std::move(replCoord));

        // Set up oplog collection.
        repl::createOplog(operationContext());
    }

    void tearDown() override {
        _opCtx.reset();
        _storage.reset();
        // Tear down mongod.
        ServiceContextMongoDTest::tearDown();
        storageGlobalParams.magicRestore = false;
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

// Test createCollectionsToRestore.
TEST_F(MagicRestoreFixture, CreateCollectionsToRestore) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    auto dbName = DatabaseName::createDatabaseName_forTest(boost::none /* tenantId*/, "testDB");
    std::string collName0 = "testColl0";
    std::string collName1 = "testColl1";
    auto ns0 = NamespaceString::createNamespaceString_forTest(dbName, collName0);
    auto ns1 = NamespaceString::createNamespaceString_forTest(dbName, collName1);
    auto uuid0 = UUID::gen();
    auto uuid1 = UUID::gen();

    std::vector<NamespaceUUIDPair> collectionsToRestore{NamespaceUUIDPair{ns0, uuid0},
                                                        NamespaceUUIDPair{ns1, uuid1}};

    ASSERT_EQUALS(ErrorCodes::NamespaceNotFound,
                  storage->getCollectionUUID(opCtx, NamespaceString::kConfigsvrRestoreNamespace));

    magic_restore::createCollectionsToRestore(opCtx, collectionsToRestore, storage);

    auto docs = getDocuments(opCtx, storage, NamespaceString::kConfigsvrRestoreNamespace);
    ASSERT_EQ(2, docs.size());

    auto expectedNs0 = NamespaceStringUtil::serialize(ns0, SerializationContext::stateDefault());

    ASSERT_EQ(docs[0].getStringField("ns"),
              NamespaceStringUtil::serialize(ns0, SerializationContext::stateDefault()));
    ASSERT_BSONELT_EQ(docs[0].getField("uuid"), uuid0.toBSON().firstElement());
    ASSERT_EQ(docs[1].getStringField("ns"),
              NamespaceStringUtil::serialize(ns1, SerializationContext::stateDefault()));
    ASSERT_BSONELT_EQ(docs[1].getField("uuid"), uuid1.toBSON().firstElement());

    ASSERT_OK(storage->dropCollection(opCtx, NamespaceString::kConfigsvrRestoreNamespace));
}

// Test of updateShardingMetadata for magic_restore::NodeTypeEnum::kConfigShard.
TEST_F(MagicRestoreFixture, UpdateShardingMetadataConfigShard) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    NamespaceString chunkTest = NamespaceString::createNamespaceString_forTest(
        DatabaseName::kConfig, "cache.chunks.test"_sd);

    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kServerConfigurationNamespace, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigMongosNamespace, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(opCtx, chunkTest, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigsvrPlacementHistoryNamespace, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigReshardingOperationsNamespace, CollectionOptions{}));

    BSONObj expectedShardIdentity =
        BSON("_id"
             << "shardIdentity"
             << "shardName"
             << "config"
             << "clusterId" << OID("65dd1017a44e231a15af5fc0") << "configsvrConnectionString"
             << "m103-csrs/localhost:26001,localhost:26002,localhost:26003");
    BSONObj previousShardIdentity = expectedShardIdentity.addField(BSON("additionalField"
                                                                        << "someValue")
                                                                       .firstElement());

    ASSERT_OK(storage->insertDocuments(opCtx,
                                       NamespaceString::kServerConfigurationNamespace,
                                       {InsertStatement{BSON("_id"
                                                             << "authSchema"
                                                             << "currentVersion" << 5)},
                                        InsertStatement{previousShardIdentity}}));

    ASSERT_OK(storage->insertDocuments(opCtx,
                                       NamespaceString::kConfigsvrPlacementHistoryNamespace,
                                       {InsertStatement{BSON("_id" << 0)}}));

    // We allow one resharding operation per collection, so the two operations below have different
    // namespaces.
    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kConfigReshardingOperationsNamespace,
        {InsertStatement{BSON("_id" << 0 << "ns"
                                    << "testDB.testColl0"
                                    << "state"
                                    << "cloning"
                                    << "donorShards"
                                    << BSON_ARRAY(BSON("id"
                                                       << "backupShard0")
                                                  << BSON("id"
                                                          << "backupShard2"))
                                    << "recipientShards"
                                    << BSON_ARRAY(BSON("id"
                                                       << "backupShard2")))},
         InsertStatement{BSON("_id" << 1 << "ns"
                                    << "testDB.testColl1"
                                    << "state"
                                    << "committing"
                                    << "donorShards"
                                    << BSON_ARRAY(BSON("id"
                                                       << "backupShard2"))
                                    << "recipientShards"
                                    << BSON_ARRAY(BSON("id"
                                                       << "backupShard2")
                                                  << BSON("id"
                                                          << "backupShard1")))}}));

    magic_restore::RestoreConfiguration restoreConfig;
    restoreConfig.setNodeType(NodeTypeEnum::kConfigShard);

    updateShardingMetadata(opCtx, restoreConfig, storage);

    auto docs = getDocuments(opCtx, storage, NamespaceString::kServerConfigurationNamespace);
    ASSERT_EQ(2, docs.size());
    ASSERT_EQ(docs[0].getStringField("_id"), "authSchema");
    ASSERT_BSONOBJ_EQ_UNORDERED(docs[1], expectedShardIdentity);

    ASSERT_EQUALS(ErrorCodes::NamespaceNotFound, storage->getCollectionUUID(opCtx, chunkTest));

    ASSERT_EQUALS(ErrorCodes::NamespaceNotFound,
                  storage->getCollectionUUID(opCtx, NamespaceString::kConfigMongosNamespace));

    auto res = storage->findDocuments(opCtx,
                                      NamespaceString::kConfigsvrPlacementHistoryNamespace,
                                      {} /* indexName */,
                                      repl::StorageInterface::ScanDirection::kForward,
                                      {} /* startKey */,
                                      BoundInclusion::kIncludeStartKeyOnly,
                                      -1 /* limit */);
    ASSERT_EQUALS(ErrorCodes::NamespaceNotFound, res.getStatus());

    docs = getDocuments(opCtx, storage, NamespaceString::kConfigReshardingOperationsNamespace);
    ASSERT_EQ(2, docs.size());
    ASSERT_EQ(docs[0].getStringField("ns"), "testDB.testColl0");
    ASSERT_EQ(docs[0].getStringField("state"), "aborting");
    ASSERT_BSONOBJ_EQ(docs[0].getObjectField("abortReason"),
                      BSON("code" << ErrorCodes::ReshardCollectionAborted << "errmsg"
                                  << "aborted by automated restore"));
    ASSERT_EQ(docs[1].getStringField("ns"), "testDB.testColl1");
    ASSERT_EQ(docs[1].getStringField("state"), "committing");
}

// Test of updateShardingMetadata for magic_restore::NodeTypeEnum::kShard.
TEST_F(MagicRestoreFixture, UpdateShardingMetadataShard) {
    std::string backupShard = "backupShard0";
    std::string restoreShard = "restoreShard0";
    std::string restoreConnStr = "DestinationConnectionString";

    magic_restore::RestoreConfiguration restoreConfig;
    restoreConfig.setNodeType(NodeTypeEnum::kShard);
    std::vector<magic_restore::ShardRenameMapping> mapping{
        {backupShard, restoreShard, restoreConnStr}};
    restoreConfig.setShardingRename(mapping);

    std::string shardIdentityName = "test";
    OID clusterId("11dd1017a44e231a15af5fc0");
    ConnectionString configsvrConnectionString(
        ConnectionString::ConnectionType::kReplicaSet, "ConfigsvrConnectionString", "repl");
    ShardIdentity shardIdentity(shardIdentityName, clusterId, configsvrConnectionString);
    restoreConfig.setShardIdentityDocument(shardIdentity);

    auto storage = storageInterface();
    auto opCtx = operationContext();

    NamespaceString chunkTest = NamespaceString::createNamespaceString_forTest(
        DatabaseName::kConfig, "cache.chunks.test"_sd);

    // This test is setting setShardingRename and getShardIdentityDocument so a lot of those
    // collections need to exist even if we don't check them (as updateShardingMetadata calls
    // updateShardNameMetadata).
    for (const auto& nss : {NamespaceString::kShardConfigCollectionsNamespace,
                            NamespaceString::kShardConfigDatabasesNamespace,
                            NamespaceString::kServerConfigurationNamespace,
                            NamespaceString::kTransactionCoordinatorsNamespace,
                            NamespaceString::kMigrationCoordinatorsNamespace,
                            NamespaceString::kRangeDeletionNamespace,
                            NamespaceString::kDonorReshardingOperationsNamespace,
                            NamespaceString::kRecipientReshardingOperationsNamespace,
                            NamespaceString::kShardingDDLCoordinatorsNamespace,
                            chunkTest}) {
        ASSERT_OK(storage->createCollection(opCtx, nss, CollectionOptions{}));
    }

    BSONObj previousShardIdentity =
        BSON("_id"
             << "shardIdentity"
             << "shardName" << backupShard << "clusterId" << OID("65dd1017a44e231a15af5fc0")
             << "configsvrConnectionString"
             << "m103-csrs/localhost:26001,localhost:26002,localhost:26003");
    BSONObj expectedShardIdentity = BSON("_id"
                                         << "shardIdentity"
                                         << "shardName" << shardIdentityName << "clusterId"
                                         << clusterId << "configsvrConnectionString"
                                         << configsvrConnectionString.toString());

    ASSERT_OK(storage->insertDocuments(opCtx,
                                       NamespaceString::kServerConfigurationNamespace,
                                       {InsertStatement{BSON("_id"
                                                             << "authSchema"
                                                             << "currentVersion" << 5)},
                                        InsertStatement{previousShardIdentity}}));

    updateShardingMetadata(opCtx, restoreConfig, storage);

    ASSERT_EQUALS(
        ErrorCodes::NamespaceNotFound,
        storage->getCollectionUUID(opCtx, NamespaceString::kShardConfigCollectionsNamespace));

    ASSERT_EQUALS(ErrorCodes::NamespaceNotFound, storage->getCollectionUUID(opCtx, chunkTest));

    ASSERT_EQUALS(
        ErrorCodes::NamespaceNotFound,
        storage->getCollectionUUID(opCtx, NamespaceString::kShardConfigDatabasesNamespace));

    auto docs = getDocuments(opCtx, storage, NamespaceString::kServerConfigurationNamespace);
    ASSERT_EQ(2, docs.size());
    ASSERT_EQ(docs[0].getStringField("_id"), "authSchema");
    ASSERT_BSONOBJ_EQ_UNORDERED(docs[1], expectedShardIdentity);
}

DEATH_TEST_REGEX_F(MagicRestoreFixture,
                   SelectiveRestoreStepsMissingParam,
                   "8948400.*Performing a selective restore with magic restore requires passing in "
                   "the --restore parameter") {
    auto storage = storageInterface();
    auto opCtx = operationContext();
    magic_restore::RestoreConfiguration restoreConfig;

    // Without the --restore parameter, this call will crash the node.
    runSelectiveRestoreSteps(opCtx, restoreConfig, storage);
}

TEST_F(MagicRestoreFixture, SelectiveRestoreSteps) {
    auto storage = storageInterface();
    auto opCtx = operationContext();
    magic_restore::RestoreConfiguration restoreConfig;


    auto dbName = DatabaseName::createDatabaseName_forTest(boost::none /* tenantId*/, "db");
    std::string collName = "collToRestore";
    auto ns = NamespaceString::createNamespaceString_forTest(dbName, collName);
    auto uuid = UUID::gen();

    std::vector<NamespaceUUIDPair> collectionsToRestore{NamespaceUUIDPair{ns, uuid}};
    restoreConfig.setCollectionsToRestore(collectionsToRestore);

    ASSERT_EQUALS(ErrorCodes::NamespaceNotFound,
                  storage->getCollectionUUID(opCtx, NamespaceString::kConfigsvrRestoreNamespace));

    storageGlobalParams.restore = true;
    runSelectiveRestoreSteps(opCtx, restoreConfig, storage);
    // Ensure the 'collections_to_restore' collection was successfully dropped after
    // 'runSelectiveRestoreSteps'.
    ASSERT_EQUALS(ErrorCodes::NamespaceNotFound,
                  storage->getCollectionUUID(opCtx, NamespaceString::kConfigsvrRestoreNamespace));
}

// Test dropNonRestoredClusterParameters.
TEST_F(MagicRestoreFixture, DropNonRestoredClusterParameters) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kClusterParametersNamespace, CollectionOptions{}));

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kClusterParametersNamespace,
        {InsertStatement{BSON("_id"
                              << "shardedClusterCardinalityForDirectConns"
                              << "foobar" << 5)},
         InsertStatement{BSON("_id"
                              << "querySettings"
                              << "testParam"
                              << "17")},
         InsertStatement{BSON("_id"
                              << "randomName"
                              << "testParam" << 3)},
         InsertStatement{BSON("_id"
                              << "pauseMigrationsDuringMultiUpdates"
                              << "testParam"
                              << "17")},
         InsertStatement{BSON("_id"
                              << "defaultMaxTimeMS"
                              << "testParam"
                              << "17")},
         InsertStatement{BSON("_id"
                              << "addOrRemoveShardInProgress"
                              << "testParam"
                              << "17")},
         InsertStatement{BSON("_id"
                              << "configServerReadPreferenceForCatalogQueries"
                              << "testParam"
                              << "17")}}));

    auto docs = getDocuments(opCtx, storage, NamespaceString::kClusterParametersNamespace);
    ASSERT_EQ(7, docs.size());

    magic_restore::dropNonRestoredClusterParameters(opCtx, storage);

    // randomName and addOrRemoveShardInProgress parameters have been dropped.
    docs = getDocuments(opCtx, storage, NamespaceString::kClusterParametersNamespace);
    ASSERT_EQ(5, docs.size());
}

// Test of updateShardNameMetadata for magic_restore::NodeTypeEnum::kDedicatedConfigServer.
TEST_F(MagicRestoreFixture, UpdateShardNameMetadataDedicatedConfigServer) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    std::string backupShard0 = "backupShard0";
    std::string restoreShard0 = "restoreShard0";
    std::string backupConnStr0 = "SourceConnectionString0";
    std::string restoreConnStr0 = "DestinationConnectionString0";

    std::string backupShard1 = "backupShard1";
    std::string restoreShard1 = "restoreShard1";
    std::string srcConnStr1 = "SourceConnectionString1";
    std::string restoreConnStr1 = "DestinationConnectionString1";

    std::string backupShard2 = "backupShard2";

    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigDatabasesNamespace, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigsvrShardsNamespace, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigsvrChunksNamespace, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kTransactionCoordinatorsNamespace, CollectionOptions{}));
    ASSERT_OK(storage->createCollection(
        opCtx, NamespaceString::kConfigReshardingOperationsNamespace, CollectionOptions{}));

    // Multiple documents with primary backupShard0 and backupShard1, see mapping below.
    ASSERT_OK(storage->insertDocuments(opCtx,
                                       NamespaceString::kConfigDatabasesNamespace,
                                       {
                                           InsertStatement{BSON("_id"
                                                                << "0"
                                                                << "primary" << backupShard0)},
                                           InsertStatement{BSON("_id"
                                                                << "1"
                                                                << "primary" << backupShard1)},
                                           InsertStatement{BSON("_id"
                                                                << "2"
                                                                << "primary" << backupShard1)},
                                           InsertStatement{BSON("_id"
                                                                << "3"
                                                                << "primary" << backupShard2)},
                                           InsertStatement{BSON("_id"
                                                                << "4"
                                                                << "primary" << backupShard0)},
                                       }));

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kConfigsvrShardsNamespace,
        {
            InsertStatement{BSON("_id" << backupShard0 << "host" << backupConnStr0)},
            InsertStatement{BSON("_id" << backupShard1 << "host" << srcConnStr1)},
            InsertStatement{BSON("_id" << backupShard2 << "host" << backupConnStr0)},
        }));

    BSONArray history0 =
        BSON_ARRAY(BSON("validAfter" << Timestamp(2) << "shard" << backupShard2)
                   << BSON("validAfter" << Timestamp(3) << "shard" << backupShard0));
    BSONArray history1 = BSON_ARRAY(BSON("validAfter" << Timestamp(3) << "shard" << backupShard1));
    BSONArray history2 =
        BSON_ARRAY(BSON("validAfter" << Timestamp(2) << "shard" << backupShard0)
                   << BSON("validAfter" << Timestamp(3) << "shard" << backupShard2));
    BSONArray history3 =
        BSON_ARRAY(BSON("validAfter" << Timestamp(2) << "shard" << backupShard2)
                   << BSON("validAfter" << Timestamp(3) << "shard" << backupShard0));
    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kConfigsvrChunksNamespace,
        {
            InsertStatement{BSON("_id"
                                 << "0"
                                 << "shard" << backupShard0 << "history" << history0
                                 << "onCurrentShardSince" << Timestamp(3))},
            InsertStatement{BSON("_id"
                                 << "1"
                                 << "shard" << backupShard1 << "history" << history1
                                 << "onCurrentShardSince" << Timestamp(3))},
            InsertStatement{BSON("_id"
                                 << "2"
                                 << "shard" << backupShard2 << "history" << history2
                                 << "onCurrentShardSince" << Timestamp(3))},
            InsertStatement{BSON("_id"
                                 << "3"
                                 << "shard" << backupShard0 << "history" << history3
                                 << "onCurrentShardSince" << Timestamp(3))},
        }));

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kTransactionCoordinatorsNamespace,
        {InsertStatement{
             BSON("_id" << 0 << "participants" << BSON_ARRAY(backupShard0 << backupShard2))},
         InsertStatement{
             BSON("_id" << 1 << "participants" << BSON_ARRAY(backupShard2 << backupShard1))}}));

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kConfigReshardingOperationsNamespace,
        {InsertStatement{
             BSON("_id" << 0 << "state"
                        << "cloning"
                        << "donorShards"
                        << BSON_ARRAY(BSON("id" << backupShard0) << BSON("id" << backupShard2))
                        << "recipientShards" << BSON_ARRAY(BSON("id" << backupShard2)))},
         InsertStatement{BSON(
             "_id" << 1 << "state"
                   << "committing"
                   << "donorShards" << BSON_ARRAY(BSON("id" << backupShard2)) << "recipientShards"
                   << BSON_ARRAY(BSON("id" << backupShard2) << BSON("id" << backupShard1)))}}));

    magic_restore::RestoreConfiguration restoreConfig;
    restoreConfig.setNodeType(magic_restore::NodeTypeEnum::kDedicatedConfigServer);

    // Only changing the mapping of shard 0 and shard 1.
    std::vector<magic_restore::ShardRenameMapping> mapping{
        {backupShard0, restoreShard0, restoreConnStr0},
        {backupShard1, restoreShard1, restoreConnStr1}};
    restoreConfig.setShardingRename(mapping);

    updateShardNameMetadata(opCtx, restoreConfig, storage);

    auto docs = getDocuments(opCtx, storage, NamespaceString::kConfigDatabasesNamespace);
    ASSERT_EQ(5, docs.size());

    ASSERT_EQ(docs[0].getStringField("primary"), restoreShard0);
    ASSERT_EQ(docs[1].getStringField("primary"), restoreShard1);
    ASSERT_EQ(docs[2].getStringField("primary"), restoreShard1);
    ASSERT_EQ(docs[3].getStringField("primary"), backupShard2);
    ASSERT_EQ(docs[4].getStringField("primary"), restoreShard0);

    docs = getDocuments(opCtx, storage, NamespaceString::kConfigsvrShardsNamespace);
    ASSERT_EQ(3, docs.size());

    ASSERT_EQ(docs[0].getStringField("_id"), backupShard2);
    ASSERT_EQ(docs[1].getStringField("_id"), restoreShard0);
    ASSERT_EQ(docs[2].getStringField("_id"), restoreShard1);

    ASSERT_EQ(docs[0].getStringField("host"), backupConnStr0);
    ASSERT_EQ(docs[1].getStringField("host"), restoreConnStr0);
    ASSERT_EQ(docs[2].getStringField("host"), restoreConnStr1);

    docs = getDocuments(opCtx, storage, NamespaceString::kConfigsvrChunksNamespace);
    ASSERT_EQ(4, docs.size());

    BSONArray dstHistory0 =
        BSON_ARRAY(BSON("validAfter" << Timestamp(0, 1) << "shard" << restoreShard0));
    BSONArray dstHistory1 =
        BSON_ARRAY(BSON("validAfter" << Timestamp(0, 1) << "shard" << restoreShard1));

    ASSERT_EQ(docs[0].getStringField("shard"), restoreShard0);
    ASSERT_BSONOBJ_EQ(docs[0].getObjectField("history"), dstHistory0);
    ASSERT_EQ(docs[0].getField("onCurrentShardSince").timestamp(), Timestamp(0, 1));

    ASSERT_EQ(docs[1].getStringField("shard"), restoreShard1);
    ASSERT_BSONOBJ_EQ(docs[1].getObjectField("history"), dstHistory1);
    ASSERT_EQ(docs[1].getField("onCurrentShardSince").timestamp(), Timestamp(0, 1));

    ASSERT_EQ(docs[2].getStringField("shard"), backupShard2);
    ASSERT_BSONOBJ_EQ(docs[2].getObjectField("history"), history2);
    ASSERT_EQ(docs[2].getField("onCurrentShardSince").timestamp(), Timestamp(0, 3));

    ASSERT_EQ(docs[3].getStringField("shard"), restoreShard0);
    ASSERT_BSONOBJ_EQ(docs[3].getObjectField("history"), dstHistory0);
    ASSERT_EQ(docs[3].getField("onCurrentShardSince").timestamp(), Timestamp(0, 1));

    docs = getDocuments(opCtx, storage, NamespaceString::kTransactionCoordinatorsNamespace);
    ASSERT_EQ(2, docs.size());
    ASSERT_BSONOBJ_EQ(docs[0].getObjectField("participants"),
                      BSON_ARRAY(restoreShard0 << backupShard2));
    ASSERT_BSONOBJ_EQ(docs[1].getObjectField("participants"),
                      BSON_ARRAY(backupShard2 << restoreShard1));

    docs = getDocuments(opCtx, storage, NamespaceString::kConfigReshardingOperationsNamespace);
    ASSERT_EQ(2, docs.size());
    ASSERT_BSONOBJ_EQ(docs[0].getObjectField("donorShards"),
                      BSON_ARRAY(BSON("id" << restoreShard0) << BSON("id" << backupShard2)));
    ASSERT_BSONOBJ_EQ(docs[0].getObjectField("recipientShards"),
                      BSON_ARRAY(BSON("id" << backupShard2)));

    ASSERT_BSONOBJ_EQ(docs[1].getObjectField("donorShards"),
                      BSON_ARRAY(BSON("id" << backupShard2)));
    ASSERT_BSONOBJ_EQ(docs[1].getObjectField("recipientShards"),
                      BSON_ARRAY(BSON("id" << backupShard2) << BSON("id" << restoreShard1)));
}

// Test of updateShardNameMetadata for magic_restore::NodeTypeEnum::kShard.
TEST_F(MagicRestoreFixture, UpdateShardNameMetadataShard) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    std::string backupShard0 = "backupShard0";
    std::string restoreShard0 = "restoreShard0";
    std::string backupConnStr0 = "SourceConnectionString0";
    std::string restoreConnStr0 = "DestinationConnectionString0";

    std::string backupShard1 = "backupShard1";
    std::string restoreShard1 = "restoreShard1";
    std::string srcConnStr1 = "SourceConnectionString1";
    std::string restoreConnStr1 = "DestinationConnectionString1";

    std::string backupShard2 = "backupShard2";

    for (const auto& nss : {NamespaceString::kTransactionCoordinatorsNamespace,
                            NamespaceString::kMigrationCoordinatorsNamespace,
                            NamespaceString::kRangeDeletionNamespace,
                            NamespaceString::kDonorReshardingOperationsNamespace,
                            NamespaceString::kRecipientReshardingOperationsNamespace,
                            NamespaceString::kShardingDDLCoordinatorsNamespace}) {
        ASSERT_OK(storage->createCollection(opCtx, nss, CollectionOptions{}));
    }

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kTransactionCoordinatorsNamespace,
        {InsertStatement{
             BSON("_id" << 0 << "participants" << BSON_ARRAY(backupShard0 << backupShard2))},
         InsertStatement{
             BSON("_id" << 1 << "participants" << BSON_ARRAY(backupShard2 << backupShard1))}}));

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kMigrationCoordinatorsNamespace,
        {InsertStatement{BSON("_id" << 0 << "donorShardId" << backupShard0 << "recipientShardId"
                                    << backupShard1)},
         InsertStatement{BSON("_id" << 1 << "donorShardId" << backupShard1 << "recipientShardId"
                                    << backupShard2)},
         InsertStatement{BSON("_id" << 2 << "donorShardId" << backupShard2 << "recipientShardId"
                                    << backupShard0)}}));

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kRangeDeletionNamespace,
        {InsertStatement{BSON("_id" << 0 << "donorShardId" << backupShard0)},
         InsertStatement{BSON("_id" << 1 << "donorShardId" << backupShard1)},
         InsertStatement{BSON("_id" << 2 << "donorShardId" << backupShard2)}}));

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kDonorReshardingOperationsNamespace,
        {InsertStatement{
             BSON("_id" << 0 << "recipientShards" << BSON_ARRAY(backupShard0 << backupShard2))},
         InsertStatement{
             BSON("_id" << 1 << "recipientShards" << BSON_ARRAY(backupShard2 << backupShard1))}}));

    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kRecipientReshardingOperationsNamespace,
        {InsertStatement{BSON("_id" << 0 << "donorShards"
                                    << BSON_ARRAY(BSON("shardId" << backupShard0)
                                                  << BSON("shardId" << backupShard2)))},
         InsertStatement{BSON("_id" << 1 << "donorShards"
                                    << BSON_ARRAY(BSON("shardId" << backupShard2)
                                                  << BSON("shardId" << backupShard1)))}}));

    // Code handles createCollection_V4 and movePrimary only so otherCommand should not be affected.
    ASSERT_OK(storage->insertDocuments(
        opCtx,
        NamespaceString::kShardingDDLCoordinatorsNamespace,
        {InsertStatement{BSON(
             "_id" << BSON("namespace"
                           << "test"  // To avoid duplicated _id with the other createCollection_V4.
                           << "operationType"
                           << "createCollection_V4")
                   << "shardIds" << BSON_ARRAY(backupShard0 << backupShard1 << backupShard2))},
         InsertStatement{BSON("_id" << BSON("operationType"
                                            << "createCollection_V4")
                                    << "originalDataShard" << backupShard1)},
         InsertStatement{BSON("_id" << BSON("operationType"
                                            << "otherCommand")
                                    << "shardIds" << BSON_ARRAY(backupShard0 << backupShard2))},
         InsertStatement{BSON("_id" << BSON("operationType"
                                            << "movePrimary")
                                    << "toShardId" << backupShard1)}}));

    magic_restore::RestoreConfiguration restoreConfig;
    restoreConfig.setNodeType(NodeTypeEnum::kShard);
    std::vector<magic_restore::ShardRenameMapping> mapping{
        {backupShard0, restoreShard0, restoreConnStr0},
        {backupShard1, restoreShard1, restoreConnStr1}};
    restoreConfig.setShardingRename(mapping);

    updateShardNameMetadata(opCtx, restoreConfig, storage);

    auto docs = getDocuments(opCtx, storage, NamespaceString::kTransactionCoordinatorsNamespace);
    ASSERT_EQ(2, docs.size());
    ASSERT_BSONOBJ_EQ(docs[0].getObjectField("participants"),
                      BSON_ARRAY(restoreShard0 << backupShard2));
    ASSERT_BSONOBJ_EQ(docs[1].getObjectField("participants"),
                      BSON_ARRAY(backupShard2 << restoreShard1));

    docs = getDocuments(opCtx, storage, NamespaceString::kMigrationCoordinatorsNamespace);
    ASSERT_EQ(3, docs.size());
    ASSERT_EQ(docs[0].getStringField("donorShardId"), restoreShard0);
    ASSERT_EQ(docs[1].getStringField("donorShardId"), restoreShard1);
    ASSERT_EQ(docs[2].getStringField("donorShardId"), backupShard2);
    ASSERT_EQ(docs[0].getStringField("recipientShardId"), restoreShard1);
    ASSERT_EQ(docs[1].getStringField("recipientShardId"), backupShard2);
    ASSERT_EQ(docs[2].getStringField("recipientShardId"), restoreShard0);

    docs = getDocuments(opCtx, storage, NamespaceString::kRangeDeletionNamespace);
    ASSERT_EQ(3, docs.size());
    ASSERT_EQ(docs[0].getStringField("donorShardId"), restoreShard0);
    ASSERT_EQ(docs[1].getStringField("donorShardId"), restoreShard1);
    ASSERT_EQ(docs[2].getStringField("donorShardId"), backupShard2);

    docs = getDocuments(opCtx, storage, NamespaceString::kDonorReshardingOperationsNamespace);
    ASSERT_EQ(2, docs.size());
    ASSERT_BSONOBJ_EQ(docs[0].getObjectField("recipientShards"),
                      BSON_ARRAY(restoreShard0 << backupShard2));
    ASSERT_BSONOBJ_EQ(docs[1].getObjectField("recipientShards"),
                      BSON_ARRAY(backupShard2 << restoreShard1));

    docs = getDocuments(opCtx, storage, NamespaceString::kRecipientReshardingOperationsNamespace);
    ASSERT_EQ(2, docs.size());
    ASSERT_BSONOBJ_EQ(
        docs[0].getObjectField("donorShards"),
        BSON_ARRAY(BSON("shardId" << restoreShard0) << BSON("shardId" << backupShard2)));
    ASSERT_BSONOBJ_EQ(
        docs[1].getObjectField("donorShards"),
        BSON_ARRAY(BSON("shardId" << backupShard2) << BSON("shardId" << restoreShard1)));

    docs = getDocuments(opCtx, storage, NamespaceString::kShardingDDLCoordinatorsNamespace);
    ASSERT_EQ(4, docs.size());
    ASSERT_BSONOBJ_EQ(docs[0].getObjectField("shardIds"),
                      BSON_ARRAY(restoreShard0 << restoreShard1 << backupShard2));
    ASSERT_EQ(docs[1].getStringField("originalDataShard"), restoreShard1);
    ASSERT_BSONOBJ_EQ(docs[2].getObjectField("shardIds"), BSON_ARRAY(backupShard0 << backupShard2));
    ASSERT_EQ(docs[3].getStringField("toShardId"), restoreShard1);
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
    // Since the 'highTerm' parameter is greater than the entry's term, we'll use 'highTerm' in
    // the no-op.
    BSONObj lastOplog = BSON("ts" << Timestamp(10, 1) << "t" << termInEntry << "wall" << now);

    const auto noopEntryTs =
        magic_restore::insertHigherTermNoOpOplogEntry(opCtx, storage, lastOplog, int64_t(highTerm));
    ASSERT_EQ(noopEntryTs, Timestamp(11, 1));
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

    const auto noopEntryTs =
        magic_restore::insertHigherTermNoOpOplogEntry(opCtx, storage, lastOplog, int64_t(highTerm));
    ASSERT_EQ(noopEntryTs, Timestamp(11, 1));
    auto docs = getDocuments(opCtx, storage, NamespaceString::kRsOplogNamespace);
    checkNoOpOplogEntry(docs, Timestamp(11, 1), termInEntry + 100, now + Seconds(1));
}

TEST_F(MagicRestoreFixture, insertHigherTermNoOpOplogEntryNoTerm) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    auto now = Date_t::now();
    auto highTerm = 10;
    // There is no 't' field in the oplog entry. This only happens in an oplog entry that
    // signals a replica set initiation. In this case, the 'highTerm' should always be used.
    BSONObj lastOplog = BSON("ts" << Timestamp(10, 1) << "wall" << now);

    const auto noopEntryTs =
        magic_restore::insertHigherTermNoOpOplogEntry(opCtx, storage, lastOplog, int64_t(highTerm));
    ASSERT_EQ(noopEntryTs, Timestamp(11, 1));
    auto docs = getDocuments(opCtx, storage, NamespaceString::kRsOplogNamespace);
    checkNoOpOplogEntry(docs, Timestamp(11, 1), highTerm + 100, now + Seconds(1));
}

// Tests that the 'upsertAutomationCredentials' correctly parses createRole
// and createUser commands and inserts them into the auth collections.
TEST_F(MagicRestoreFixture, CreateNewAutomationCredentials) {
    auto opCtx = operationContext();
    auto storage = storageInterface();

    createInternalCollectionsWithUuid(
        opCtx,
        storage,
        std::vector<NamespaceUUIDPair>{
            NamespaceUUIDPair{NamespaceString::kAdminRolesNamespace, UUID::gen()},
            NamespaceUUIDPair{NamespaceString::kAdminUsersNamespace, UUID::gen()}});

    auto testRole = BSON("createRole"
                         << "testRole"
                         << "$db"
                         << "admin"
                         << "privileges" << BSONArray() << "roles" << BSONArray());

    auto testUser = BSON("createUser"
                         << "testUser"
                         << "$db"
                         << "admin"
                         << "pwd"
                         << "password"
                         << "roles" << BSONArray());

    auto autoCreds = magic_restore::AutomationCredentials::parse(
        IDLParserContext("AutomationCredentials"),
        BSON("createRoleCommands" << BSON_ARRAY(testRole) << "createUserCommands"
                                  << BSON_ARRAY(testUser)));

    magic_restore::upsertAutomationCredentials(opCtx, autoCreds, storage);

    auto docs = getDocuments(opCtx, storage, NamespaceString::kAdminRolesNamespace);
    ASSERT_EQ(1, docs.size());
    ASSERT_BSONOBJ_EQ(BSON("_id"
                           << "admin.testRole"
                           << "role"
                           << "testRole"
                           << "db"
                           << "admin"
                           << "privileges" << BSONArray() << "roles" << BSONArray()),
                      docs[0]);

    docs = getDocuments(opCtx, storage, NamespaceString::kAdminUsersNamespace);
    ASSERT_EQ(1, docs.size());
    // The user document contains randomly generated fields such as UUIDs and SCRAM-SHA-1
    // credentials, so we can only assert on certain fields.
    ASSERT_EQ(docs[0].getField("_id").String(), "admin.testUser");
    ASSERT_EQ(docs[0].getField("user").String(), "testUser");
    ASSERT_EQ(docs[0].getField("db").String(), "admin");
    ASSERT(docs[0].getField("roles").Array().empty());
}

// Tests that the 'upsertAutomationCredentials' will convert create operations into updates if the
// specified roles or users already exist.
TEST_F(MagicRestoreFixture, UpdateAutomationCredentials) {
    auto storage = storageInterface();

    auto mockBackend = reinterpret_cast<auth::AuthorizationBackendMock*>(
        auth::AuthorizationBackendInterface::get(getService()));

    // Re-initialize the client after setting the AuthorizationManager to get an
    // AuthorizationSession.
    auto oldClient = Client::releaseCurrent();
    Client::initThread(getThreadName(), getServiceContext()->getService());
    {
        auto op = cc().makeOperationContext();
        auto opCtx = op.get();
        opCtx->getClient()->setIsInternalClient(true);

        createInternalCollectionsWithUuid(
            opCtx,
            storage,
            std::vector<NamespaceUUIDPair>{
                NamespaceUUIDPair{NamespaceString::kAdminRolesNamespace, UUID::gen()},
                NamespaceUUIDPair{NamespaceString::kAdminUsersNamespace, UUID::gen()}});

        auto testRole = BSON("createRole"
                             << "testRole"
                             << "$db"
                             << "admin"
                             << "privileges" << BSONArray() << "roles" << BSONArray());

        auto testUser = BSON("createUser"
                             << "testUser"
                             << "$db"
                             << "admin"
                             << "pwd"
                             << "password"
                             << "roles" << BSONArray());

        auto autoCreds = magic_restore::AutomationCredentials::parse(
            IDLParserContext("AutomationCredentials"),
            BSON("createRoleCommands" << BSON_ARRAY(testRole) << "createUserCommands"
                                      << BSON_ARRAY(testUser)));

        magic_restore::upsertAutomationCredentials(opCtx, autoCreds, storage);

        // Insert the role and user docs into the mock authorization manager state. This will
        // ensure the update code path can succeed.
        auto roleDoc = BSON("_id"
                            << "admin.testRole"
                            << "role"
                            << "testRole"
                            << "db"
                            << "admin"
                            << "privileges" << BSONArray() << "roles" << BSONArray());
        auto userDoc = BSON("_id"
                            << "admin.testUser"
                            << "user"
                            << "testUser"
                            << "db"
                            << "admin");
        ASSERT_OK(mockBackend->insert(opCtx, NamespaceString::kAdminRolesNamespace, roleDoc, {}));
        ASSERT_OK(mockBackend->insert(opCtx, NamespaceString::kAdminUsersNamespace, userDoc, {}));

        auto docs = getDocuments(opCtx, storage, NamespaceString::kAdminRolesNamespace);
        ASSERT_EQ(1, docs.size());
        docs = getDocuments(opCtx, storage, NamespaceString::kAdminUsersNamespace);
        ASSERT_EQ(1, docs.size());

        // Now that the role and user already exist, the 'createRole' and 'createUser' should be
        // converted into an 'updateRole' and 'updateUser' commands with the same arguments.
        auto updatedRole = BSON("createRole"
                                << "testRole"
                                << "$db"
                                << "admin"
                                << "privileges" << BSONArray() << "roles"
                                << BSON_ARRAY("readWriteAnyDatabase"));

        auto updatedUser = BSON("createUser"
                                << "testUser"
                                << "$db"
                                << "admin"
                                << "pwd"
                                << "password"
                                << "roles" << BSON_ARRAY("readWriteAnyDatabase"));

        autoCreds = magic_restore::AutomationCredentials::parse(
            IDLParserContext("AutomationCredentials"),
            BSON("createRoleCommands" << BSON_ARRAY(updatedRole) << "createUserCommands"
                                      << BSON_ARRAY(updatedUser)));

        magic_restore::upsertAutomationCredentials(opCtx, autoCreds, storage);
        docs = getDocuments(opCtx, storage, NamespaceString::kAdminRolesNamespace);
        ASSERT_EQ(1, docs.size());
        ASSERT_EQ(docs[0].getField("_id").String(), "admin.testRole");
        ASSERT_EQ(docs[0].getField("role").String(), "testRole");
        ASSERT_EQ(docs[0].getField("db").String(), "admin");
        ASSERT_EQ(docs[0].getField("roles").Array().size(), 1);
        ASSERT_BSONOBJ_EQ(docs[0].getField("roles").Array().begin()->Obj(),
                          BSON("role"
                               << "readWriteAnyDatabase"
                               << "db"
                               << "admin"));

        docs = getDocuments(opCtx, storage, NamespaceString::kAdminUsersNamespace);
        ASSERT_EQ(1, docs.size());
        ASSERT_EQ(docs[0].getField("_id").String(), "admin.testUser");
        ASSERT_EQ(docs[0].getField("user").String(), "testUser");
        ASSERT_EQ(docs[0].getField("db").String(), "admin");
        ASSERT_EQ(docs[0].getField("roles").Array().size(), 1);
        ASSERT_BSONOBJ_EQ(docs[0].getField("roles").Array().begin()->Obj(),
                          BSON("role"
                               << "readWriteAnyDatabase"
                               << "db"
                               << "admin"));
    }
    // Restore the original client.
    Client::releaseCurrent();
    Client::setCurrent(std::move(oldClient));
}


TEST_F(MagicRestoreFixture, CreateInternalCollectionsWithUuid) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    auto ns0 = NamespaceString::createNamespaceString_forTest("config", "settings");
    auto ns1 = NamespaceString::createNamespaceString_forTest("admin", "system.roles");
    auto ns2 = NamespaceString::createNamespaceString_forTest("admin", "system.users");
    auto uuid0 = UUID::gen();
    auto uuid1 = UUID::gen();
    auto uuid2 = UUID::gen();

    std::vector<NamespaceUUIDPair> colls{NamespaceUUIDPair{ns0, uuid0},
                                         NamespaceUUIDPair{ns1, uuid1},
                                         NamespaceUUIDPair{ns2, uuid2}};

    for (const auto& nsAndUuid : colls) {
        ASSERT_EQUALS(ErrorCodes::NamespaceNotFound,
                      storage->findSingleton(opCtx, nsAndUuid.getNs()).getStatus());
    }

    magic_restore::createInternalCollectionsWithUuid(opCtx, storage, colls);

    for (const auto& nsAndUuid : colls) {
        const auto& ns = nsAndUuid.getNs();
        const auto& uuid = nsAndUuid.getUuid();
        AutoGetCollectionForRead autoColl(opCtx, ns);
        ASSERT_TRUE(autoColl.getCollection());
        ASSERT_EQ(autoColl->getCollectionOptions().uuid, uuid);
    }
}

DEATH_TEST_REGEX_F(MagicRestoreFixture,
                   CreateNewAutomationCredentialsMissingInternalCollections,
                   "8816900.*Internal replicated collection does not exist on node") {
    auto storage = storageInterface();
    auto opCtx = operationContext();
    auto testRole = BSON("createRole"
                         << "testRole"
                         << "$db"
                         << "admin"
                         << "privileges" << BSONArray() << "roles" << BSONArray());

    auto testUser = BSON("createUser"
                         << "testUser"
                         << "$db"
                         << "admin"
                         << "pwd"
                         << "password"
                         << "roles" << BSONArray());

    auto autoCreds = magic_restore::AutomationCredentials::parse(
        IDLParserContext("AutomationCredentials"),
        BSON("createRoleCommands" << BSON_ARRAY(testRole) << "createUserCommands"
                                  << BSON_ARRAY(testUser)));

    magic_restore::upsertAutomationCredentials(opCtx, autoCreds, storage);
}

TEST_F(MagicRestoreFixture, CreateInternalCollectionsWithUuidCollectionsAlreadyExist) {
    auto storage = storageInterface();
    auto opCtx = operationContext();

    ASSERT_OK(storageInterface()->createCollection(
        opCtx, NamespaceString::kConfigSettingsNamespace, CollectionOptions{}));
    ASSERT_OK(storageInterface()->createCollection(
        opCtx, NamespaceString::kAdminRolesNamespace, CollectionOptions{}));
    ASSERT_OK(storageInterface()->createCollection(
        opCtx, NamespaceString::kAdminUsersNamespace, CollectionOptions{}));

    auto ns0 = NamespaceString::createNamespaceString_forTest("config", "settings");
    auto ns1 = NamespaceString::createNamespaceString_forTest("admin", "system.roles");
    auto ns2 = NamespaceString::createNamespaceString_forTest("admin", "system.users");
    auto uuid0 = UUID::gen();
    auto uuid1 = UUID::gen();
    auto uuid2 = UUID::gen();

    std::vector<NamespaceUUIDPair> colls{NamespaceUUIDPair{ns0, uuid0},
                                         NamespaceUUIDPair{ns1, uuid1},
                                         NamespaceUUIDPair{ns2, uuid2}};

    for (const auto& nsAndUuid : colls) {
        ASSERT_EQUALS(ErrorCodes::CollectionIsEmpty,
                      storage->findSingleton(opCtx, nsAndUuid.getNs()).getStatus());
    }

    // Though the collections already exist, this function should not throw.
    magic_restore::createInternalCollectionsWithUuid(opCtx, storage, colls);

    for (const auto& nsAndUuid : colls) {
        const auto& ns = nsAndUuid.getNs();
        const auto& uuid = nsAndUuid.getUuid();
        AutoGetCollectionForRead autoColl(opCtx, ns);
        ASSERT_TRUE(autoColl.getCollection());
        // Since the collections were created prior to the call to
        // 'createInternalCollectionsWithUuid', their UUID values should be different than the ones
        // specified in the NamespaceUUIDPair list.
        ASSERT_NE(autoColl->getCollectionOptions().uuid, uuid);
    }
}

TEST_F(MagicRestoreFixture, CheckInternalCollectionExists) {
    auto opCtx = operationContext();
    ASSERT_OK(storageInterface()->createCollection(
        opCtx, NamespaceString::kConfigSettingsNamespace, CollectionOptions{}));

    // Collection exists, so this should return successfully.
    magic_restore::checkInternalCollectionExists(opCtx, NamespaceString::kConfigSettingsNamespace);
}

DEATH_TEST_REGEX_F(MagicRestoreFixture,
                   CheckInternalCollectionExistsFails,
                   "8816900.*Internal replicated collection does not exist on node") {
    auto opCtx = operationContext();
    // The collection does not exist, so this should fatally assert.
    magic_restore::checkInternalCollectionExists(opCtx, NamespaceString::kConfigSettingsNamespace);
}

// Tests setting the 'stopped' field in the balancer settings.
TEST_F(MagicRestoreFixture, SetBalancerSettings) {
    auto opCtx = operationContext();
    auto storage = storageInterface();
    // Test setting the 'stopped' field from false to true and vice versa.
    for (const auto stopped : std::vector{true, false}) {
        ASSERT_OK(storage->createCollection(
            opCtx, NamespaceString::kConfigSettingsNamespace, CollectionOptions{}));
        ASSERT_OK(storage->insertDocuments(opCtx,
                                           NamespaceString::kConfigSettingsNamespace,
                                           {InsertStatement{BSON("_id"
                                                                 << "balancer"
                                                                 << "stopped" << !stopped)}}));

        magic_restore::setBalancerSettingsStopped(opCtx, storage, stopped);
        auto res = storage->findSingleton(opCtx, NamespaceString::kConfigSettingsNamespace);
        ASSERT_OK(res.getStatus());
        auto balancerSettings = res.getValue();
        ASSERT_EQ(balancerSettings.getStringField("_id"), "balancer");
        ASSERT_EQ(balancerSettings.getBoolField("stopped"), stopped);
        ASSERT_OK(storage->dropCollection(opCtx, NamespaceString::kConfigSettingsNamespace));
    }
}

// Test collection info retrieval when multiple collections match the regex.
TEST_F(MagicRestoreFixture, LocalReshardingMetadataBothCollectionsMatchRegex) {
    auto opCtx = operationContext();
    auto storage = storageInterface();
    const auto dbName = "config";

    const std::vector<std::string> expectedColls = {"localReshardingConflictStash.00.shardId",
                                                    "localReshardingOplogBuffer.01.shardId"};

    for (const auto& collName : expectedColls) {
        ASSERT_OK(storage->createCollection(
            opCtx,
            NamespaceString::createNamespaceString_forTest(dbName, collName),
            CollectionOptions{}));
    }

    auto colls = magic_restore::getLocalReshardingMetadataColls(opCtx);
    // listCollections output is unsorted.
    std::sort(colls.begin(), colls.end());
    ASSERT_EQ(colls.size(), expectedColls.size());
    for (size_t i = 0; i < expectedColls.size(); ++i) {
        ASSERT_EQ(colls[i], expectedColls[i]);
    }
}

// Test collection info retrieval when only some collection names match the regex.
TEST_F(MagicRestoreFixture, LocalReshardingMetadataOnlyOneCollectionMatchesRegex) {
    auto opCtx = operationContext();
    auto storage = storageInterface();
    const auto dbName = "config";

    const std::vector<std::string> expectedColls = {"localReshardingConflictStash.00.shardId",
                                                    // coll1 is missing a letter in the prefix.
                                                    "locaReshardingOplogBuffer.01.shardId"};

    for (const auto& collName : expectedColls) {
        ASSERT_OK(storage->createCollection(
            opCtx,
            NamespaceString::createNamespaceString_forTest(dbName, collName),
            CollectionOptions{}));
    }

    auto colls = magic_restore::getLocalReshardingMetadataColls(opCtx);
    ASSERT_EQ(colls.size(), 1);
    ASSERT_EQ(colls.front(), expectedColls[0]);
}

// Test that collection info retrieval returns an empty list when no collection names match the
// regex.
TEST_F(MagicRestoreFixture, LocalReshardingMetadataNoCollectionsMatchRegex) {
    auto opCtx = operationContext();
    auto storage = storageInterface();
    const auto dbName = "config";

    const std::vector<std::string> expectedColls = {"localReshardingFoo.00.shardId",
                                                    "localReshardingBar.01.shardId"};

    for (const auto& collName : expectedColls) {
        ASSERT_OK(storage->createCollection(
            opCtx,
            NamespaceString::createNamespaceString_forTest(dbName, collName),
            CollectionOptions{}));
    }

    auto colls = magic_restore::getLocalReshardingMetadataColls(opCtx);
    ASSERT(colls.empty());
}

// Test collection info retrieval when multiple collections match the same prefix in the regex.
TEST_F(MagicRestoreFixture, LocalReshardingMetadataCollectionsSamePrefix) {
    auto opCtx = operationContext();
    auto storage = storageInterface();
    const auto dbName = "config";

    const std::vector<std::string> expectedColls = {"localReshardingConflictStash.00.shardId",
                                                    "localReshardingConflictStash.01.shardId",
                                                    "localReshardingOplogBuffer.00.shardId",
                                                    "localReshardingOplogBuffer.01.shardId"};

    for (const auto& collName : expectedColls) {
        ASSERT_OK(storage->createCollection(
            opCtx,
            NamespaceString::createNamespaceString_forTest(dbName, collName),
            CollectionOptions{}));
    }

    auto colls = magic_restore::getLocalReshardingMetadataColls(opCtx);
    // listCollections output is unsorted.
    std::sort(colls.begin(), colls.end());
    ASSERT_EQ(colls.size(), expectedColls.size());
    for (size_t i = 0; i < expectedColls.size(); ++i) {
        ASSERT_EQ(colls[i], expectedColls[i]);
    }
}

// Test renaming local resharding metadata collections.
TEST_F(MagicRestoreFixture, LocalReshardingMetadataRenameSuccess) {
    auto opCtx = operationContext();
    auto storage = storageInterface();
    const auto dbName = "config";

    const std::vector<std::string> collsWithOldShardId = {
        "localReshardingConflictStash.00.oldShardId", "localReshardingOplogBuffer.01.oldShardId"};

    for (const auto& collName : collsWithOldShardId) {
        ASSERT_OK(storage->createCollection(
            opCtx,
            NamespaceString::createNamespaceString_forTest(dbName, collName),
            CollectionOptions{}));
    }
    renameLocalReshardingMetadataCollections(
        opCtx, storage, collsWithOldShardId, ShardId{"oldShardId"}, ShardId{"newShardId"});

    auto colls = magic_restore::getLocalReshardingMetadataColls(opCtx);
    std::sort(colls.begin(), colls.end());

    const std::vector<std::string> expectedColls = {"localReshardingConflictStash.00.newShardId",
                                                    "localReshardingOplogBuffer.01.newShardId"};
    ASSERT_EQ(colls.size(), expectedColls.size());
    for (size_t i = 0; i < expectedColls.size(); ++i) {
        ASSERT_EQ(colls[i], expectedColls[i]);
    }
}

// Test passing an empty list into renameLocalReshardingMetadataCollections succeeds without
// renaming any collections.
TEST_F(MagicRestoreFixture, LocalReshardingMetadataNoCollsInlist) {
    auto opCtx = operationContext();
    auto storage = storageInterface();
    const auto dbName = "config";

    const std::vector<std::string> expectedColls = {"collection0", "collection1"};
    for (const auto& collName : expectedColls) {
        ASSERT_OK(storage->createCollection(
            opCtx,
            NamespaceString::createNamespaceString_forTest(dbName, collName),
            CollectionOptions{}));
    }

    renameLocalReshardingMetadataCollections(opCtx,
                                             storage,
                                             std::vector<std::string>{} /* empty list */,
                                             ShardId{"oldShardId"},
                                             ShardId{"newShardId"});

    DBDirectClient client(opCtx);
    const auto collInfo = client.getCollectionInfos(DatabaseName::kConfig);
    std::vector<std::string> colls;
    for (const auto& coll : collInfo) {
        colls.push_back(coll.getStringField("name").toString());
    }
    // listCollections output is unsorted.
    std::sort(colls.begin(), colls.end());

    ASSERT_EQ(colls.size(), expectedColls.size());
    for (size_t i = 0; i < expectedColls.size(); ++i) {
        ASSERT_EQ(colls[i], expectedColls[i]);
    }
}

// Test that passing any non-resharding collection name that doesn't match the regex into
// renameLocalReshardingMetadataCollections will fassert.
DEATH_TEST_REGEX_F(MagicRestoreFixture,
                   LocalReshardingMetadataCollListIsntReshardingMetadata,
                   "8742900") {
    auto opCtx = operationContext();
    auto storage = storageInterface();

    const std::vector<std::string> colls = {"notReshardingCollection"};
    renameLocalReshardingMetadataCollections(
        opCtx, storage, colls, ShardId{"oldShardId"}, ShardId{"newShardId"});
}

}  // namespace
}  // namespace mongo::magic_restore
