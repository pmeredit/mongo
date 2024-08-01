/**
 * Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "document_source_backup_cursor.h"

#include <memory>

#include "backup_cursor_service.h"
#include "mock_mongo_interface_for_backup_tests.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/process_interface/stub_mongo_process_interface.h"
#include "mongo/db/pipeline/variables.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/db/storage/devnull/devnull_kv_engine.h"
#include "mongo/db/storage/storage_engine_impl.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/unittest.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo {
namespace {

// TODO SERVER-83935 move these helpers into some pipeline testing library.
auto serializeToBson(const auto& stage, const SerializationOptions& opts = {}) {
    std::vector<Value> array;
    stage->serializeToArray(array, opts);
    return array[0].getDocument().toBson();
}

auto debugShape(const auto& stage) {
    return serializeToBson(stage, SerializationOptions::kDebugQueryShapeSerializeOptions);
}

auto representativeQueryShape(const auto& stage) {
    return serializeToBson(stage, SerializationOptions::kRepresentativeQueryShapeSerializeOptions);
}

class DocumentSourceBackupCursorTest : public ServiceContextMongoDTest {
public:
    DocumentSourceBackupCursorTest()
        : ServiceContextMongoDTest(Options{}.engine("devnull")),
          _opCtx(cc().makeOperationContext()) {
        repl::ReplicationCoordinator::set(
            getServiceContext(),
            std::make_unique<repl::ReplicationCoordinatorMock>(getServiceContext()));
        repl::StorageInterface::set(getServiceContext(),
                                    std::make_unique<repl::StorageInterfaceMock>());
        BackupCursorHooks::initialize(getServiceContext());
    }

protected:
    ServiceContext::UniqueOperationContext _opCtx;
};

// Tests that the files retrieved in $backupCursor through DocumentSourceBackupCursor
// are correctly tracked and checked in BackupCursorService.
TEST_F(DocumentSourceBackupCursorTest, TestFilenameCheck) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);

    // Set up the $backupCursor stage.
    const auto spec = fromjson("{$backupCursor: {}}");
    auto backupCursorStage =
        DocumentSourceBackupCursor::createFromBson(spec.firstElement(), expCtx);

    // Call `getNext()` once, to retrieve the metadata document.
    backupCursorStage->getNext();

    // Get 'testFile.txt'.
    backupCursorStage->getNext();

    auto svcCtx = _opCtx->getClient()->getServiceContext();
    auto backupCursorService = static_cast<BackupCursorService*>(BackupCursorService::get(svcCtx));

    auto backupId = backupCursorService->getBackupId_forTest();

    std::string backupFilePath = storageGlobalParams.dbpath + "/testFile.txt";
    std::string invalidBackupFilePath = storageGlobalParams.dbpath + "/notTestFile.txt";

    ASSERT(backupCursorService->isFileReturnedByCursor(backupId, backupFilePath));
    ASSERT_FALSE(backupCursorService->isFileReturnedByCursor(UUID::gen(), backupFilePath));
    ASSERT_FALSE(backupCursorService->isFileReturnedByCursor(backupId, invalidBackupFilePath));
}

// Tests that the serialization behaves as we expect. Mostly this is important for testing the query
// shape of $backupCursor.
TEST_F(DocumentSourceBackupCursorTest, TestDefaultSerialization) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);

    const auto spec = fromjson("{$backupCursor: {}}");
    BSONObj representativeShape;
    {
        auto backupCursorStage =
            DocumentSourceBackupCursor::createFromBson(spec.firstElement(), expCtx);
        ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
            R"({
                "$backupCursor": {
                    "incrementalBackup": false,
                    "blockSize": 16,
                    "disableIncrementalBackup": false
                }
            })",
            serializeToBson(backupCursorStage));
        ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
            R"({
                "$backupCursor": {
                    "incrementalBackup": false,
                    "blockSize": "?number",
                    "disableIncrementalBackup": false
                }
            })",
            debugShape(backupCursorStage));

        representativeShape = representativeQueryShape(backupCursorStage);
        ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
            R"({
                "$backupCursor": {
                    "incrementalBackup": false,
                    "blockSize": 1,
                    "disableIncrementalBackup": false
                }
            })",
            representativeShape);
    }

    auto reParsed =
        DocumentSourceBackupCursor::createFromBson(representativeShape.firstElement(), expCtx);

    // Test that the representative query is a fix point - should always get here.
    ASSERT_BSONOBJ_EQ(representativeShape, representativeQueryShape(reParsed));
}

// Test with all options specified
TEST_F(DocumentSourceBackupCursorTest, TestComplexSerialization) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    const auto spec = fromjson(R"({
            $backupCursor: {
                incrementalBackup: true,
                thisBackupName: "obviously this is a test",
                srcBackupName: "test",
                blockSize: 32,
                disableIncrementalBackup: false
            }
        })");
    BSONObj representativeShape;
    {
        auto backupCursorStage =
            DocumentSourceBackupCursor::createFromBson(spec.firstElement(), expCtx);
        ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
            R"({
                "$backupCursor": {
                    "incrementalBackup": true,
                    "thisBackupName": "obviously this is a test",
                    "srcBackupName": "test",
                    "blockSize": 32,
                    "disableIncrementalBackup": false
                }
            })",
            serializeToBson(backupCursorStage));
        ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
            R"({
                "$backupCursor": {
                    "incrementalBackup": true,
                    "thisBackupName": "?string",
                    "srcBackupName": "?string",
                    "blockSize": "?number",
                    "disableIncrementalBackup": false
                }
            })",
            debugShape(backupCursorStage));

        representativeShape = representativeQueryShape(backupCursorStage);
        ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
            R"({
            "$backupCursor": {
                "incrementalBackup": true,
                "thisBackupName": "?",
                "srcBackupName": "?",
                "blockSize": 1,
                "disableIncrementalBackup": false
            }
        })",
            representativeShape);
    }

    // Test that we are able to parse the representative query.
    auto reParsed =
        DocumentSourceBackupCursor::createFromBson(representativeShape.firstElement(), expCtx);

    // Test that the representative query is a fix point - should always get here.
    ASSERT_BSONOBJ_EQ(representativeShape, representativeQueryShape(reParsed));
}

}  // namespace
}  // namespace mongo
