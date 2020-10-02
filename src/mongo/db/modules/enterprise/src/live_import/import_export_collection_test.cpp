/**
 * Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "export_collection.h"
#include "import_collection.h"

#include "mongo/db/catalog/catalog_test_fixture.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/uuid.h"

namespace mongo {
namespace {

class ImportExportCollectionTest : public CatalogTestFixture {
public:
    ImportExportCollectionTest() : CatalogTestFixture("wiredTiger") {}

    BSONObj exportCollection(const NamespaceString& nss) {
        storageGlobalParams.readOnly = true;
        BSONObjBuilder exportBuilder;
        mongo::exportCollection(operationContext(), nss, &exportBuilder);
        storageGlobalParams.readOnly = false;
        return exportBuilder.obj();
    }
};

TEST_F(ImportExportCollectionTest, SizeInfoDryRun) {
    const NamespaceString nss("test.foo");
    ASSERT_OK(storageInterface()->createCollection(operationContext(), nss, {}));
    BSONObj exportObj = exportCollection(nss);

    // Rename the collection out of the way as we want to keep the on-disk files intact.
    bool stayTemp = false;
    ASSERT_OK(storageInterface()->renameCollection(
        operationContext(), nss, NamespaceString("test.bar"), stayTemp));

    long long numRecords = 1000;
    long long dataSize = 2000;
    bool isDryRun = true;
    importCollection(operationContext(),
                     UUID::gen(),
                     nss,
                     numRecords,
                     dataSize,
                     exportObj.getField("metadata").Obj(),
                     isDryRun);

    // Because this was a dry run, the collection should not exist.
    ASSERT(!CollectionCatalog::get(operationContext())
                .lookupCollectionByNamespaceForRead(operationContext(), nss));
}

TEST_F(ImportExportCollectionTest, SizeInfoNoDryRun) {
    const NamespaceString nss("test.foo");
    ASSERT_OK(storageInterface()->createCollection(operationContext(), nss, {}));
    BSONObj exportObj = exportCollection(nss);

    // Rename the collection out of the way as we want to keep the on-disk files intact.
    bool stayTemp = false;
    ASSERT_OK(storageInterface()->renameCollection(
        operationContext(), nss, NamespaceString("test.bar"), stayTemp));

    long long numRecords = 1000;
    long long dataSize = 2000;
    bool isDryRun = false;
    importCollection(operationContext(),
                     UUID::gen(),
                     nss,
                     numRecords,
                     dataSize,
                     exportObj.getField("metadata").Obj(),
                     isDryRun);

    // Verify the number of records and data size.
    auto swCollectionCount = storageInterface()->getCollectionCount(operationContext(), {nss});
    ASSERT_OK(swCollectionCount);

    auto swCollectionSize = storageInterface()->getCollectionSize(operationContext(), {nss});
    ASSERT_OK(swCollectionSize);

    ASSERT_EQUALS(swCollectionCount.getValue(), numRecords);
    ASSERT_EQUALS(swCollectionSize.getValue(), dataSize);
}

}  // namespace
}  // namespace mongo
