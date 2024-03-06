/**
 * Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include "export_collection.h"

#include "live_import/collection_properties_gen.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/durable_catalog.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"

namespace mongo {

namespace {

const std::string kTableExtension = ".wt";

std::string constructFilePath(std::string ident) {
    boost::filesystem::path directoryPath = boost::filesystem::path(storageGlobalParams.dbpath);
    boost::filesystem::path filePath = directoryPath /= (ident + kTableExtension);
    return filePath.string();
}

void appendStorageMetadata(OperationContext* opCtx, const std::string& ident, BSONObjBuilder* out) {
    const std::string tableUri = "table:" + ident;
    const std::string fileUri = "file:" + ident + kTableExtension;

    auto tableMetadata = uassertStatusOK(WiredTigerUtil::getMetadata(opCtx, tableUri));
    auto fileMetadata = uassertStatusOK(WiredTigerUtil::getMetadata(opCtx, fileUri));

    out->append(ident, BSON("tableMetadata" << tableMetadata << "fileMetadata" << fileMetadata));
}

}  // namespace

void exportCollection(OperationContext* opCtx, const NamespaceString& nss, BSONObjBuilder* out) {
    uassert(5088600,
            "Exporting a collection is only permitted in read-only mode.",
            storageGlobalParams.readOnly);

    // Allow fetching views to provide a well defined error below.
    AutoGetCollectionForRead autoCollection(opCtx, nss, AutoGetCollectionViewMode::kViewsPermitted);
    uassert(5091800,
            str::stream() << "The given namespace " << nss
                          << " is a view. Only collections can be exported.",
            !autoCollection.getView());
    uassert(5091801,
            str::stream() << "Collection with namespace " << nss << " does not exist.",
            autoCollection.getDb() && autoCollection.getCollection());

    CollectionProperties collectionProperties;
    const auto& collection = autoCollection.getCollection();
    collectionProperties.setNs(nss);

    // Extract the collection's catalog entry.
    DurableCatalog* durableCatalog = DurableCatalog::get(opCtx);
    collectionProperties.setMetadata(
        durableCatalog->getCatalogEntry(opCtx, collection->getCatalogId()));

    // Append the size storer information.
    collectionProperties.setNumRecords(collection->numRecords(opCtx));
    collectionProperties.setDataSize(collection->dataSize(opCtx));

    // This will contain the WiredTiger file and table metadata for each ident being exported.
    BSONObjBuilder wtMetadataBuilder;

    // Append the collection and index paths that need to be copied from disk.
    collectionProperties.setCollectionFile(
        constructFilePath(collection->getSharedIdent()->getIdent()));
    appendStorageMetadata(opCtx, collection->getSharedIdent()->getIdent(), &wtMetadataBuilder);

    BSONObjBuilder indexIdentsBuilder;
    const IndexCatalog* indexCatalog = collection->getIndexCatalog();
    auto it = indexCatalog->getIndexIterator(opCtx, /*includeUnfinishedIndexes=*/true);
    while (it->more()) {
        const IndexCatalogEntry* entry = it->next();
        const std::string indexName = entry->descriptor()->indexName();
        uassert(5091802,
                str::stream() << "Cannot export collection " << nss << ". Index " << indexName
                              << " has not finished building yet.",
                entry->isReady(opCtx));

        indexIdentsBuilder.append(indexName, constructFilePath(entry->getIdent()));
        appendStorageMetadata(opCtx, entry->getIdent(), &wtMetadataBuilder);
    }
    collectionProperties.setIndexFiles(indexIdentsBuilder.obj());
    collectionProperties.setStorageMetadata(wtMetadataBuilder.obj());
    collectionProperties.serialize(out);
}

}  // namespace mongo
