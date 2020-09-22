/**
 * Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include "export_collection.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/durable_catalog.h"

namespace mongo {

namespace {

const std::string kTableExtension = ".wt";

std::string constructFilePath(std::string ident) {
    boost::filesystem::path directoryPath = boost::filesystem::path(storageGlobalParams.dbpath);
    boost::filesystem::path filePath = directoryPath /= (ident + kTableExtension);
    return filePath.string();
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

    const Collection* collection = autoCollection.getCollection();
    out->append("namespace", nss.toString());

    // Extract the collection's catalog entry.
    DurableCatalog* durableCatalog = DurableCatalog::get(opCtx);
    out->append("metadata", durableCatalog->getCatalogEntry(opCtx, collection->getCatalogId()));

    // Append the size storer information.
    out->appendIntOrLL("numRecords", collection->numRecords(opCtx));
    out->appendIntOrLL("dataSize", collection->dataSize(opCtx));

    // Append the collection and index paths that need to be copied from disk.
    out->append("collectionFile", constructFilePath(collection->getSharedIdent()->getIdent()));

    BSONObjBuilder indexIdentsBuilder(out->subobjStart("indexFiles"));
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
    }

    // TODO SERVER-50977: Append the required WiredTiger information to 'out'.
}

}  // namespace mongo
