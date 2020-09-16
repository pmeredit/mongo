/**
 * Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "export_collection.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/durable_catalog.h"

namespace mongo {

namespace {

const std::string kTableExtension = ".wt";

}  // namespace

void exportCollection(OperationContext* opCtx, const NamespaceString& nss, BSONObjBuilder* out) {
    // TODO SERVER-50886: Verify that this function is only called when running in read-only mode.

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

    // Extract the collection's metadata.
    DurableCatalog* durableCatalog = DurableCatalog::get(opCtx);
    auto md = durableCatalog->getMetaData(opCtx, collection->getCatalogId());
    out->append("metadata", md.toBSON());

    // Append the size storer information.
    out->appendNumber("numRecords", collection->numRecords(opCtx));
    out->appendNumber("dataSize", collection->dataSize(opCtx));

    // Append the collection and index idents that need to be copied from disk.
    out->append("collectionIdent", collection->getSharedIdent()->getIdent() + kTableExtension);

    BSONArrayBuilder indexIdentsBuilder(out->subarrayStart("indexIdents"));
    const IndexCatalog* indexCatalog = collection->getIndexCatalog();
    auto it = indexCatalog->getIndexIterator(opCtx, /*includeUnfinishedIndexes=*/true);
    while (it->more()) {
        const IndexCatalogEntry* entry = it->next();
        const std::string indexName = entry->descriptor()->indexName();
        uassert(5091802,
                str::stream() << "Cannot export collection " << nss << ". Index " << indexName
                              << " has not finished building yet.",
                entry->isReady(opCtx));

        indexIdentsBuilder.append(BSON(indexName << entry->getIdent() + kTableExtension));
    }

    // TODO SERVER-50977: Append the required WiredTiger information to 'out'.
}

}  // namespace mongo
