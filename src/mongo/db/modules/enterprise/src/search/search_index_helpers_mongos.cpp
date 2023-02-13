/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_helpers_mongos.h"

#include "mongo/db/service_context.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/grid.h"

namespace mongo {

ServiceContext::ConstructorActionRegisterer searchIndexHelpersMongosImplementation{
    "searchIndexHelpersMongos-registration", [](ServiceContext* serviceContext) {
        invariant(serviceContext);
        SearchIndexHelpers::set(serviceContext, std::make_unique<SearchIndexHelpersMongos>());
    }};

UUID SearchIndexHelpersMongos::fetchCollectionUUIDOrThrow(OperationContext* opCtx,
                                                          const NamespaceString& nss) {
    auto shardingCatalogClient = Grid::get(opCtx)->catalogClient();
    invariant(shardingCatalogClient);

    // This will throw NamespaceNotFound if the collection doesn't exist.
    return shardingCatalogClient->getCollection(opCtx, nss).getUuid();
}

}  // namespace mongo
