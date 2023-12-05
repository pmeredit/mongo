/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_helpers_shard.h"

#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/service_context.h"

namespace mongo {

ServiceContext::ConstructorActionRegisterer searchIndexHelpersShardImplementation{
    "searchIndexHelpersShard-registration", [](ServiceContext* serviceContext) {
        invariant(serviceContext);
        // Only register the router implementation if this server has a shard service.
        if (auto service = serviceContext->getService(ClusterRole::ShardServer); service) {
            SearchIndexHelpers::set(service, std::make_unique<SearchIndexHelpersShard>());
        }
    }};

boost::optional<UUID> SearchIndexHelpersShard::fetchCollectionUUID(OperationContext* opCtx,
                                                                   const NamespaceString& nss) {
    return CollectionCatalog::get(opCtx)->lookupUUIDByNSS(opCtx, nss);
}

UUID SearchIndexHelpersShard::fetchCollectionUUIDOrThrow(OperationContext* opCtx,
                                                         const NamespaceString& nss) {
    auto optUuid = fetchCollectionUUID(opCtx, nss);
    uassert(ErrorCodes::NamespaceNotFound,
            str::stream() << "Collection '" << nss.toStringForErrorMsg() << "' does not exist.",
            optUuid);
    return optUuid.get();
}

}  // namespace mongo
