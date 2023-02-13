/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_helpers_mongod.h"

#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/service_context.h"

namespace mongo {

ServiceContext::ConstructorActionRegisterer searchIndexHelpersMongodImplementation{
    "searchIndexHelpersMongod-registration", [](ServiceContext* serviceContext) {
        invariant(serviceContext);
        SearchIndexHelpers::set(serviceContext, std::make_unique<SearchIndexHelpersMongod>());
    }};

UUID SearchIndexHelpersMongod::fetchCollectionUUIDOrThrow(OperationContext* opCtx,
                                                          const NamespaceString& nss) {
    auto optUuid = CollectionCatalog::get(opCtx)->lookupUUIDByNSS(opCtx, nss);
    uassert(ErrorCodes::NamespaceNotFound,
            str::stream() << "Collection '" << nss << "' does not exist.",
            optUuid);
    return optUuid.get();
}

}  // namespace mongo
