/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_helpers.h"

#include "mongo/db/service_context.h"

namespace mongo {

ServiceContext::Decoration<std::unique_ptr<SearchIndexHelpers>> searchIndexHelpersDecoration =
    ServiceContext::declareDecoration<std::unique_ptr<SearchIndexHelpers>>();

SearchIndexHelpers* SearchIndexHelpers::get(ServiceContext* service) {
    invariant(searchIndexHelpersDecoration(service).get());
    return searchIndexHelpersDecoration(service).get();
}

SearchIndexHelpers* SearchIndexHelpers::get(OperationContext* ctx) {
    return get(ctx->getClient()->getServiceContext());
}

void SearchIndexHelpers::set(ServiceContext* serviceContext,
                             std::unique_ptr<SearchIndexHelpers> impl) {
    invariant(!searchIndexHelpersDecoration(serviceContext).get());
    searchIndexHelpersDecoration(serviceContext) = std::move(impl);
}

}  // namespace mongo
