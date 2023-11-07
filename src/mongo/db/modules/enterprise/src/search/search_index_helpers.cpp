/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_helpers.h"

#include "mongo/db/service_context.h"

namespace mongo {

Service::Decoration<std::unique_ptr<SearchIndexHelpers>> searchIndexHelpersDecoration =
    Service::declareDecoration<std::unique_ptr<SearchIndexHelpers>>();

SearchIndexHelpers* SearchIndexHelpers::get(Service* service) {
    invariant(searchIndexHelpersDecoration(service).get());
    return searchIndexHelpersDecoration(service).get();
}

SearchIndexHelpers* SearchIndexHelpers::get(OperationContext* ctx) {
    return get(ctx->getService());
}

void SearchIndexHelpers::set(Service* service, std::unique_ptr<SearchIndexHelpers> impl) {
    invariant(!searchIndexHelpersDecoration(service).get());
    searchIndexHelpersDecoration(service) = std::move(impl);
}

}  // namespace mongo
