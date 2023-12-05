/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_helpers.h"

namespace mongo {

class SearchIndexHelpersShard : public SearchIndexHelpers {
public:
    boost::optional<UUID> fetchCollectionUUID(OperationContext* opCtx,
                                              const NamespaceString& nss) override;

    UUID fetchCollectionUUIDOrThrow(OperationContext* opCtx, const NamespaceString& nss) override;
};

}  // namespace mongo
