/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_helpers.h"

namespace mongo {

class SearchIndexHelpersMongod : public SearchIndexHelpers {
public:
    UUID fetchCollectionUUIDOrThrow(OperationContext* opCtx, const NamespaceString& nss) override;
};

}  // namespace mongo
