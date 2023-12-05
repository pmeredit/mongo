/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"

namespace mongo {

/**
 * Interface to separate router role and shard role implementations.
 */
class SearchIndexHelpers {
public:
    virtual ~SearchIndexHelpers() = default;

    static SearchIndexHelpers* get(Service* service);
    static SearchIndexHelpers* get(OperationContext* opCtx);

    static void set(Service* service, std::unique_ptr<SearchIndexHelpers> impl);

    /**
     * Returns the collection UUID or throws a NamespaceNotFound error.
     */
    virtual UUID fetchCollectionUUIDOrThrow(OperationContext* opCtx,
                                            const NamespaceString& nss) = 0;

    /**
     * Returns the collection UUID or boost::none if no collection is found.
     */
    virtual boost::optional<UUID> fetchCollectionUUID(OperationContext* opCtx,
                                                      const NamespaceString& nss) = 0;
};

}  // namespace mongo
