/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"

namespace mongo {

/**
 * Interface to separate linking of mongod and mongos implementations.
 *
 * In this way the search index commands can be implemented once and linked into both the mongod and
 * mongos binaries, while avoiding linking the mongod or mongos specific code into the other binary.
 */
class SearchIndexHelpers {
public:
    virtual ~SearchIndexHelpers() = default;

    static SearchIndexHelpers* get(ServiceContext* serviceContext);
    static SearchIndexHelpers* get(OperationContext* opCtx);

    static void set(ServiceContext* serviceContext, std::unique_ptr<SearchIndexHelpers> impl);

    /**
     * Returns the collection UUID or throws a NamespaceNotFound error.
     */
    virtual UUID fetchCollectionUUIDOrThrow(OperationContext* opCtx,
                                            const NamespaceString& nss) = 0;
};

}  // namespace mongo
