/**
 * Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "live_import/collection_properties_gen.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"

namespace mongo {

/**
 * Import a collection.
 *
 * This function first verifies that the collection with namespace nss does not already exist. And
 * then it validates that the provided catalog metadata is well-formed and that the ident files
 * corresponding to the idents specified in the metadata do exist and are importable. Finally, this
 * function writes the metadata of the collection into the durable catalog, instructs the storage
 * engine to import the ident files and updates any necessary in-memory catalog structures. If this
 * is a dryRun, the import will be aborted after making sure the collection is importable.
 * Otherwise, the collection will be imported.
 */
void importCollection(OperationContext* opCtx,
                      const UUID& importUUID,
                      const NamespaceString& nss,
                      long long numRecords,
                      long long dataSize,
                      const BSONObj& catalogEntry,
                      const BSONObj& storageMetadata,
                      bool isDryRun);
/**
 * Implementation of the importCollection command.
 *
 * This function performs additional sanity checks before calling the importCollection function
 * above.
 * When force is false, this function first does a dryRun import and then waits for all other
 * nodes in the cluster to do the same. If the dryRun finishes successfully on all nodes, it will
 * perform an actual import. Otherwise, an error will be returned. If failover happens during the
 * dryRun phase, the import command will be interrupted and return an error.
 * When force is true, this function performs an actual import directly.
 */
void runImportCollectionCommand(OperationContext* opCtx,
                                const CollectionProperties& collectionProperties,
                                bool force);

}  // namespace mongo
