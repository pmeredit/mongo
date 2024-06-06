/**
 * Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/namespace_string.h"

namespace mongo {

class BSONObjBuilder;
class OperationContext;

/**
 * Extracts the collection properties, appending them to 'out'. Throws if the collection with
 * namespace 'nss' does not exist or if the namespace 'nss' points to a view.
 *
 * The following collection properties are extracted and appended to 'out':
 *   - The collection metadata from the durable catalog.
 *   - The number of records and data size for the collection tracked in the size storer.
 *   - The idents for the collection and all the indexes belonging to that collection.
 */
void exportCollection(OperationContext* opCtx, const NamespaceString& nss, BSONObjBuilder* out);

}  // namespace mongo
