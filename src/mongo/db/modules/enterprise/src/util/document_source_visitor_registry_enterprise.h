/**
 *  Copyright (C) 2022-present MongoDB, Inc.
 */

#pragma once

#include "mongo/db/pipeline/visitors/document_source_visitor_registry.h"

namespace mongo {

class DocumentSourceInternalSearchIdLookUp;
class DocumentSourceInternalSearchMongotRemote;
class DocumentSourceSearchMeta;
class DocumentSourceSearch;
class DocumentSourceBackupCursor;
class DocumentSourceBackupCursorExtend;
class DocumentSourceBackupFile;

/**
 * Register 'visit()' functions for all search DocumentSources for the visitor specified as the
 * template parameter in the DocumentSource visitor regsitry in the given ServiceContext. Using this
 * function helps provide compile-time safety that ensures visitor implementors have provided an
 * implementation for all DocumentSoures. This function is intended to be used in the following
 * manner:
 *
 * // Define visit functions for all search DocumentSources
 * void visit(FooVisitorCtx* ctx, const DocumentSourceInternalSearchIdLookUp& match) { ... }
 * ...
 *
 * const ServiceContext::ConstructorActionRegisterer fooRegisterer{
 *   "FooRegisterer", [](ServiceContext* service) {
 *       registerSearchVisitor<FooVisitorCtx>(service);
 *   }};
 */
template <typename T>
void registerSearchVisitor(ServiceContext* service) {
    auto& registry = getDocumentSourceVisitorRegistry(service);
    registerVisitFuncs<T,
                       DocumentSourceInternalSearchIdLookUp,
                       DocumentSourceInternalSearchMongotRemote,
                       DocumentSourceSearchMeta,
                       DocumentSourceSearch>(&registry);
}

/**
 * See 'registerSearchVisitor'. This function has the same semantics except for the DocumentSources
 * defined in the 'hot_backups' module.
 */
template <typename T>
void registerBackupVisitor(ServiceContext* service) {
    auto& registry = getDocumentSourceVisitorRegistry(service);
    registerVisitFuncs<T,
                       DocumentSourceBackupCursor,
                       DocumentSourceBackupCursorExtend,
                       DocumentSourceBackupFile>(&registry);
}

}  // namespace mongo
