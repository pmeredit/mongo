/**
 *  Copyright (C) 2022-present MongoDB, Inc.
 */

#pragma once

#include "hot_backups/document_source_backup_cursor.h"
#include "hot_backups/document_source_backup_cursor_extend.h"
#include "hot_backups/document_source_backup_file.h"
#include "mongo/db/pipeline/visitors/document_source_visitor_registry.h"
#include "search/document_source_internal_search_id_lookup.h"
#include "search/document_source_internal_search_mongot_remote.h"
#include "search/document_source_search.h"
#include "search/document_source_search_meta.h"

namespace mongo {

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
