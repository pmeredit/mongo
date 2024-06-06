/**
 *  Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/pipeline/visitors/document_source_visitor_registry.h"

namespace mongo {

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
 * void visit(FooVisitorCtx* ctx, const DocumentSourceBackupCursor& docSource) { ... }
 * ...
 *
 * const ServiceContext::ConstructorActionRegisterer fooRegisterer{
 *   "FooRegisterer", [](ServiceContext* service) {
 *       registerBackupVisitor<FooVisitorCtx>(service);
 *   }};
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
