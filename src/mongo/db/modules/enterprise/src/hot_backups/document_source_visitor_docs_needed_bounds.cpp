/**
 *    Copyright (C) 2024 MongoDB Inc.
 */

#include "mongo/db/pipeline/visitors/document_source_visitor_docs_needed_bounds.h"
#include "mongo/db/pipeline/visitors/document_source_visitor_registry.h"

#include "document_source_backup_cursor.h"
#include "document_source_backup_cursor_extend.h"
#include "document_source_backup_file.h"
#include "util/document_source_visitor_registry_enterprise.h"

namespace mongo {
template <typename T>
void visit(DocsNeededBoundsContext* ctx, const T&) {
    ctx->applyUnknownStage();
}

const ServiceContext::ConstructorActionRegisterer DocsNeededBoundsRegisterer{
    "DocsNeededBoundsRegistererBackup",
    [](ServiceContext* service) { registerBackupVisitor<DocsNeededBoundsContext>(service); }};
}  // namespace mongo
