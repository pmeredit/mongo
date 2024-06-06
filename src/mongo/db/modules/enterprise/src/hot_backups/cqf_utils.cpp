/**
 *    Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "document_source_backup_cursor.h"
#include "document_source_backup_cursor_extend.h"
#include "document_source_backup_file.h"
#include "mongo/db/pipeline/abt/document_source_visitor.h"
#include "mongo/db/pipeline/visitors/document_source_visitor_registry.h"
#include "mongo/db/query/cqf_command_utils.h"
#include "mongo/db/service_context.h"
#include "util/document_source_visitor_registry_enterprise.h"

namespace mongo::optimizer {

template <typename T>
void visit(ABTUnsupportedDocumentSourceVisitorContext* ctx, const T&) {
    ctx->eligibility.setIneligible();
}

const ServiceContext::ConstructorActionRegisterer abtUnsupportedRegisterer{
    "ABTUnsupportedRegistererBackup", [](ServiceContext* service) {
        registerBackupVisitor<ABTUnsupportedDocumentSourceVisitorContext>(service);
    }};

template <typename T>
void visit(ABTDocumentSourceTranslationVisitorContext*, const T& source) {
    uasserted(ErrorCodes::InternalErrorNotSupported,
              str::stream() << "Stage is not supported: " << source.getSourceName());
}

const ServiceContext::ConstructorActionRegisterer abtTranslationRegisterer{
    "ABTTranslationRegistererBackup", [](ServiceContext* service) {
        registerBackupVisitor<ABTDocumentSourceTranslationVisitorContext>(service);
    }};

}  // namespace mongo::optimizer
