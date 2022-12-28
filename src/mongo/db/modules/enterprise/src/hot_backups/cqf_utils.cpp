/**
 *    Copyright (C) 2022 MongoDB Inc.
 */

#include "mongo/db/pipeline/visitors/document_source_visitor_registry.h"
#include "mongo/db/query/cqf_command_utils.h"
#include "mongo/db/service_context.h"
#include "util/document_source_visitor_registry_enterprise.h"

namespace mongo::optimizer {

template <typename T>
void visit(ABTUnsupportedDocumentSourceVisitorContext* ctx, const T&) {
    ctx->eligible = false;
}

const ServiceContext::ConstructorActionRegisterer abtUnsupportedRegisterer{
    "ABTUnsupportedRegistererBackup", [](ServiceContext* service) {
        registerBackupVisitor<ABTUnsupportedDocumentSourceVisitorContext>(service);
    }};

}  // namespace mongo::optimizer
