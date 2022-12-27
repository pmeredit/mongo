/**
 *    Copyright (C) 2022 MongoDB Inc.
 */

#include "mongo/db/pipeline/abt/document_source_visitor.h"
#include "mongo/db/pipeline/visitors/document_source_visitor_registry.h"
#include "mongo/db/query/cqf_command_utils.h"
#include "mongo/db/service_context.h"
#include "search/document_source_internal_search_id_lookup.h"
#include "search/document_source_internal_search_mongot_remote.h"
#include "search/document_source_search.h"
#include "search/document_source_search_meta.h"
#include "util/document_source_visitor_registry_enterprise.h"

namespace mongo::optimizer {

template <typename T>
void visit(ABTUnsupportedDocumentSourceVisitorContext* ctx, const T&) {
    ctx->eligible = false;
}

const ServiceContext::ConstructorActionRegisterer abtUnsupportedRegisterer{
    "ABTUnsupportedRegistererSearch", [](ServiceContext* service) {
        registerSearchVisitor<ABTUnsupportedDocumentSourceVisitorContext>(service);
    }};

template <typename T>
void visit(ABTDocumentSourceTranslationVisitorContext*, const T& source) {
    uasserted(ErrorCodes::InternalErrorNotSupported,
              str::stream() << "Stage is not supported: " << source.getSourceName());
}

const ServiceContext::ConstructorActionRegisterer abtTranslationRegisterer{
    "ABTTranslationRegistererSearch", [](ServiceContext* service) {
        registerSearchVisitor<ABTDocumentSourceTranslationVisitorContext>(service);
    }};

}  // namespace mongo::optimizer
