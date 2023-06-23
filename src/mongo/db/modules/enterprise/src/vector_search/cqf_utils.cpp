/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/db/pipeline/abt/document_source_visitor.h"
#include "mongo/db/query/cqf_command_utils.h"
#include "util/document_source_visitor_registry_enterprise.h"
#include "vector_search/document_source_vector_search.h"

namespace mongo::optimizer {

template <typename T>
void visit(ABTUnsupportedDocumentSourceVisitorContext* ctx, const T&) {
    ctx->eligible = false;
}

const ServiceContext::ConstructorActionRegisterer abtUnsupportedRegisterer{
    "ABTUnsupportedRegistererVectorSearch", [](ServiceContext* service) {
        registerVectorSearchVisitor<ABTUnsupportedDocumentSourceVisitorContext>(service);
    }};

template <typename T>
void visit(ABTDocumentSourceTranslationVisitorContext*, const T& source) {
    uasserted(ErrorCodes::InternalErrorNotSupported,
              str::stream() << "Stage is not supported: " << source.getSourceName());
}

const ServiceContext::ConstructorActionRegisterer abtTranslationRegisterer{
    "ABTTranslationRegistererVectorSearch", [](ServiceContext* service) {
        registerVectorSearchVisitor<ABTDocumentSourceTranslationVisitorContext>(service);
    }};

}  // namespace mongo::optimizer
