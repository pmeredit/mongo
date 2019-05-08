/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_search_beta.h"

#include "document_source_internal_search_beta_id_lookup.h"
#include "document_source_internal_search_beta_mongot_remote.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression_context.h"

namespace mongo {

using boost::intrusive_ptr;
using std::list;

REGISTER_MULTI_STAGE_ALIAS(searchBeta,
                           DocumentSourceSearchBeta::LiteParsed::parse,
                           DocumentSourceSearchBeta::createFromBson);

const char* DocumentSourceSearchBeta::getSourceName() const {
    return "$searchBeta";
}

list<intrusive_ptr<DocumentSource>> DocumentSourceSearchBeta::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {

    uassert(ErrorCodes::FailedToParse,
            str::stream() << "$searchBeta value must be an object. Found: "
                          << typeName(elem.type()),
            elem.type() == BSONType::Object);

    return {DocumentSourceInternalSearchBetaMongotRemote::createFromBson(elem, pExpCtx),
            new DocumentSourceInternalSearchBetaIdLookUp(pExpCtx)};
}

}  // namespace mongo
