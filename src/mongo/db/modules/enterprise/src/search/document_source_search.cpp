/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_search.h"

#include "document_source_internal_search_id_lookup.h"
#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/expression_context.h"

namespace mongo {

using boost::intrusive_ptr;
using std::list;

REGISTER_MULTI_STAGE_ALIAS(search,
                           DocumentSourceSearch::LiteParsed::parse,
                           DocumentSourceSearch::createFromBson);

// $searchBeta is supported as an alias for $search for compatibility with applications that used
// search during its beta period.
REGISTER_MULTI_STAGE_ALIAS(searchBeta,
                           DocumentSourceSearch::LiteParsed::parse,
                           DocumentSourceSearch::createFromBson);

const char* DocumentSourceSearch::getSourceName() const {
    return kStageName.rawData();
}

std::list<intrusive_ptr<DocumentSource>> DocumentSourceSearch::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {

    uassert(ErrorCodes::FailedToParse,
            str::stream() << "$search value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);

    return {
        DocumentSourceInternalSearchMongotRemote::createFromBson(elem, pExpCtx),
        new DocumentSourceInternalSearchIdLookUp(pExpCtx),
    };
}

}  // namespace mongo
