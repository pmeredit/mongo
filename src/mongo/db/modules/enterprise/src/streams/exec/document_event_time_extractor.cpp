/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "streams/exec/document_event_time_extractor.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

namespace streams {

using namespace mongo;

DocumentEventTimeExtractor::DocumentEventTimeExtractor(
    boost::intrusive_ptr<mongo::ExpressionContext> expCtx,
    boost::intrusive_ptr<mongo::Expression> expr)
    : _expCtx(std::move(expCtx)), _expr(std::move(expr)) {}

Date_t DocumentEventTimeExtractor::extractEventTime(const Document& doc) {
    auto eventTimeVal = _expr->evaluate(doc, &_expCtx->variables);
    return eventTimeVal.getDate();
}

}  // namespace streams
