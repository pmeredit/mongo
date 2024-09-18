/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/document_timestamp_extractor.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

DocumentTimestampExtractor::DocumentTimestampExtractor(
    boost::intrusive_ptr<mongo::ExpressionContext> expCtx,
    boost::intrusive_ptr<mongo::Expression> expr)
    : _expCtx(std::move(expCtx)), _expr(std::move(expr)) {}

Date_t DocumentTimestampExtractor::extractTimestamp(const Document& doc) {
    auto timestampVal = _expr->evaluate(doc, &_expCtx->variables);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Failed to extract timestamp from document, extracted timestampVal: "
                          << timestampVal.toString(),
            timestampVal.getType() == BSONType::Date);
    return timestampVal.getDate();
}

}  // namespace streams
