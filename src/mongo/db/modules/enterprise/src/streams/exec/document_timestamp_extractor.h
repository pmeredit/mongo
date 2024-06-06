/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/exec/document_value/document.h"
#include "mongo/util/time_support.h"

namespace mongo {
class Expression;
class ExpressionContext;
}  // namespace mongo

namespace streams {

/**
 * This class can be used to extract event time from a document using the given expression.
 */
class DocumentTimestampExtractor {
public:
    DocumentTimestampExtractor(boost::intrusive_ptr<mongo::ExpressionContext> expCtx,
                               boost::intrusive_ptr<mongo::Expression> expr);

    /**
     * Extracts event timestamp from the given document.
     * Throws if there are any errors in evaluating the expression on 'doc' or if the result
     * is not a Date_t value.
     */
    mongo::Date_t extractTimestamp(const mongo::Document& doc);

private:
    boost::intrusive_ptr<mongo::ExpressionContext> _expCtx;
    boost::intrusive_ptr<mongo::Expression> _expr;
};

}  // namespace streams
