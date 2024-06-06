/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace mongo {
class DocumentSourceSingleDocumentTransformation;
class SingleDocumentTransformationProcessor;
}  // namespace mongo

namespace streams {

struct Context;

/**
 * The base operator for $addFields, $project, $replaceRoot, $set.
 */
class SingleDocumentTransformationOperator : public Operator {
public:
    struct Options {
        // DocumentSource stage that this Operator wraps.
        mongo::DocumentSourceSingleDocumentTransformation* documentSource;
    };

    SingleDocumentTransformationOperator(Context* context, Options options);

    mongo::DocumentSourceSingleDocumentTransformation* documentSource() {
        return _options.documentSource;
    }

protected:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    Options _options;
    mongo::SingleDocumentTransformationProcessor* _processor{nullptr};
};


}  // namespace streams
