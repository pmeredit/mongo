#pragma once

#include "mongo/db/exec/sort_executor.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace mongo {
class DocumentSourceSort;
class SortKeyGenerator;
}  // namespace mongo

namespace streams {

struct Context;

/**
 * The operator that implements the $sort stage.
 */
class SortOperator : public Operator {
public:
    struct Options {
        // DocumentSourceGroup stage that this Operator wraps.
        mongo::DocumentSourceSort* documentSource;
    };

    SortOperator(Context* context, Options options);

    mongo::DocumentSourceSort* documentSource() {
        return _options.documentSource;
    }

protected:
    std::string doGetName() const override {
        return "SortOperator";
    }

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    Options _options;
    mongo::SortExecutor<mongo::Document>* _processor{nullptr};
    mongo::SortKeyGenerator* _sortKeyGenerator{nullptr};
};

}  // namespace streams
