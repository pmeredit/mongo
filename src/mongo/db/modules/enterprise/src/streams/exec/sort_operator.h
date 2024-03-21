#pragma once

#include "mongo/db/exec/sort_executor.h"
#include "mongo/db/index/sort_key_generator.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace mongo {
class DocumentSourceSort;
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

    OperatorStats doGetStats() override;

    mongo::StreamMeta getStreamMeta();

    // Processes a eofSignal control message.
    void processEof();

private:
    friend class SortOperatorTest;

    Options _options;
    boost::optional<mongo::SortExecutor<mongo::Document>> _processor;
    boost::optional<mongo::SortKeyGenerator> _sortKeyGenerator;
    boost::optional<mongo::StreamMeta> _streamMetaTemplate;
    bool _receivedEof{false};
    bool _reachedEof{false};
};

}  // namespace streams
