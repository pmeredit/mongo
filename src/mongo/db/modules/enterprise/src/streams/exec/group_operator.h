#pragma once

#include "streams/exec/group_processor.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace mongo {
class DocumentSourceGroup;
class GroupProcessor;
}  // namespace mongo

namespace streams {

struct Context;

/**
 * The operator that implements the $group stage.
 */
class GroupOperator : public Operator {
public:
    struct Options {
        // DocumentSourceGroup stage that this Operator wraps.
        mongo::DocumentSourceGroup* documentSource;
    };

    GroupOperator(Context* context, Options options);

    mongo::DocumentSourceGroup* documentSource() {
        return _options.documentSource;
    }

protected:
    std::string doGetName() const override {
        return "GroupOperator";
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
    friend class GroupOperatorTest;

    Options _options;
    GroupProcessor _processor;
    boost::optional<mongo::StreamMeta> _streamMetaTemplate;
    bool _receivedEof{false};
    bool _reachedEof{false};
};
}  // namespace streams
