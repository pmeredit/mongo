#pragma once

#include <queue>

#include "mongo/stdx/condition_variable.h"
#include "streams/exec/generated_data_source_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"

namespace streams {

/**
 * This test-only class can act as a source in an operator dag.
 * You can use it to push a set of documents through the operator dag.
 * This class is thread-safe.
 */
class InMemorySourceOperator : public GeneratedDataSourceOperator {
public:
    struct Options : public SourceOperator::Options {
        Options(SourceOperator::Options baseOptions)
            : SourceOperator::Options(std::move(baseOptions)) {}
        Options() = default;
    };

    InMemorySourceOperator(Context* context, Options options);

    /**
     * Adds a data message and an optional control message to this operator.
     * The message will be sent forward by runOnce().
     */
    void addDataMsg(StreamDataMsg dataMsg,
                    boost::optional<StreamControlMsg> controlMsg = boost::none);

    /**
     * Adds a control message to this operator.
     * The message will be sent forward by runOnce().
     */
    void addControlMsg(StreamControlMsg controlMsg);

    // Returns the list of currently buffered messages that have not been consumed yet.
    std::vector<StreamMsgUnion> getMessages(mongo::WithLock) override;

private:
    friend class InMemorySourceSinkOperatorTest;
    friend class WindowOperatorTest;

    std::string doGetName() const override {
        return "InMemorySourceOperator";
    }

    const SourceOperator::Options& getOptions() const override {
        return _options;
    }

    void addDataMsgInner(StreamDataMsg dataMsg, boost::optional<StreamControlMsg> controlMsg);
    void addControlMsgInner(StreamControlMsg controlMsg);

    Options _options;

    /**
     * This field holds the messages added to this operator by addDataMsg() and addControlMsg().
     * This is protected by the GeneratedDataSourceOperator's mutex.
     */
    std::vector<StreamMsgUnion> _messages;
};

}  // namespace streams
