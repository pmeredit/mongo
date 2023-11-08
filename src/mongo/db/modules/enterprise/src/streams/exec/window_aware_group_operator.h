#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/group_processor.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/window_assigner.h"
#include "streams/exec/window_aware_operator.h"

namespace mongo {
class DocumentSourceGroup;
class GroupProcessor;
}  // namespace mongo

namespace streams {

struct Context;

// A "window aware" implementation of the $group stage. See the base class for more details.
// TODO(SERVER-82922): Rename to GroupOperator.
class WindowAwareGroupOperator : public WindowAwareOperator {
public:
    struct Options : public WindowAwareOperator::Options {
        Options(WindowAwareOperator::Options baseOptions)
            : WindowAwareOperator::Options(std::move(baseOptions)) {}

        // The parsed DocumentSourceGroup stage that this Operator duplicates to create
        // GroupProcessor instances for each window.
        mongo::DocumentSourceGroup* documentSource;
    };

    WindowAwareGroupOperator(Context* context, Options options);

    mongo::DocumentSourceGroup* documentSource() {
        return _options.documentSource;
    }

private:
    std::string doGetName() const override {
        return "GroupOperator";
    }

    // $group information in each open window.
    struct GroupWindow : public WindowAwareOperator::Window {
        GroupWindow(WindowAwareOperator::Window base,
                    boost::intrusive_ptr<mongo::DocumentSource> documentSource,
                    std::unique_ptr<GroupProcessor> processor)
            : WindowAwareOperator::Window(std::move(base)),
              documentSource(std::move(documentSource)),
              processor(std::move(processor)) {}

        // The cloned DocumentSource used to create the processor for this window.
        boost::intrusive_ptr<mongo::DocumentSource> documentSource;
        // The GroupProcessor for this window.
        std::unique_ptr<GroupProcessor> processor;
    };

    void doProcessDocs(Window* window, const std::vector<StreamDocument>& streamDocs) override;
    std::unique_ptr<Window> doMakeWindow(Window baseState) override;
    void doCloseWindow(Window* window) override;
    void doUpdateStats(Window* window) override;

    // Checks and casts the window base class to a group window.
    GroupWindow* getGroupWindow(WindowAwareOperator::Window* window);

    // Options supplied to the operator.
    Options _options;
};

}  // namespace streams
