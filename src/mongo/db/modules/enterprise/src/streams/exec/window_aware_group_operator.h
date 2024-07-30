/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "mongo/util/chunked_memory_aggregator.h"
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
                    std::unique_ptr<GroupProcessor> processor,
                    mongo::MemoryUsageHandle memoryUsageHandle)
            : WindowAwareOperator::Window(std::move(base)),
              documentSource(std::move(documentSource)),
              processor(std::move(processor)),
              memoryUsageHandle(std::move(memoryUsageHandle)) {}

        // The cloned DocumentSource used to create the processor for this window.
        boost::intrusive_ptr<mongo::DocumentSource> documentSource;
        // The GroupProcessor for this window.
        std::unique_ptr<GroupProcessor> processor;
        // Tracks memory usage within the processor.
        mongo::MemoryUsageHandle memoryUsageHandle;

        void doMerge(Window* other) override;
    };

    void doProcessDocs(Window* window, std::vector<StreamDocument> streamDocs) override;
    std::unique_ptr<Window> doMakeWindow(Window baseState) override;
    void doCloseWindow(Window* window) override;
    void doUpdateStats(Window* window) override;
    void doSaveWindowState(CheckpointStorage::WriterHandle* writer, Window* window) override;
    void doRestoreWindowState(Window* window, mongo::Document record) override;
    const WindowAwareOperator::Options& getOptions() const override {
        return _options;
    }

    // Checks and casts the window base class to a group window.
    GroupWindow* getGroupWindow(WindowAwareOperator::Window* window);

    // Options supplied to the operator.
    Options _options;
};

}  // namespace streams
