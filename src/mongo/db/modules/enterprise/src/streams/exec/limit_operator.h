/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/window_aware_operator.h"

namespace streams {

struct Context;

// A "window aware" implementation of the $limit stage. See the base class for more details.
class LimitOperator : public WindowAwareOperator {
public:
    struct Options : public WindowAwareOperator::Options {
        Options(WindowAwareOperator::Options baseOptions)
            : WindowAwareOperator::Options(std::move(baseOptions)) {}

        int64_t limit{0};
    };

    LimitOperator(Context* context, Options options);

private:
    friend class WindowAwareOperatorTest;

    std::string doGetName() const override {
        return "LimitOperator";
    }

    // $limit information in each open window.
    struct LimitWindow : public WindowAwareOperator::Window {
        LimitWindow(WindowAwareOperator::Window base)
            : WindowAwareOperator::Window(std::move(base)) {}

        // The number of docs sent already.
        int64_t numSent{0};
        int64_t limit{0};

        void doMerge(Window* other) override;
    };

    void doProcessDocs(Window* window, std::vector<StreamDocument> streamDocs) override;
    std::unique_ptr<Window> doMakeWindow(Window baseState) override;
    // This is a no-op for $limit.
    void doCloseWindow(Window* window) override {}
    // This is a no-op for $limit.
    void doUpdateStats(Window* window) override {}
    void doSaveWindowState(CheckpointStorage::WriterHandle* writer, Window* window) override;
    void doRestoreWindowState(Window* window, mongo::Document record) override;
    const WindowAwareOperator::Options& getOptions() const override {
        return _options;
    }

    // Checks and casts the window base class to a limit window.
    LimitWindow* getLimitWindow(WindowAwareOperator::Window* window);

    // Options supplied to the operator.
    Options _options;
};

}  // namespace streams
