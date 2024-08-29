/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/exec/sort_executor.h"
#include "mongo/db/index/sort_key_generator.h"
#include "mongo/util/chunked_memory_aggregator.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/window_assigner.h"
#include "streams/exec/window_aware_operator.h"

namespace mongo {
class DocumentSourceSort;
}  // namespace mongo

namespace streams {

struct Context;

// A "window aware" implementation of the $sort stage. See the base class for more details.
class SortOperator : public WindowAwareOperator {
public:
    struct Options : public WindowAwareOperator::Options {
        Options(WindowAwareOperator::Options baseOptions)
            : WindowAwareOperator::Options(std::move(baseOptions)) {}

        // DocumentSourceGroup stage that this Operator wraps.
        mongo::DocumentSourceSort* documentSource;
    };

    SortOperator(Context* context, Options options);

    mongo::DocumentSourceSort* documentSource() {
        return _options.documentSource;
    }

private:
    std::string doGetName() const override {
        return "SortOperator";
    }

    // $sort information in each open window.
    struct SortWindow : public WindowAwareOperator::Window {
        SortWindow(WindowAwareOperator::Window baseWindow,
                   boost::intrusive_ptr<mongo::DocumentSource> documentSource,
                   std::unique_ptr<mongo::SortExecutor<mongo::Document>> processor,
                   boost::optional<mongo::SortKeyGenerator> sortKeyGenerator,
                   mongo::MemoryUsageHandle memoryUsageHandle)
            : WindowAwareOperator::Window(std::move(baseWindow)),
              documentSource(std::move(documentSource)),
              processor(std::move(processor)),
              sortKeyGenerator(std::move(sortKeyGenerator)),
              memoryUsageHandle(std::move(memoryUsageHandle)) {}

        boost::intrusive_ptr<mongo::DocumentSource> documentSource;
        std::unique_ptr<mongo::SortExecutor<mongo::Document>> processor;
        boost::optional<mongo::SortKeyGenerator> sortKeyGenerator;
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

    SortWindow* getSortWindow(WindowAwareOperator::Window* window);

    Options _options;
};

}  // namespace streams
