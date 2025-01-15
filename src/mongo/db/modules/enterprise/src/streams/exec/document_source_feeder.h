/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <queue>

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/query/query_shape/serialization_options.h"

namespace streams {

/**
 * This class is used to feed documents to DocumentSource instances. It allows
 * us to let DocumentSources pull documents via the getNext() API while the
 * Operators push documents.
 */
class DocumentSourceFeeder : public mongo::DocumentSource {
public:
    DocumentSourceFeeder(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx);

    const char* getSourceName() const override;

    static const Id& id;

    Id getId() const override {
        return id;
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() override {
        MONGO_UNREACHABLE;
    }

    mongo::StageConstraints constraints(mongo::Pipeline::SplitState pipeState) const override {
        MONGO_UNREACHABLE;
    }

    void addVariableRefs(std::set<mongo::Variables::Id>* refs) const override {
        MONGO_UNREACHABLE;
    }

    mongo::Value serialize(
        const mongo::SerializationOptions& opts = mongo::SerializationOptions{}) const override {
        MONGO_UNREACHABLE;
    }

    /**
     * Adds a document to the feeder.
     */
    void addDocument(mongo::Document doc);

    /**
     * Whether doDispose() has been called.
     * Only needed for testing purposes.
     */
    bool isDisposed() const {
        return _isDisposed;
    }

    /**
     * Set whether Pause or EOF signal is sent when this
     * feeder is out of _docs.
     */
    void setEndOfBufferSignal(GetNextResult signal) {
        _endOfBufferSignal = std::move(signal);
    }

protected:
    mongo::DocumentSource::GetNextResult doGetNext() override;

    void doDispose() override {
        _isDisposed = true;
    }

private:
    bool _isDisposed{false};
    std::queue<mongo::Document> _docs;
    GetNextResult _endOfBufferSignal = GetNextResult::makePauseExecution();
};

}  // namespace streams
