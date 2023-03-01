#pragma once

#include <queue>

#include "mongo/db/pipeline/document_source.h"

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

    boost::optional<DistributedPlanLogic> distributedPlanLogic() override {
        MONGO_UNREACHABLE;
    }

    mongo::StageConstraints constraints(mongo::Pipeline::SplitState pipeState) const override {
        MONGO_UNREACHABLE;
    }

    void addVariableRefs(std::set<mongo::Variables::Id>* refs) const override {
        MONGO_UNREACHABLE;
    }

    mongo::Value serialize(boost::optional<mongo::explain::VerbosityEnum> explain) const override {
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

protected:
    mongo::DocumentSource::GetNextResult doGetNext() override;

    void doDispose() override {
        _isDisposed = true;
    }

private:
    const mongo::DocumentSource::GetNextResult _pauseSignal;
    bool _isDisposed{false};
    std::queue<mongo::Document> _docs;
};

}  // namespace streams
