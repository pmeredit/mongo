/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/pipeline/pipeline.h"

#include "streams/exec/document_source_feeder.h"

namespace streams {

/**
 * This class is used to wrap managed pipelines to support explicitly feeding documents in and
 * getting the resulting document from the pipeline.
 */
class FeedablePipeline {
public:
    FeedablePipeline(mongo::PipelinePtr pipeline) : _pipeline{std::move(pipeline)} {
        _feeder = boost::intrusive_ptr<DocumentSourceFeeder>(
            new DocumentSourceFeeder(_pipeline->getContext()));
        _pipeline->addInitialSource(_feeder);
    }

    /**
     * Adds a document to the pipeline.
     */
    void addDocument(mongo::Document doc) {
        _feeder->addDocument(std::move(doc));
    }

    /**
     * Returns the next result from the pipeline, or boost::none if there are no more results.
     */
    boost::optional<mongo::Document> getNext() {
        return _pipeline->getNext();
    }

private:
    mongo::PipelinePtr _pipeline;
    boost::intrusive_ptr<DocumentSourceFeeder> _feeder;
};

}  // namespace streams
