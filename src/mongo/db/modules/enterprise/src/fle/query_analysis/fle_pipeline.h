/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "encryption_schema_tree.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/pipeline_metadata_tree.h"

namespace mongo {

/**
 * Represents a Pipeline which has been mutated based on an encryption schema tree to contain
 * intent-to-encrypt markings.
 */
class FLEPipeline {
public:
    FLEPipeline(std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
                const EncryptionSchemaTreeNode& schema);

    /**
     * Returns the schema of the document flowing *out* of the pipeline.
     */
    const EncryptionSchemaTreeNode& getOutputSchema() const {
        return *_finalSchema.get();
    }

    const Pipeline& getPipeline() const {
        return *_parsedPipeline.get();
    }

    void serialize(BSONArrayBuilder* arr) {
        for (auto&& stage : _parsedPipeline->serialize()) {
            invariant(stage.getType() == BSONType::Object);
            arr->append(stage.getDocument().toBson());
        }
    }

    /**
     * Boolean to indicate whether any constants in the pipeline were replaced with their
     * intent-to-encrypt markings. The per-stage analyzers are responsible for setting this bit when
     * adding a placeholder.
     */
    bool hasEncryptedPlaceholders{false};

private:
    // Owned pipeline which may be modified if there are any constants that are marked for
    // encryption.
    std::unique_ptr<Pipeline, PipelineDeleter> _parsedPipeline;

    // Schema of the document flowing out of the pipeline, not associated with any Stage.
    clonable_ptr<EncryptionSchemaTreeNode> _finalSchema;
};

}  // namespace mongo
