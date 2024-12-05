/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "encryption_schema_tree.h"
#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/pipeline_metadata_tree.h"

namespace mongo {

/**
 * Represents a Pipeline which has been mutated based on an encryption schema tree to contain
 * intent-to-encrypt markings.
 */
class FLEPipeline {
public:
    using EncryptionSchemaTreeNodePtr = clonable_ptr<EncryptionSchemaTreeNode>;
    using CloneableEncryptionSchemaMap = std::map<NamespaceString, EncryptionSchemaTreeNodePtr>;

    // Constructor to be used only by commands that don't expect multiple schemas (i.e Find).
    FLEPipeline(std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
                const EncryptionSchemaTreeNode& schema);

    // Constructor to be used by commands which use multiple schemas (i.e Aggregate).
    FLEPipeline(std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
                EncryptionSchemaMap&& schemaMap);

    /**
     * Returns the schema of the document flowing *out* of the pipeline.
     */
    const EncryptionSchemaTreeNode& getOutputSchema() const {
        return *_finalSchema.get();
    }

    const Pipeline& getPipeline() const {
        return *_parsedPipeline.get();
    }

    void serialize(BSONArrayBuilder* arr) const {
        SerializationOptions opts{.serializeForQueryAnalysis = true};
        for (auto&& stage : _parsedPipeline->serialize(opts)) {
            invariant(stage.getType() == BSONType::Object);
            arr->append(stage.getDocument().toBson());
        }
    }

    void serializeLoneProject(BSONObjBuilder* bob) const {
        auto&& sources = _parsedPipeline->getSources();
        invariant(sources.size() == 1);
        auto&& loneSource = sources.front().get();
        invariant(typeid(*loneSource) == typeid(DocumentSourceSingleDocumentTransformation));
        bob->appendElements(static_cast<DocumentSourceSingleDocumentTransformation*>(loneSource)
                                ->getTransformer()
                                .serializeTransformation()
                                .toBson());
    }

    /**
     * Boolean to indicate whether any constants in the pipeline were replaced with their
     * intent-to-encrypt markings. The per-stage analyzers are responsible for setting this bit when
     * adding a placeholder.
     */
    bool hasEncryptedPlaceholders{false};

private:
    // Note, pipeline is an r-value reference here to accommodate the call site at the single schema
    // constructor, which uses the pipeline to obtain the namespace string in order to create the
    // CloneableEncryptionSchemaMap.
    FLEPipeline(CloneableEncryptionSchemaMap initialStageContents,
                std::unique_ptr<Pipeline, PipelineDeleter>&& pipeline);
    // Owned pipeline which may be modified if there are any constants that are marked for
    // encryption.
    std::unique_ptr<Pipeline, PipelineDeleter> _parsedPipeline;

    // Schema of the document flowing out of the pipeline, not associated with any Stage.
    EncryptionSchemaTreeNodePtr _finalSchema;
};

}  // namespace mongo
