/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "fle_pipeline.h"

#include "fle_match_expression.h"
#include "mongo/db/pipeline/document_source_coll_stats.h"
#include "mongo/db/pipeline/document_source_geo_near.h"
#include "mongo/db/pipeline/document_source_index_stats.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_lookup.h"
#include "mongo/db/pipeline/document_source_sample.h"
#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/db/pipeline/document_source_skip.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/db/pipeline/parsed_exclusion_projection.h"
#include "mongo/db/pipeline/parsed_inclusion_projection.h"
#include "mongo/db/pipeline/transformer_interface.h"

namespace mongo {

namespace {

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForInclusion(
    const EncryptionSchemaTreeNode& prevSchema,
    const parsed_aggregation_projection::ParsedInclusionProjection& includer) {
    const auto& root = includer.getRoot();
    std::set<std::string> preservedPaths;
    root.reportProjectedPaths(&preservedPaths);
    std::unique_ptr<EncryptionSchemaTreeNode> futureSchema =
        std::make_unique<EncryptionSchemaNotEncryptedNode>();
    // Each string is a projected, included path.
    for (auto& projection : preservedPaths) {
        FieldRef path(projection);
        if (auto includedNode = prevSchema.getNode(path)) {
            futureSchema->addChild(path, includedNode->clone());
        }
    }

    std::set<std::string> computedPaths;
    StringMap<std::string> renamedPaths;
    root.reportComputedPaths(&computedPaths, &renamedPaths);
    // TODO SERVER-40828: Propagate schema for computed paths.
    uassert(30037,
            "Cannot project with computed or renamed paths",
            computedPaths.size() == 0 && renamedPaths.size() == 0);

    return std::move(futureSchema);
}

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForExclusion(
    const EncryptionSchemaTreeNode& prevSchema,
    const parsed_aggregation_projection::ParsedExclusionProjection& excluder) {
    const auto& root = excluder.getRoot();
    std::set<std::string> removedPaths;
    root.reportProjectedPaths(&removedPaths);
    std::unique_ptr<EncryptionSchemaTreeNode> futureSchema = prevSchema.clone();
    // Each string is a projected, included path.
    for (auto& projection : removedPaths) {
        futureSchema->removeNode(FieldRef(projection));
    }

    std::set<std::string> computedPaths;
    StringMap<std::string> renamedPaths;
    root.reportComputedPaths(&computedPaths, &renamedPaths);
    invariant(computedPaths.size() == 0);
    invariant(renamedPaths.size() == 0);
    return std::move(futureSchema);
}

//
// DocumentSource schema propagation
//

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForGeoNear(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSourceGeoNear& source) {
    clonable_ptr<EncryptionSchemaTreeNode> newSchema = prevSchema->clone();

    // Mark projected paths as unencrypted.
    newSchema->addChild(FieldRef(source.getDistanceField().fullPath()),
                        std::make_unique<EncryptionSchemaNotEncryptedNode>());
    if (source.getLocationField()) {
        newSchema->addChild(FieldRef(source.getLocationField()->fullPath()),
                            std::make_unique<EncryptionSchemaNotEncryptedNode>());
    }
    return newSchema;
}

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForLookUp(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSourceLookUp& source) {
    uassert(51208,
            "Non-empty 'let' field is not allowed in the $lookup aggregation stage over an "
            "encrypted collection.",
            source.getLetVariables().empty());

    if (source.wasConstructedWithPipelineSyntax()) {
        invariant(children.size() == 1);
        // In order to support looking up encrypted data, encrypting arrays would need to be
        // supported.
        uassert(51205,
                str::stream() << "Looking up encrypted fields is not allowed. Consider restricting "
                                 "output of the subpipeline.",
                !children[0]->containsEncryptedNode());
    } else {
        invariant(source.getLocalField() && source.getForeignField());

        auto localField = source.getLocalField();
        FieldRef localRef(localField->fullPath());
        auto localMetadata = prevSchema->getEncryptionMetadataForPath(localRef);
        uassert(
            51206,
            str::stream() << "'localField' '" << localField->fullPath()
                          << "' in the $lookup aggregation stage cannot have an encrypted child.",
            localMetadata || !prevSchema->containsEncryptedNodeBelowPrefix(localRef));

        auto foreignField = source.getForeignField();
        FieldRef foreignRef(foreignField->fullPath());
        auto foreignMetadata = prevSchema->getEncryptionMetadataForPath(foreignRef);
        uassert(
            51207,
            str::stream() << "'foreignField' '" << foreignField->fullPath()
                          << "' in the $lookup aggregation stage cannot have an encrypted child.",
            foreignMetadata || !prevSchema->containsEncryptedNodeBelowPrefix(foreignRef));

        uassert(51210,
                str::stream() << "'localField' '" << localField->fullPath()
                              << " and 'foreignField' '"
                              << foreignField->fullPath()
                              << "' in the $lookup aggregation stage need to be both unencypted or "
                                 "be encrypted with the same bsonType.",
                (!localMetadata && !foreignMetadata) || localMetadata == foreignMetadata);
        uassert(51211,
                str::stream() << "'localField' '" << localField->fullPath()
                              << " and 'foreignField' '"
                              << foreignField->fullPath()
                              << "' in the $lookup aggregation stage need to be both encrypted "
                                 " the with deterministic algorithm.",
                (!localMetadata && !foreignMetadata) ||
                    localMetadata->algorithm == FleAlgorithmEnum::kDeterministic);
    }

    // Mark modified paths as unencrypted. We only expect a finite set of paths without renames.
    clonable_ptr<EncryptionSchemaTreeNode> newSchema = prevSchema->clone();
    const auto& modifiedPaths = source.getModifiedPaths();
    invariant(modifiedPaths.type == DocumentSource::GetModPathsReturn::Type::kFiniteSet);
    invariant(modifiedPaths.renames.empty());
    for (const auto& path : modifiedPaths.paths) {
        newSchema->addChild(FieldRef(path), std::make_unique<EncryptionSchemaNotEncryptedNode>());
    }
    return newSchema;
}

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaNoop(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSource& source) {
    return prevSchema->clone();
}

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaNoEncryption(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSource& source) {
    return clonable_ptr<EncryptionSchemaTreeNode>(
        std::make_unique<EncryptionSchemaNotEncryptedNode>());
}

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForProject(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSourceSingleDocumentTransformation& source) {
    const auto& transformer = source.getTransformer();
    switch (transformer.getType()) {
        case TransformerInterface::TransformerType::kInclusionProjection: {
            const auto& includer =
                static_cast<const parsed_aggregation_projection::ParsedInclusionProjection&>(
                    transformer);
            return propagateSchemaForInclusion(*prevSchema, includer);
        }
        case TransformerInterface::TransformerType::kExclusionProjection: {
            const auto& excluder =
                static_cast<const parsed_aggregation_projection::ParsedExclusionProjection&>(
                    transformer);
            return propagateSchemaForExclusion(*prevSchema, excluder);
        }
        case TransformerInterface::TransformerType::kComputedProjection:
        case TransformerInterface::TransformerType::kReplaceRoot:
        case TransformerInterface::TransformerType::kGroupFromFirstDocument:
            uasserted(ErrorCodes::CommandNotSupported, "Agg stage not yet supported");
    }
    MONGO_UNREACHABLE;
}

//
// DocumentSource encryption analysis
//

void analyzeStageNoop(FLEPipeline* flePipe,
                      const EncryptionSchemaTreeNode& schema,
                      DocumentSource* source) {}

void analyzeForMatch(FLEPipeline* flePipe,
                     const EncryptionSchemaTreeNode& schema,
                     DocumentSourceMatch* source) {
    // Build a FLEMatchExpression from the MatchExpression within the $match stage, replacing any
    // constants with their appropriate intent-to-encrypt markings.
    FLEMatchExpression fleMatch{source->getMatchExpression()->shallowClone(), schema};

    flePipe->hasEncryptedPlaceholders =
        flePipe->hasEncryptedPlaceholders || fleMatch.containsEncryptedPlaceholders();

    // Rebuild the DocumentSourceMatch using the serialized MatchExpression after replacing
    // encrypted values.
    source->rebuild([&]() {
        BSONObjBuilder bob;
        fleMatch.getMatchExpression()->serialize(&bob);
        return bob.obj();
    }());
}

void analyzeForGeoNear(FLEPipeline* flePipe,
                       const EncryptionSchemaTreeNode& schema,
                       DocumentSourceGeoNear* source) {
    // Build a FLEMatchExpression from the MatchExpression within the $geoNear stage, replacing any
    // constants with their appropriate intent-to-encrypt markings.
    auto queryExpression =
        uassertStatusOK(MatchExpressionParser::parse(source->getQuery(),
                                                     flePipe->getPipeline().getContext(),
                                                     ExtensionsCallbackNoop(),
                                                     Pipeline::kGeoNearMatcherFeatures));
    FLEMatchExpression fleMatch{std::move(queryExpression), schema};
    flePipe->hasEncryptedPlaceholders =
        flePipe->hasEncryptedPlaceholders || fleMatch.containsEncryptedPlaceholders();

    if (auto key = source->getKeyField()) {
        FieldRef keyField(key->fullPath());
        uassert(51212,
                str::stream() << "'key' field '" << key->fullPath()
                              << "' in the $geoNear aggregation stage cannot be encrypted.",
                !schema.getEncryptionMetadataForPath(keyField) &&
                    !schema.containsEncryptedNodeBelowPrefix(keyField));
    }

    // Update the query in the DocumentSourceGeoNear using the serialized MatchExpression
    // after replacing encrypted values.
    source->setQuery([&]() {
        BSONObjBuilder bob;
        fleMatch.getMatchExpression()->serialize(&bob);
        return bob.obj();
    }());
}

void analyzeForSort(FLEPipeline* flePipe,
                    const EncryptionSchemaTreeNode& schema,
                    DocumentSourceSort* source) {
    // Sort pattern cannot have encrypted fields. 'Expression' key parts are currently only used by
    // $meta sort, which does not involve encrypted fields.
    for (const auto& part : source->getSortKeyPattern()) {
        if (part.fieldPath) {
            // Note that positional path components will be handled correctly as they could only be
            // problematic if they refer to an array index. However, it is not allowed to have
            // arrays with encrypted paths.
            FieldRef keyField(part.fieldPath->fullPath());
            uassert(51201,
                    str::stream() << "Sorting on key '" << part.fieldPath->fullPath()
                                  << "' is not allowed due to encryption.",
                    !schema.getEncryptionMetadataForPath(keyField) &&
                        !schema.containsEncryptedNodeBelowPrefix(keyField));
        }
    }
}

// The 'schemaPropagatorMap' is a map of the typeid of a concrete DocumentSource class to the
// appropriate dispatch function for schema modification.
static stdx::unordered_map<
    std::type_index,
    std::function<clonable_ptr<EncryptionSchemaTreeNode>(
        const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
        const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& subPipelineSchemas,
        const DocumentSource& source)>>
    schemaPropagatorMap;

// The 'stageAnalyzerMap' is a map of the typeid of a concrete DocumentSource class to the
// appropriate dispatch function for encryption analysis.
static stdx::unordered_map<
    std::type_index,
    std::function<void(FLEPipeline*,
                       pipeline_metadata_tree::Stage<clonable_ptr<EncryptionSchemaTreeNode>>*,
                       DocumentSource*)>>
    stageAnalyzerMap;

#define REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(className, schemaFunc, analyzeFunc)             \
    MONGO_INITIALIZER(encryptedAnalyzerFor_##className)(InitializerContext*) {                \
        invariant(schemaPropagatorMap.find(typeid(className)) == schemaPropagatorMap.end());  \
        schemaPropagatorMap[typeid(className)] =                                              \
            [&](const auto& prevSchema, const auto& subPipelineSchemas, const auto& source) { \
                return schemaFunc(                                                            \
                    prevSchema, subPipelineSchemas, static_cast<const className&>(source));   \
            };                                                                                \
                                                                                              \
        invariant(stageAnalyzerMap.find(typeid(className)) == stageAnalyzerMap.end());        \
        stageAnalyzerMap[typeid(className)] = [&](auto* flePipe, auto* stage, auto* source) { \
            return analyzeFunc(flePipe, *stage->contents, static_cast<className*>(source));   \
        };                                                                                    \
        return Status::OK();                                                                  \
    }

// Whitelisted set of DocumentSource classes which are supported and/or require action for
// encryption with callbacks for schema propagation and encryption analysis, respectively.
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceCollStats,
                                      propagateSchemaNoEncryption,
                                      analyzeStageNoop);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceGeoNear,
                                      propagateSchemaForGeoNear,
                                      analyzeForGeoNear);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceIndexStats,
                                      propagateSchemaNoEncryption,
                                      analyzeStageNoop);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceLimit, propagateSchemaNoop, analyzeStageNoop);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceLookUp,
                                      propagateSchemaForLookUp,
                                      analyzeStageNoop);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceMatch, propagateSchemaNoop, analyzeForMatch);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceSample, propagateSchemaNoop, analyzeStageNoop);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceSkip, propagateSchemaNoop, analyzeStageNoop);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceSort, propagateSchemaNoop, analyzeForSort);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceSingleDocumentTransformation,
                                      propagateSchemaForProject,
                                      analyzeStageNoop);

}  // namespace

FLEPipeline::FLEPipeline(std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
                         const EncryptionSchemaTreeNode& schema)
    : _parsedPipeline{std::move(pipeline)} {
    // Method for propagating a schema from one stage to the next by dynamically dispatching based
    // on the runtime-type of 'source'. The 'prevSchema' represents the schema of the document
    // flowing into 'source', and the 'subPipelineSchemas' represent the input schemas of any
    // sub-pipelines for the given source.
    const auto& propagateSchemaFunction =
        [&](const auto& prevSchema, const auto& subPipelineSchemas, const auto& source) {
            uassert(31011,
                    str::stream() << "Aggregation stage " << source.getSourceName()
                                  << " is not allowed or supported with encryption.",
                    schemaPropagatorMap.find(typeid(source)) != schemaPropagatorMap.end());
            return schemaPropagatorMap[typeid(source)](prevSchema, subPipelineSchemas, source);
        };

    // Currently, drivers provide the schema only for the main collection, hence, sub-pipelines
    // cannot reference other collections.
    auto referencedCollections = _parsedPipeline->getInvolvedCollections();
    referencedCollections.insert(_parsedPipeline->getContext()->ns);
    uassert(51204,
            "Pipeline over an encrypted collection cannot reference additional collections.",
            referencedCollections.size() == 1);

    auto[metadataTree, finalSchema] =
        pipeline_metadata_tree::makeTree<clonable_ptr<EncryptionSchemaTreeNode>>(
            {{_parsedPipeline->getContext()->ns, schema.clone()}},
            *_parsedPipeline.get(),
            propagateSchemaFunction);

    _finalSchema = std::move(finalSchema);

    // If 'metadataTree' is not set, then this implies that the pipeline is empty and we can
    // return early here.
    if (!metadataTree)
        return;

    // Method for analyzing a DocumentSource alongside it's stage in the pipeline metadata tree.
    // Replaces any constants with intent-to-encrypt markings based on the schema held in
    // 'stage', or throws an assertion if 'source' contains an invalid expression/operation over
    // an encrypted field.
    const auto& stageAnalysisFunction = [&](auto* stage, auto* source) {
        // The assumption is that every stage which has a registered propagator, also has a
        // registered analyzer.
        invariant(stageAnalyzerMap.find(typeid(*source)) != stageAnalyzerMap.end());
        return stageAnalyzerMap[typeid(*source)](this, stage, source);
    };

    pipeline_metadata_tree::zip<clonable_ptr<EncryptionSchemaTreeNode>>(
        &metadataTree.get(), _parsedPipeline.get(), stageAnalysisFunction);
}

}  // namespace mongo
