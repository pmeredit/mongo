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
#include "mongo/db/pipeline/document_source_geo_near.h"
#include "mongo/db/pipeline/document_source_limit.h"

namespace mongo {

namespace {

//
// DocumentSource schema propagation
//

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaNoop(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSource& source) {
    return prevSchema->clone();
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
        uassert(51200,
                str::stream() << "Key field '" << key->fullPath()
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
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceGeoNear,
                                      propagateSchemaNoop,
                                      analyzeForGeoNear);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceMatch, propagateSchemaNoop, analyzeForMatch);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceLimit, propagateSchemaNoop, analyzeStageNoop);

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

    auto[metadataTree, finalSchema] =
        pipeline_metadata_tree::makeTree<clonable_ptr<EncryptionSchemaTreeNode>>(
            {schema.clone()}, *_parsedPipeline.get(), propagateSchemaFunction);

    _finalSchema = std::move(finalSchema);

    // If 'metadataTree' is not set, then this implies that the pipeline is empty and we can return
    // early here.
    if (!metadataTree)
        return;

    // Method for analyzing a DocumentSource alongside it's stage in the pipeline metadata tree.
    // Replaces any constants with intent-to-encrypt markings based on the schema held in 'stage',
    // or throws an assertion if 'source' contains an invalid expression/operation over an encrypted
    // field.
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
