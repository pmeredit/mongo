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

#include <string>

#include "fle_pipeline.h"

#include "aggregate_expression_intender.h"
#include "fle_match_expression.h"
#include "mongo/db/pipeline/document_source_coll_stats.h"
#include "mongo/db/pipeline/document_source_geo_near.h"
#include "mongo/db/pipeline/document_source_graph_lookup.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/db/pipeline/document_source_index_stats.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_lookup.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/document_source_sample.h"
#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/db/pipeline/document_source_skip.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/db/pipeline/document_source_unwind.h"
#include "mongo/db/pipeline/parsed_add_fields.h"
#include "mongo/db/pipeline/parsed_exclusion_projection.h"
#include "mongo/db/pipeline/parsed_inclusion_projection.h"
#include "mongo/db/pipeline/transformer_interface.h"

namespace mongo {

namespace {

using namespace std::string_literals;

/*
 * This function handles propagating the schema through an inclusion projection and an $addFields
 * stage. It takes in the schema before this stage, the inclusion to be performed, and the output
 * schema to append to. It returns the schema that is
 * created by performing all the operations contained in 'root' on the given 'futureSchema'.
 */
clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForInclusionNode(
    const EncryptionSchemaTreeNode& prevSchema,
    const parsed_aggregation_projection::InclusionNode& root,
    std::unique_ptr<EncryptionSchemaTreeNode> futureSchema) {
    std::set<std::string> preservedPaths;
    root.reportProjectedPaths(&preservedPaths);
    // Each string is a projected, included path.
    for (const auto& projection : preservedPaths) {
        FieldRef path(projection);
        if (auto includedNode = prevSchema.getNode(path)) {
            futureSchema->addChild(path, includedNode->clone());
        }
    }

    std::set<std::string> computedPaths;
    StringMap<std::string> renamedPaths;
    root.reportComputedPaths(&computedPaths, &renamedPaths);
    for (const auto& path : computedPaths) {
        auto fullPath = FieldRef{path};
        if (auto expr = root.getExpressionForPath(FieldPath(path))) {
            auto expressionSchema =
                aggregate_expression_intender::getOutputSchema(prevSchema, expr.get(), false);
            // If the schema has no encrypted values, it doesn't matter if we insert it into the
            // tree.
            uassert(31140,
                    "Cannot assign encrypted value to dotted path " + fullPath.dottedField(0),
                    !expressionSchema->containsEncryptedNode() || fullPath.numParts() < 2);
            futureSchema->addChild(fullPath, std::move(expressionSchema));
        }
    }
    for (const auto & [ newName, oldName ] : renamedPaths) {
        auto targetField = FieldRef{newName};
        if (auto oldEncryptionInfo = prevSchema.getNode(FieldRef{oldName})) {
            // If the schema has no encrypted values, it doesn't matter if we insert it into the
            // tree.
            uassert(
                31139,
                "Cannot assign encrypted field or prefix of an encrypted field to dotted path " +
                    targetField.dottedField(0),
                !oldEncryptionInfo->containsEncryptedNode() || targetField.numParts() < 2);
            futureSchema->addChild(targetField, oldEncryptionInfo->clone());
        }
    }

    return std::move(futureSchema);
}

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForExclusion(
    const EncryptionSchemaTreeNode& prevSchema,
    const parsed_aggregation_projection::ExclusionNode& root) {
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

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForGroup(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSourceGroup& source) {
    clonable_ptr<EncryptionSchemaTreeNode> newSchema =
        std::make_unique<EncryptionSchemaNotEncryptedNode>();

    for (const auto & [ pathStr, expression ] : source.getIdFields()) {
        auto fieldPath = FieldRef{pathStr};
        // The expressions here are used for grouping things together, which is an equality
        // comparison.
        const bool expressionResultCompared = true;

        auto expressionSchema = aggregate_expression_intender::getOutputSchema(
            *prevSchema, expression.get(), expressionResultCompared);
        // This must be external to the usassert invocation to satisfy clang since it references a
        // structured binding.
        std::string errorMessage = str::stream()
            << "'" << pathStr << "' cannot have fields encrypted with the random algorithm when "
                                 "used for grouping.";
        uassert(51222, errorMessage, !expressionSchema->containsRandomlyEncryptedNode());
        newSchema->addChild(fieldPath, std::move(expressionSchema));
    }
    for (const auto& accuStmt : source.getAccumulatedFields()) {
        boost::intrusive_ptr<Accumulator> accu = accuStmt.makeAccumulator(source.getContext());
        const bool expressionResultCompared = accu->getOpName() == "$addToSet"s;
        auto expressionSchema = aggregate_expression_intender::getOutputSchema(
            *prevSchema, accuStmt.expression.get(), expressionResultCompared);

        if (accu->getOpName() == "$addToSet"s || accu->getOpName() == "$push"s) {
            if (expressionSchema->containsEncryptedNode()) {
                newSchema->addChild(FieldRef(accuStmt.fieldName),
                                    std::make_unique<EncryptionSchemaStateMixedNode>());
            } else {
                newSchema->addChild(FieldRef(accuStmt.fieldName),
                                    std::make_unique<EncryptionSchemaNotEncryptedNode>());
            }
            if (accu->getOpName() == "$addToSet"s) {
                uassert(51223,
                        str::stream() << "'" << accuStmt.fieldName
                                      << "' cannot have fields encrypted with the random algorithm "
                                         "when used in an $addToSet accumulator.",
                        !expressionSchema->containsRandomlyEncryptedNode());
            }
        } else if (accu->getOpName() == "$first"s || accu->getOpName() == "$last"s) {
            newSchema->addChild(FieldRef{accuStmt.fieldName}, std::move(expressionSchema));
        } else {
            uassert(51221,
                    str::stream() << "Accumulator '" << accu->getOpName()
                                  << "' cannot aggregate encrypted fields.",
                    !expressionSchema->containsEncryptedNode());
            newSchema->addChild(FieldRef(accuStmt.fieldName),
                                std::make_unique<EncryptionSchemaNotEncryptedNode>());
        }
    }
    return newSchema;
}

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForGraphLookUp(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSourceGraphLookUp& source) {
    auto connectFromField = source.getConnectFromField();
    FieldRef connectFromRef(connectFromField.fullPath());
    auto connectFromMetadata = prevSchema->getEncryptionMetadataForPath(connectFromRef);
    uassert(
        51230,
        str::stream() << "'connectFromField' '" << connectFromField.fullPath()
                      << "' in the $graphLookup aggregation stage cannot have an encrypted child.",
        connectFromMetadata || !prevSchema->containsEncryptedNodeBelowPrefix(connectFromRef));

    auto connectToField = source.getConnectToField();
    FieldRef connectToRef(connectToField.fullPath());
    auto connectToMetadata = prevSchema->getEncryptionMetadataForPath(connectToRef);
    uassert(
        51231,
        str::stream() << "'connectToField' '" << connectToField.fullPath()
                      << "' in the $graphLookup aggregation stage cannot have an encrypted child.",
        connectToMetadata || !prevSchema->containsEncryptedNodeBelowPrefix(connectToRef));

    uassert(
        51232,
        str::stream() << "'connectFromField' '" << connectFromField.fullPath()
                      << "' and 'connectToField' '"
                      << connectToField.fullPath()
                      << "' in the $graphLookup aggregation stage need to be both unencypted or "
                         "be encrypted with the same encryption properties.",
        (!connectFromMetadata && !connectToMetadata) || connectFromMetadata == connectToMetadata);
    uassert(51233,
            str::stream() << "'connectFromField' '" << connectFromField.fullPath()
                          << " and 'connectToField' '"
                          << connectToField.fullPath()
                          << "' in the $graphLookup aggregation stage need to be both encrypted "
                             " the with the deterministic algorithm.",
            (!connectFromMetadata && !connectToMetadata) ||
                connectFromMetadata->algorithm == FleAlgorithmEnum::kDeterministic);

    clonable_ptr<EncryptionSchemaTreeNode> newSchema = prevSchema->clone();
    // Mark modified paths with unknown encryption, which ensures an exception if a field is
    // referenced in a query. Also, we only expect a finite set of paths without renames.
    const auto& modifiedPaths = source.getModifiedPaths();
    invariant(modifiedPaths.type == DocumentSource::GetModPathsReturn::Type::kFiniteSet);
    invariant(modifiedPaths.renames.empty());
    for (const auto& path : modifiedPaths.paths) {
        if (prevSchema->containsEncryptedNode()) {
            newSchema->addChild(FieldRef(path), std::make_unique<EncryptionSchemaStateMixedNode>());
        } else {
            newSchema->addChild(FieldRef(path),
                                std::make_unique<EncryptionSchemaNotEncryptedNode>());
        }
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

    clonable_ptr<EncryptionSchemaTreeNode> newSchema = prevSchema->clone();
    const auto& modifiedPaths = source.getModifiedPaths();
    invariant(modifiedPaths.type == DocumentSource::GetModPathsReturn::Type::kFiniteSet);
    invariant(modifiedPaths.renames.empty());

    if (source.wasConstructedWithPipelineSyntax()) {
        // Mark modified paths with unknown encryption, which ensures an exception if a field is
        // referenced in a query. Also, we only expect a finite set of paths without renames.
        invariant(children.size() == 1);
        for (const auto& path : modifiedPaths.paths) {
            if (children[0]->containsEncryptedNode()) {
                newSchema->addChild(FieldRef(path),
                                    std::make_unique<EncryptionSchemaStateMixedNode>());
            } else {
                newSchema->addChild(FieldRef(path),
                                    std::make_unique<EncryptionSchemaNotEncryptedNode>());
            }
        }
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
                                 "be encrypted with the same encryption properties.",
                (!localMetadata && !foreignMetadata) || localMetadata == foreignMetadata);
        uassert(51211,
                str::stream() << "'localField' '" << localField->fullPath()
                              << " and 'foreignField' '"
                              << foreignField->fullPath()
                              << "' in the $lookup aggregation stage need to be both encrypted "
                                 " the with deterministic algorithm.",
                (!localMetadata && !foreignMetadata) ||
                    localMetadata->algorithm == FleAlgorithmEnum::kDeterministic);
        for (const auto& path : modifiedPaths.paths) {
            newSchema->addChild(FieldRef(path), std::make_unique<EncryptionSchemaStateMixedNode>());
        }
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

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForSingleDocumentTransformation(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSourceSingleDocumentTransformation& source) {
    const auto& transformer = source.getTransformer();
    switch (transformer.getType()) {
        case TransformerInterface::TransformerType::kInclusionProjection: {
            const auto& includer =
                static_cast<const parsed_aggregation_projection::ParsedInclusionProjection&>(
                    transformer);
            return propagateSchemaForInclusionNode(
                *prevSchema,
                includer.getRoot(),
                std::make_unique<EncryptionSchemaNotEncryptedNode>());
        }
        case TransformerInterface::TransformerType::kExclusionProjection: {
            const auto& excluder =
                static_cast<const parsed_aggregation_projection::ParsedExclusionProjection&>(
                    transformer);
            return propagateSchemaForExclusion(*prevSchema, excluder.getRoot());
        }
        case TransformerInterface::TransformerType::kComputedProjection: {
            const auto& projector =
                static_cast<const parsed_aggregation_projection::ParsedAddFields&>(transformer);
            return propagateSchemaForInclusionNode(
                *prevSchema, projector.getRoot(), prevSchema->clone());
        }
        case TransformerInterface::TransformerType::kReplaceRoot: {
            const auto& replaceRoot = static_cast<const ReplaceRootTransformation&>(transformer);
            auto outputSchema = aggregate_expression_intender::getOutputSchema(
                *prevSchema, replaceRoot.getExpression().get(), false);
            uassert(31159,
                    "$replaceRoot cannot have an encrypted field as root",
                    !outputSchema->getEncryptionMetadata());
            return std::move(outputSchema);
        }
        case TransformerInterface::TransformerType::kGroupFromFirstDocument:
            uasserted(ErrorCodes::CommandNotSupported, "Agg stage not yet supported");
    }
    MONGO_UNREACHABLE;
}

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForUnwind(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSourceUnwind& source) {

    const auto unwindPath = source.getUnwindPath();
    const auto unwindPathMetadata = prevSchema->getEncryptionMetadataForPath(FieldRef(unwindPath));

    uassert(31153,
            "$unwind is not allowed on a field which is encrypted with the randomized algorithm",
            !unwindPathMetadata || unwindPathMetadata->algorithm != FleAlgorithmEnum::kRandom);

    std::unique_ptr<EncryptionSchemaTreeNode> newSchema = prevSchema->clone();

    // If the $unwind has an "includeArrayIndex" path then we will overwrite any existing field on
    // the same path and can consider this path to be unencrypted.
    auto arrayIndexPath = source.indexPath();
    if (arrayIndexPath) {
        newSchema->addChild(FieldRef(arrayIndexPath->fullPath()),
                            std::make_unique<EncryptionSchemaNotEncryptedNode>());
    }

    return std::move(newSchema);
}

//
// DocumentSource encryption analysis
//

aggregate_expression_intender::Intention analyzeStageNoop(FLEPipeline* flePipe,
                                                          const EncryptionSchemaTreeNode& schema,
                                                          DocumentSource* source) {
    return aggregate_expression_intender::Intention::NotMarked;
}

aggregate_expression_intender::Intention analyzeForInclusionNode(
    FLEPipeline* flePipe,
    const EncryptionSchemaTreeNode& schema,
    const parsed_aggregation_projection::InclusionNode& root) {
    auto didMark = aggregate_expression_intender::Intention::NotMarked;
    std::set<std::string> computedPaths;
    StringMap<std::string> renamedPaths;
    root.reportComputedPaths(&computedPaths, &renamedPaths);
    for (const auto& path : computedPaths) {
        if (auto expr = root.getExpressionForPath(FieldPath(path))) {
            if (aggregate_expression_intender::mark(
                    *(flePipe->getPipeline().getContext().get()), schema, expr.get(), false) ==
                aggregate_expression_intender::Intention::Marked) {
                didMark = aggregate_expression_intender::Intention::Marked;
            }
        }
    }
    return didMark;
}

aggregate_expression_intender::Intention analyzeForSingleDocumentTransformation(
    FLEPipeline* flePipe,
    const EncryptionSchemaTreeNode& schema,
    DocumentSourceSingleDocumentTransformation* source) {
    const auto& transformer = source->getTransformer();
    switch (transformer.getType()) {
        case TransformerInterface::TransformerType::kInclusionProjection: {
            const auto& includer =
                static_cast<const parsed_aggregation_projection::ParsedInclusionProjection&>(
                    transformer);
            return analyzeForInclusionNode(flePipe, schema, includer.getRoot());
        }
        case TransformerInterface::TransformerType::kExclusionProjection: {
            return aggregate_expression_intender::Intention::NotMarked;
        }
        case TransformerInterface::TransformerType::kComputedProjection: {
            const auto& projector =
                static_cast<const parsed_aggregation_projection::ParsedAddFields&>(transformer);
            return analyzeForInclusionNode(flePipe, schema, projector.getRoot());
        }
        case TransformerInterface::TransformerType::kReplaceRoot: {
            const auto& replaceRoot = static_cast<const ReplaceRootTransformation&>(transformer);
            return aggregate_expression_intender::mark(*(flePipe->getPipeline().getContext().get()),
                                                       schema,
                                                       replaceRoot.getExpression().get(),
                                                       false);
        }
        case TransformerInterface::TransformerType::kGroupFromFirstDocument:
            uasserted(ErrorCodes::CommandNotSupported, "Agg stage not yet supported");
    }
    return aggregate_expression_intender::Intention::NotMarked;
}

aggregate_expression_intender::Intention analyzeForMatch(FLEPipeline* flePipe,
                                                         const EncryptionSchemaTreeNode& schema,
                                                         DocumentSourceMatch* source) {
    // Build a FLEMatchExpression from the MatchExpression within the $match stage, replacing any
    // constants with their appropriate intent-to-encrypt markings.
    FLEMatchExpression fleMatch{source->getMatchExpression()->shallowClone(), schema};

    // Rebuild the DocumentSourceMatch using the serialized MatchExpression after replacing
    // encrypted values.
    source->rebuild([&]() {
        BSONObjBuilder bob;
        fleMatch.getMatchExpression()->serialize(&bob);
        return bob.obj();
    }());
    if (fleMatch.containsEncryptedPlaceholders()) {
        return aggregate_expression_intender::Intention::Marked;
    } else {
        return aggregate_expression_intender::Intention::NotMarked;
    }
}

aggregate_expression_intender::Intention analyzeForGeoNear(FLEPipeline* flePipe,
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
    if (fleMatch.containsEncryptedPlaceholders()) {
        return aggregate_expression_intender::Intention::Marked;
    } else {
        return aggregate_expression_intender::Intention::NotMarked;
    }
}

aggregate_expression_intender::Intention analyzeForGraphLookUp(
    FLEPipeline* flePipe,
    const EncryptionSchemaTreeNode& schema,
    DocumentSourceGraphLookUp* source) {
    // Replace contants with their appropriate intent-to-encrypt markings in the 'startWith' field.
    auto didMark = aggregate_expression_intender::mark(
        *flePipe->getPipeline().getContext(), schema, source->getStartWithField(), false);

    // Build a FLEMatchExpression from the MatchExpression for the additional filter, replacing any
    // constants with their appropriate intent-to-encrypt markings.
    if (source->getAdditionalFilter()) {
        auto queryExpression =
            uassertStatusOK(MatchExpressionParser::parse(*source->getAdditionalFilter(),
                                                         flePipe->getPipeline().getContext(),
                                                         ExtensionsCallbackNoop(),
                                                         Pipeline::kAllowedMatcherFeatures));
        FLEMatchExpression fleMatch{std::move(queryExpression), schema};

        // Update the query in the DocumentSourceGraphLookUp using the serialized MatchExpression
        // after replacing encrypted values.
        source->setAdditionalFilter([&]() {
            BSONObjBuilder bob;
            fleMatch.getMatchExpression()->serialize(&bob);
            return bob.obj();
        }());
        if (fleMatch.containsEncryptedPlaceholders()) {
            didMark = aggregate_expression_intender::Intention::Marked;
        }
    }
    return didMark;
}

aggregate_expression_intender::Intention analyzeForGroup(FLEPipeline* flePipe,
                                                         const EncryptionSchemaTreeNode& schema,
                                                         DocumentSourceGroup* source) {
    aggregate_expression_intender::Intention didMark =
        aggregate_expression_intender::Intention::NotMarked;
    for (const auto & [ fieldName, expression ] : source->getIdFields()) {
        // The expressions here are used for grouping things together, which is an equality
        // comparison.
        const bool expressionResultCompared = true;
        didMark = didMark ||
            aggregate_expression_intender::mark(*(flePipe->getPipeline().getContext().get()),
                                                schema,
                                                expression.get(),
                                                expressionResultCompared);
    }
    for (auto& accuStmt : source->getAccumulatedFields()) {
        // The expressions here are used for adding things to a set requires an equality
        // comparison.
        boost::intrusive_ptr<Accumulator> accu = accuStmt.makeAccumulator(source->getContext());
        const bool expressionResultCompared = accu->getOpName() == "$addToSet"s;
        didMark = didMark ||
            aggregate_expression_intender::mark(*(flePipe->getPipeline().getContext().get()),
                                                schema,
                                                accuStmt.expression.get(),
                                                expressionResultCompared);
    }
    return didMark;
}

aggregate_expression_intender::Intention analyzeForSort(FLEPipeline* flePipe,
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
    return aggregate_expression_intender::Intention::NotMarked;
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
            aggregate_expression_intender::Intention markStatus =                             \
                analyzeFunc(flePipe, *stage->contents, static_cast<className*>(source));      \
            flePipe->hasEncryptedPlaceholders = flePipe->hasEncryptedPlaceholders ||          \
                markStatus == aggregate_expression_intender::Intention::Marked;               \
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
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceGraphLookUp,
                                      propagateSchemaForGraphLookUp,
                                      analyzeForGraphLookUp);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceGroup,
                                      propagateSchemaForGroup,
                                      analyzeForGroup);
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
                                      propagateSchemaForSingleDocumentTransformation,
                                      analyzeForSingleDocumentTransformation);
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceUnwind,
                                      propagateSchemaForUnwind,
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
