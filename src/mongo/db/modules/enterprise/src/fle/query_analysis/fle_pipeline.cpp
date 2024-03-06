/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <string>

#include "fle_pipeline.h"

#include "aggregate_expression_intender.h"
#include "fle_match_expression.h"
#include "mongo/db/exec/add_fields_projection_executor.h"
#include "mongo/db/exec/exclusion_projection_executor.h"
#include "mongo/db/exec/inclusion_projection_executor.h"
#include "mongo/db/pipeline/document_source_bucket_auto.h"
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
#include "mongo/db/pipeline/transformer_interface.h"

namespace mongo {

namespace {

using namespace std::string_literals;

/*
 * This function handles propagating the schema through an inclusion projection and an $addFields
 * stage. It takes in the schema before this stage, the inclusion to be performed, and the output
 * schema to append to. It returns the schema that is created by performing all the operations
 * contained in 'root' on the given 'futureSchema'.
 */
clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForInclusionNode(
    const EncryptionSchemaTreeNode& prevSchema,
    const projection_executor::InclusionNode& root,
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
            // Dotted path projections can implicitly traverse arrays to access fields of an object
            // within an array. For instance, if 'a' is an array in the incoming document, then
            // projecting the path 'a.b' will refer to the field 'b' within an object in the array
            // 'a'.
            //
            // For encryption, this means that we may end up with encrypted fields within an
            // array for dotted path projections, so mark the first path component with an
            // EncryptionSchemaStateMixedNode in the schema tree.
            if (expressionSchema->mayContainEncryptedNode() && fullPath.numParts() > 1) {
                futureSchema->addChild(FieldRef(fullPath[0]),
                                       std::make_unique<EncryptionSchemaStateMixedNode>());
            } else {
                // Output schema for the expression does not contain any encrypted fields OR the
                // projected field is not a dotted path.
                futureSchema->addChild(fullPath, std::move(expressionSchema));
            }
        }
    }
    for (const auto& [newName, oldName] : renamedPaths) {
        auto targetField = FieldRef{newName};
        if (auto oldEncryptionInfo = prevSchema.getNode(FieldRef{oldName})) {
            // Similar to the comment in computed projections above, if the target field is a dotted
            // path then we need to make sure not to have any encrypted nodes since it may reference
            // an array.
            if (oldEncryptionInfo->mayContainEncryptedNode() && targetField.numParts() > 1) {
                futureSchema->addChild(FieldRef(targetField[0]),
                                       std::make_unique<EncryptionSchemaStateMixedNode>());
            } else {
                // Output schema for the expression does not contain any encrypted fields OR the
                // projected field is not a dotted path.
                futureSchema->addChild(targetField, oldEncryptionInfo->clone());
            }
        }
    }

    return std::move(futureSchema);
}

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForExclusion(
    const EncryptionSchemaTreeNode& prevSchema, const projection_executor::ExclusionNode& root) {
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

void propagateAccumulatedFieldsToSchema(const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
                                        const std::vector<AccumulationStatement>& accumulatedFields,
                                        clonable_ptr<EncryptionSchemaTreeNode>& newSchema,
                                        bool groupKeyMayContainEncryptedFields) {
    for (const auto& accuStmt : accumulatedFields) {
        boost::intrusive_ptr<AccumulatorState> accu = accuStmt.makeAccumulator();

        const bool perDocExprResultCompared = accu->getOpName() == "$addToSet"s;
        auto perDocExprSchema = aggregate_expression_intender::getOutputSchema(
            *prevSchema, accuStmt.expr.argument.get(), perDocExprResultCompared);

        if (accu->getOpName() == "$addToSet"s || accu->getOpName() == "$push"s) {
            if (perDocExprSchema->mayContainEncryptedNode()) {
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
                                         "or whose encryption properties are not known until "
                                         "runtime when used in an $addToSet accumulator.",
                        !perDocExprSchema->mayContainRandomlyEncryptedNode());
            }
        } else if (accu->getOpName() == "$first"s || accu->getOpName() == "$last"s) {
            newSchema->addChild(FieldRef{accuStmt.fieldName}, std::move(perDocExprSchema));
        } else {
            uassert(51221,
                    str::stream() << "Accumulator '" << accu->getOpName()
                                  << "' cannot aggregate encrypted fields.",
                    !perDocExprSchema->mayContainEncryptedNode());
            // Similarly, we don't want to allow the initializer to contain encrypted fields.
            // For almost all accumulators, the initializer is a trivial {$const: null}.
            // Conservatively, just ban a non-$const initializer when the group key might
            // contain any encrypted data.
            if (groupKeyMayContainEncryptedFields) {
                uassert(51808,
                        str::stream() << "Accumulator '" << accu->getOpName()
                                      << "' must have a constant initializer (initArgs) "
                                      << "when the group key contains encrypted fields.",
                        ExpressionConstant::isNullOrConstant(
                            accuStmt.expr.initializer));
            }

            newSchema->addChild(FieldRef(accuStmt.fieldName),
                                std::make_unique<EncryptionSchemaNotEncryptedNode>());
        }
    }
}

//
// DocumentSource schema propagation
//

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForBucketAuto(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSourceBucketAuto& source) {
    clonable_ptr<EncryptionSchemaTreeNode> newSchema =
        std::make_unique<EncryptionSchemaNotEncryptedNode>();

    // Schema of the grouping expression cannot have encrypted nodes, because bucketization
    // expression uses inequality comparisons against the 'groupBy' field.
    const bool expressionResultCompared = true;
    auto expressionSchema = aggregate_expression_intender::getOutputSchema(
        *prevSchema, source.getGroupByExpression().get(), expressionResultCompared);
    uassert(51238,
            "'groupBy' expression cannot reference encrypted fields or their prefixes.",
            !expressionSchema->mayContainEncryptedNode());

    // Always project a not encrypted '_id' field.
    newSchema->addChild(FieldRef{"_id"}, std::make_unique<EncryptionSchemaNotEncryptedNode>());

    const bool groupKeyMayContainEncryptedFields = false;
    propagateAccumulatedFieldsToSchema(
        prevSchema, source.getAccumulatedFields(), newSchema, groupKeyMayContainEncryptedFields);
    return newSchema;
}

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

    bool groupKeyMayContainEncryptedFields = false;
    for (const auto& [pathStr, expression] : source.getIdFields()) {
        auto fieldPath = FieldRef{pathStr};
        // The expressions here are used for grouping things together, which is an equality
        // comparison.
        const bool expressionResultCompared = true;
        auto expressionSchema = aggregate_expression_intender::getOutputSchema(
            *prevSchema, expression.get(), expressionResultCompared);
        // This must be external to the usassert invocation to satisfy clang since it references a
        // structured binding.
        std::string errorMessage = str::stream()
            << "Cannot group on field '" << pathStr
            << "' which is encrypted with the random algorithm or whose encryption properties are "
               "not known until runtime";
        uassert(51222, errorMessage, !expressionSchema->mayContainRandomlyEncryptedNode());
        if (expressionSchema->mayContainEncryptedNode())
            groupKeyMayContainEncryptedFields = true;
        newSchema->addChild(fieldPath, std::move(expressionSchema));
    }

    propagateAccumulatedFieldsToSchema(
        prevSchema, source.getAccumulatedFields(), newSchema, groupKeyMayContainEncryptedFields);
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
        connectFromMetadata || !prevSchema->mayContainEncryptedNodeBelowPrefix(connectFromRef));

    auto connectToField = source.getConnectToField();
    FieldRef connectToRef(connectToField.fullPath());
    auto connectToMetadata = prevSchema->getEncryptionMetadataForPath(connectToRef);
    uassert(
        51231,
        str::stream() << "'connectToField' '" << connectToField.fullPath()
                      << "' in the $graphLookup aggregation stage cannot have an encrypted child.",
        connectToMetadata || !prevSchema->mayContainEncryptedNodeBelowPrefix(connectToRef));

    uassert(
        51232,
        str::stream() << "'connectFromField' '" << connectFromField.fullPath()
                      << "' and 'connectToField' '" << connectToField.fullPath()
                      << "' in the $graphLookup aggregation stage need to be both unencypted or "
                         "be encrypted with the same encryption properties.",
        (!connectFromMetadata && !connectToMetadata) || connectFromMetadata == connectToMetadata);
    uassert(51233,
            str::stream() << "'connectFromField' '" << connectFromField.fullPath()
                          << " and 'connectToField' '" << connectToField.fullPath()
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
        if (prevSchema->mayContainEncryptedNode()) {
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
            if (children[0]->mayContainEncryptedNode()) {
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
            localMetadata || !prevSchema->mayContainEncryptedNodeBelowPrefix(localRef));

        auto foreignField = source.getForeignField();
        FieldRef foreignRef(foreignField->fullPath());
        auto foreignMetadata = prevSchema->getEncryptionMetadataForPath(foreignRef);
        uassert(
            51207,
            str::stream() << "'foreignField' '" << foreignField->fullPath()
                          << "' in the $lookup aggregation stage cannot have an encrypted child.",
            foreignMetadata || !prevSchema->mayContainEncryptedNodeBelowPrefix(foreignRef));

        uassert(51210,
                str::stream() << "'localField' '" << localField->fullPath()
                              << " and 'foreignField' '" << foreignField->fullPath()
                              << "' in the $lookup aggregation stage need to be both unencypted or "
                                 "be encrypted with the same encryption properties.",
                (!localMetadata && !foreignMetadata) || localMetadata == foreignMetadata);
        uassert(51211,
                str::stream() << "'localField' '" << localField->fullPath()
                              << " and 'foreignField' '" << foreignField->fullPath()
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
                static_cast<const projection_executor::InclusionProjectionExecutor&>(transformer);
            return propagateSchemaForInclusionNode(
                *prevSchema,
                *includer.getRoot(),
                std::make_unique<EncryptionSchemaNotEncryptedNode>());
        }
        case TransformerInterface::TransformerType::kExclusionProjection: {
            const auto& excluder =
                static_cast<const projection_executor::ExclusionProjectionExecutor&>(transformer);
            return propagateSchemaForExclusion(*prevSchema, *excluder.getRoot());
        }
        case TransformerInterface::TransformerType::kComputedProjection: {
            const auto& projector =
                static_cast<const projection_executor::AddFieldsProjectionExecutor&>(transformer);
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
    const projection_executor::InclusionNode& root) {
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
                static_cast<const projection_executor::InclusionProjectionExecutor&>(transformer);
            return analyzeForInclusionNode(flePipe, schema, *includer.getRoot());
        }
        case TransformerInterface::TransformerType::kExclusionProjection: {
            return aggregate_expression_intender::Intention::NotMarked;
        }
        case TransformerInterface::TransformerType::kComputedProjection: {
            const auto& projector =
                static_cast<const projection_executor::AddFieldsProjectionExecutor&>(transformer);
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

aggregate_expression_intender::Intention analyzeForBucketAuto(
    FLEPipeline* flePipe,
    const EncryptionSchemaTreeNode& schema,
    DocumentSourceBucketAuto* source) {
    const bool expressionResultCompared = true;
    aggregate_expression_intender::Intention didMark =
        aggregate_expression_intender::mark(*(flePipe->getPipeline().getContext().get()),
                                            schema,
                                            source->getGroupByExpression().get(),
                                            expressionResultCompared);

    for (auto& accuStmt : source->getAccumulatedFields()) {
        // The expressions here are used for adding things to a set requires an equality
        // comparison.
        boost::intrusive_ptr<AccumulatorState> accu = accuStmt.makeAccumulator();
        const bool expressionResultCompared = accu->getOpName() == "$addToSet"s;
        didMark = didMark ||
            aggregate_expression_intender::mark(*(flePipe->getPipeline().getContext().get()),
                                                schema,
                                                accuStmt.expr.argument.get(),
                                                expressionResultCompared);
        // In bucketAuto we only allow constants for initializer (after optimization),
        // so we shouldn't need to analyze this.
        invariant(ExpressionConstant::isNullOrConstant(accuStmt.expr.initializer));
    }
    return didMark;
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
                    !schema.mayContainEncryptedNodeBelowPrefix(keyField));
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
    for (const auto& [fieldName, expression] : source->getIdFields()) {
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
        boost::intrusive_ptr<AccumulatorState> accu = accuStmt.makeAccumulator();
        const bool expressionResultCompared = accu->getOpName() == "$addToSet"s;
        didMark = didMark ||
            aggregate_expression_intender::mark(*(flePipe->getPipeline().getContext().get()),
                                                schema,
                                                accuStmt.expr.argument.get(),
                                                expressionResultCompared);

        // In propagateSchemaForGroup we require the initializer to be constant if the group
        // key might contain any encrypted fields, so here we shouldn't need to analyze it.
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
                        !schema.mayContainEncryptedNodeBelowPrefix(keyField));
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
REGISTER_DOCUMENT_SOURCE_FLE_ANALYZER(DocumentSourceBucketAuto,
                                      propagateSchemaForBucketAuto,
                                      analyzeForBucketAuto);
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
                                  << " is not allowed or supported with automatic encryption.",
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

    auto [metadataTree, finalSchema] =
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
