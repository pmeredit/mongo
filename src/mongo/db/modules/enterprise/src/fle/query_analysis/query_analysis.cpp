/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "fle/query_analysis/resolved_encryption_info.h"
#include "mongo/db/auth/validated_tenancy_scope.h"
#include "mongo/platform/basic.h"

#include "query_analysis.h"

#include <cmath>
#include <limits>
#include <stack>

#include "encryption_schema_tree.h"
#include "encryption_update_visitor.h"
#include "fle_match_expression.h"
#include "fle_pipeline.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/crypto/encryption_fields_gen.h"
#include "mongo/crypto/encryption_fields_util.h"
#include "mongo/crypto/encryption_fields_validation.h"
#include "mongo/crypto/fle_crypto.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/db/basic_types_gen.h"
#include "mongo/db/coll_mod_gen.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/create_gen.h"
#include "mongo/db/commands/query_cmd/bulk_write_common.h"
#include "mongo/db/commands/query_cmd/bulk_write_crud_op.h"
#include "mongo/db/create_indexes_gen.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/matcher/expression_tree.h"
#include "mongo/db/matcher/expression_type.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/query/count_command_gen.h"
#include "mongo/db/query/distinct_command_gen.h"
#include "mongo/db/query/projection_parser.h"
#include "mongo/db/query/query_request_helper.h"
#include "mongo/db/query/write_ops/parsed_update_array_filters.h"
#include "mongo/db/query/write_ops/write_ops.h"
#include "mongo/db/query/write_ops/write_ops_gen.h"
#include "mongo/db/update/update_driver.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/util/assert_util.h"

namespace mongo::query_analysis {
namespace {

static constexpr auto kHasEncryptionPlaceholders = "hasEncryptionPlaceholders"_sd;
static constexpr auto kSchemaRequiresEncryption = "schemaRequiresEncryption"_sd;
static constexpr auto kResult = "result"_sd;
std::string typeSetToString(const MatcherTypeSet& typeSet) {
    StringBuilder sb;
    sb << "[ ";
    if (typeSet.allNumbers) {
        sb << "number ";
    }
    for (auto&& type : typeSet.bsonTypes) {
        sb << typeName(type) << " ";
    }
    sb << "]";
    return sb.str();
}

BSONObj extractEncryptionInformationSchemas(const NamespaceString ns,
                                            FleVersion& fleVersion,
                                            const BSONElement& e,
                                            const EncryptionInformation& encryptionInfo,
                                            BSONObjBuilder& stripped) {
    auto schemaSpec = encryptionInfo.getSchema().getOwned();
    fleVersion = FleVersion::kFle2;
    // Unlike FLE 1, 'encryptionInformation' should be retained in the command BSON as it
    // will be forwarded to the server.
    stripped.append(e);
    return schemaSpec;
}

template <typename T>
std::map<NamespaceString, T> extractSchemaMap(const NamespaceString& ns,
                                              const BSONObj& schemas,
                                              std::function<T(const BSONObj&)> func) {
    std::map<NamespaceString, T> schemaMap;
    for (const auto& elem : schemas) {
        uassert(6327504, "Each namespace schema must be an object", elem.type() == Object);
        schemaMap.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(NamespaceStringUtil::deserialize(
                boost::none, elem.fieldNameStringData(), SerializationContext::stateDefault())),
            std::forward_as_tuple(func(elem.Obj())));
    }
    uassert(9686712,
            "Namespace in encryption schemas: does not match namespace given in command: '" +
                ns.toStringForErrorMsg() + '\'',
            schemaMap.count(ns));
    return schemaMap;
}

EncryptionSchemaType isRemoteSchemaToEncryptionSchemaType(bool isRemoteSchema) {
    return isRemoteSchema ? EncryptionSchemaType::kRemote : EncryptionSchemaType::kLocal;
}

}  // namespace

QueryAnalysisParams::QueryAnalysisParams(const NamespaceString& ns,
                                         const BSONObj& jsonSchema,
                                         bool isRemoteSchema,
                                         BSONObj strippedObj)
    : schema(FLE1SchemaMap()), strippedObj(std::move(strippedObj)) {
    uassert(9686704, "NamespaceString containing tenant id is not supported", !ns.tenantId());
    auto& schemaMap = std::get<FLE1SchemaMap>(schema);
    schemaMap.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(ns),
        std::forward_as_tuple(jsonSchema, isRemoteSchemaToEncryptionSchemaType(isRemoteSchema)));
}

QueryAnalysisParams::QueryAnalysisParams(const NamespaceString& ns,
                                         const BSONObj& schemas,
                                         BSONObj strippedObj,
                                         FleVersion fleVersion,
                                         bool isMultiSchema)
    : schema(), strippedObj(std::move(strippedObj)) {
    uassert(9686700, "NamespaceString containing tenant id is not supported", !ns.tenantId());
    if (fleVersion == FleVersion::kFle1) {
        schema = extractSchemaMap<FLE1Params>(ns, schemas, [](const BSONObj& obj) {
            // EncryptionSchemaCSFLE::jsonSchema is already an owned BSONObj.
            auto schemaSpec =
                EncryptionSchemaCSFLE::parse(IDLParserContext("EncryptionSchemaCSFLE"), obj);
            return FLE1Params{schemaSpec.getJsonSchema(),
                              isRemoteSchemaToEncryptionSchemaType(schemaSpec.getIsRemoteSchema())};
        });
    } else {
        // We were parsing a single schema command, assert we have a single schema.
        if (!isMultiSchema) {
            uassert(6327503,
                    "Exactly one namespace is supported with encryptionInformation",
                    schemas.nFields() == 1);

            // We call serializeWithoutTenantPrefix_UNSAFE here because the encryptionInformation
            // document does not contain the tenantId. The tenantId is contained in the validated
            // tenancy scope of
            // the owning object.
            uassert(6411900,
                    "Namespace in encryptionInformation: '" +
                        schemas.firstElementFieldNameStringData() +
                        "' does not match namespace given in command: '" +
                        ns.toStringForErrorMsg() + '\'',
                    schemas.firstElementFieldNameStringData() ==
                        ns.serializeWithoutTenantPrefix_UNSAFE());
        }
        // EncryptionInformation::schemas is already an owned BSONObj.
        schema = extractSchemaMap<BSONObj>(
            ns, schemas, [](const BSONObj& obj) { return obj.getOwned(); });
    }
}

/**
 * Extracts and returns the 'QueryAnalysisPArams' in the command 'obj' by parsing the 'jsonSchema'
 * field (FLE 1), 'encryptionInformation' field (FLE 2) or 'csfleEncryptionSchemas' (CSFLE
 * SchemaMap)
 *
 * Throws an AssertionException if a required parameter is missing or if conflicting parameters are
 * given.
 */
QueryAnalysisParams extractCryptdParameters(const BSONObj& obj,
                                            const NamespaceString& ns,
                                            bool parseMultiSchema) {
    boost::optional<BSONObj> jsonSchema;
    boost::optional<bool> isRemoteSchema;
    boost::optional<BSONObj> csfleEncryptionSchemas;
    boost::optional<BSONObj> encryptionInformationSchemas;
    FleVersion fleVersion = FleVersion::kFle1;
    BSONObjBuilder stripped;
    for (auto& e : obj) {
        if (e.fieldNameStringData() == kJsonSchema) {
            uassert(51090, "jsonSchema is expected to be a object", e.type() == Object);
            jsonSchema = e.Obj();
        } else if (e.fieldNameStringData() == kIsRemoteSchema) {
            uassert(31102, "isRemoteSchema is expected to be a boolean", e.type() == Bool);
            isRemoteSchema = e.Bool();
        } else if (e.fieldNameStringData() == kEncryptionInformation) {
            uassert(6327501, "encryptionInformation must be an object", e.type() == Object);
            auto encryptionInformation =
                EncryptionInformation::parse(IDLParserContext("EncryptInformation"), e.Obj());
            encryptionInformationSchemas = extractEncryptionInformationSchemas(
                ns, fleVersion, e, encryptionInformation, stripped);
        } else if (e.fieldNameStringData() == "nsInfo") {
            // BulkWrite puts EncryptInformation below nsInfo.
            NamespaceInfoEntry nsInfoEntry = bulk_write_common::getFLENamespaceInfoEntry(obj);
            uassert(ErrorCodes::BadValue,
                    "BulkWrite with Queryable Encryption expects NamespaceInfoEntry to contain "
                    "encryptionInformation",
                    nsInfoEntry.getEncryptionInformation().has_value());
            encryptionInformationSchemas = extractEncryptionInformationSchemas(
                ns, fleVersion, e, nsInfoEntry.getEncryptionInformation().value(), stripped);
        } else if (e.fieldNameStringData() == kCsfleEncryptionSchemas) {
            uassert(9686713,
                    "csfleEncryptionSchemas is only supported for aggregate command.",
                    parseMultiSchema);
            uassert(9686702, "csfleEncryptionSchemas must be an object", e.type() == Object);
            csfleEncryptionSchemas = e.Obj();
            fleVersion = FleVersion::kFle1;
        } else {
            stripped.append(e);
        }
    }

    if (parseMultiSchema) {
        // We need to keep the parsing code for legacy style json schema/remote schema for
        // backwards compatibility.
        uassert(9686705,
                "Must specify either jsonSchema, csfleEncryptionSchemas or encryptionInformation",
                csfleEncryptionSchemas || encryptionInformationSchemas || jsonSchema);

        if (jsonSchema) {
            uassert(9686706, "isRemoteSchema is a required command field", isRemoteSchema);
            uassert(9686707,
                    "Cannot specify both jsonSchema and csfleEncryptionSchemas",
                    !csfleEncryptionSchemas);
            uassert(9686708,
                    "Cannot specify both jsonSchema and encryptionInformation",
                    !encryptionInformationSchemas);
            return QueryAnalysisParams(ns, *jsonSchema, *isRemoteSchema, stripped.obj());
        }

        uassert(9686709,
                "Cannot specify both isRemoteSchema and "
                "encryptionInformation/csfleEncryptionSchemas",
                !isRemoteSchema);

        uassert(9686710,
                "Cannot specify both encryptionInformation and csfleEncryptionSchemas",
                (csfleEncryptionSchemas && !encryptionInformationSchemas) ||
                    (!csfleEncryptionSchemas && encryptionInformationSchemas));
        return csfleEncryptionSchemas
            ? QueryAnalysisParams(
                  ns, *csfleEncryptionSchemas, stripped.obj(), fleVersion, parseMultiSchema)
            : QueryAnalysisParams(
                  ns, *encryptionInformationSchemas, stripped.obj(), fleVersion, parseMultiSchema);
    }

    uassert(51073,
            "jsonSchema or encryptionInformation is required",
            jsonSchema || encryptionInformationSchemas);
    uassert(31104,
            "isRemoteSchema is a required command field",
            isRemoteSchema || fleVersion == FleVersion::kFle2);

    uassert(6327500,
            "Cannot specify both jsonSchema and encryptionInformation",
            (jsonSchema && !encryptionInformationSchemas) ||
                (!jsonSchema && encryptionInformationSchemas));
    uassert(6327502,
            "Cannot specify both isRemoteSchema and encryptionInformation",
            (isRemoteSchema && !encryptionInformationSchemas) ||
                (!isRemoteSchema && encryptionInformationSchemas));

    return fleVersion == FleVersion::kFle2
        ? QueryAnalysisParams(
              ns, *encryptionInformationSchemas, stripped.obj(), fleVersion, parseMultiSchema)
        : QueryAnalysisParams(ns, *jsonSchema, *isRemoteSchema, stripped.obj());
}

namespace {

/**
 * If 'collation' is boost::none, returns nullptr. Otherwise, uses the collator factory
 * decorating 'opCtx' to produce a collator for 'collation'.
 */
std::unique_ptr<CollatorInterface> parseCollator(OperationContext* opCtx,
                                                 const boost::optional<BSONObj>& collation) {
    if (!collation) {
        return nullptr;
    }

    auto collatorFactory = CollatorFactoryInterface::get(opCtx->getServiceContext());
    return uassertStatusOK(collatorFactory->makeFromBSON(*collation));
}

/**
 * If the 'collation' field exists in 'command', extracts it and returns the corresponding
 * CollatorInterface pointer. Otherwise, returns nullptr.
 *
 * Throws a user assertion if the 'collation' field exists in 'command' but is not of BSON type
 * Object.
 */
std::unique_ptr<CollatorInterface> extractCollator(OperationContext* opCtx,
                                                   const BSONObj& command) {
    auto collationElt = command["collation"_sd];
    if (!collationElt) {
        return nullptr;
    }

    uassert(31084,
            "collation command parameter must be of type Object",
            collationElt.type() == BSONType::Object);
    return parseCollator(opCtx, collationElt.embeddedObject());
}

/**
 * Recursively descends through the doc curDoc. For each key in the doc, checks the schema and
 * replaces it with an encryption placeholder if neccessary. If origDoc is given it is passed to
 * buildEncryptPlaceholder() to resolve JSON Pointers. If it is not given, schemas with pointers
 * will error.
 *
 * Does not descend into arrays.


 */
BSONObj replaceEncryptedFieldsRecursive(const EncryptionSchemaTreeNode* schema,
                                        BSONObj curDoc,
                                        EncryptionPlaceholderContext placeholderContext,
                                        const boost::optional<BSONObj>& origDoc,
                                        const CollatorInterface* collator,
                                        FieldRef* leadingPath,
                                        bool* encryptedFieldFound) {
    BSONObjBuilder builder;
    for (auto&& element : curDoc) {
        auto fieldName = element.fieldNameStringData();
        leadingPath->appendPart(fieldName);
        if (auto metadata = schema->getEncryptionMetadataForPath(*leadingPath)) {
            *encryptedFieldFound = true;
            BSONObj placeholder = buildEncryptPlaceholder(
                element, metadata.value(), placeholderContext, collator, origDoc, *schema);
            builder.append(placeholder[fieldName]);
        } else if (element.type() == BSONType::Object) {
            builder.append(fieldName,
                           replaceEncryptedFieldsRecursive(schema,
                                                           element.embeddedObject(),
                                                           placeholderContext,
                                                           origDoc,
                                                           collator,
                                                           leadingPath,
                                                           encryptedFieldFound));
        } else if (element.type() == BSONType::Array) {
            // Encrypting beneath an array is not supported. If the user has an array along an
            // encrypted path, they have violated the type:"object" condition of the schema. For
            // example, if the user's schema indicates that "foo.bar" is encrypted, it is
            // implied that "foo" is an object. We should therefore return an error if the user
            // attempts to insert a document such as {foo: [{bar: 1}, {bar: 2}]}.
            //
            // Although mongocryptd doesn't enforce the provided JSON Schema in full, we make an
            // effort here to enforce the encryption-related aspects of the schema. Ensuring
            // that there are no arrays along an encrypted path falls within this mandate.
            uassert(31006,
                    str::stream() << "An array at path '" << leadingPath->dottedField()
                                  << "' would violate the schema",
                    !schema->mayContainEncryptedNodeBelowPrefix(*leadingPath));
            builder.append(element);
        } else {
            builder.append(element);
        }

        leadingPath->removeLastPart();
    }
    return builder.obj();
}

/**
 * Returns a new BSONObj that has all of the fields from 'response' that are also in 'original'
 * and always removes $db.
 */
BSONObj removeExtraFields(const std::set<StringData>& original, const BSONObj& response) {
    BSONObjBuilder bob;
    for (auto&& elem : response) {
        auto field = elem.fieldNameStringData();
        // "$db" is always removed because the drivers adds it to every message sent. If
        // we sent them in the reply, the drivers would wind up trying to add it again when
        // sending the query to mongod. In addition, some queries may not want to send $db at
        // all to a specific mongod version, so we remove it here instead of in the driver.
        if (field == "$db") {
            continue;
        }
        if (original.find(field) != original.end()) {
            bob.append(elem);
        }
    }
    return bob.obj();
}

/**
 * Parses the MatchExpression given by 'filter' and replaces encrypted fields with their
 * appropriate EncryptionPlaceholder according to the schema.
 *
 * Returns a PlaceHolderResult containing the new MatchExpression and a boolean to indicate
 * whether it contains an intent-to-encrypt marking. Throws an assertion if the MatchExpression
 * is invalid or contains an invalid operator over an encrypted field.
 *
 * Throws an assertion if 'filter' might require a collation-aware comparison and 'collator' is
 * non-null.
 */
PlaceHolderResult replaceEncryptedFieldsInFilter(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const EncryptionSchemaTreeNode& schemaTree,
    BSONObj filter) {
    // Build a parsed MatchExpression from the filter, allowing all special features.
    auto matchExpr = uassertStatusOK(MatchExpressionParser::parse(
        filter, expCtx, ExtensionsCallbackNoop(), MatchExpressionParser::kAllowAllSpecialFeatures));

    // Build a FLEMatchExpression, which will replace encrypted values with their appropriate
    // intent-to-encrypt markings.
    FLEMatchExpression fleMatchExpr(std::move(matchExpr), schemaTree, FLE2FieldRefExpr::allowed);
    auto serializedObj = fleMatchExpr.getMatchExpression()->serialize();

    // Replace the previous filter object with the new MatchExpression after marking it for
    // encryption.
    return {fleMatchExpr.containsEncryptedPlaceholders(),
            schemaTree.mayContainEncryptedNode(),
            fleMatchExpr.releaseMatchExpression(),
            serializedObj};
}

/**
 * Parses the update given by 'updateMod' and replaces encrypted fields with their appropriate
 * EncryptionPlaceholder according to the schema.
 *
 * Returns a PlaceHolderResult containing the new update object and a boolean to indicate
 * whether it contains an intent-to-encrypt marking. Throws an assertion if the update object is
 * invalid or contains an invalid operator over an encrypted field.
 */
PlaceHolderResult replaceEncryptedFieldsInUpdate(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const EncryptionSchemaTreeNode& schemaTree,
    const write_ops::UpdateModification& updateMod,
    const std::vector<mongo::BSONObj>& arrayFilters) {
    UpdateDriver driver(expCtx);
    // Although arrayFilters cannot contain encrypted fields, pass them through to the
    // UpdateDriver to prevent parsing errors for arrayFilters on a non-encrypted field path.
    auto parsedArrayFilters =
        uassertStatusOK(parsedUpdateArrayFilters(expCtx, arrayFilters, NamespaceString::kEmpty));
    driver.parse(updateMod, parsedArrayFilters);

    // 'updateVisitor' must live through driver serialization.
    auto updateVisitor = EncryptionUpdateVisitor(schemaTree);

    bool hasEncryptionPlaceholder = false;
    switch (driver.type()) {
        case UpdateDriver::UpdateType::kOperator:
            driver.visitRoot(&updateVisitor);
            hasEncryptionPlaceholder = updateVisitor.hasPlaceholder();
            break;
        case UpdateDriver::UpdateType::kReplacement: {
            auto updateExec = static_cast<ObjectReplaceExecutor*>(driver.getUpdateExecutor());
            // Replacement update need not respect the collation. It is legal to use replacement
            // update to create an encrypted string field, even if the update operation has a
            // non-simple collation.
            const CollatorInterface* collator = nullptr;
            auto placeholder = replaceEncryptedFields(updateExec->getReplacement(),
                                                      &schemaTree,
                                                      EncryptionPlaceholderContext::kWrite,
                                                      FieldRef{},
                                                      boost::none,
                                                      collator);
            if (placeholder.hasEncryptionPlaceholders) {
                updateExec->setReplacement(placeholder.result);
                hasEncryptionPlaceholder = true;
            }
            break;
        }
        case UpdateDriver::UpdateType::kPipeline: {
            // Build a FLEPipeline which will replace encrypted fields with intent-to-encrypt
            // markings.
            FLEPipeline flePipe{Pipeline::parse(updateMod.getUpdatePipeline(), expCtx), schemaTree};

            // The current pipeline analysis assumes that the document coming out of the
            // pipeline is being returned to the user but for pipelines in an update it is being
            // written to a collection. For instance, a simple {$addFields: {a: 5}} will never
            // mark 'a' for encryption as it's treated as a new field in the returned doc.
            //
            // However, in an update, the 5 may need to be marked for encryption if the
            // schema indicates that 'a' is encrypted. For now, assert that the pipeline
            // does not alter the schema and handle marking literals in examples like the
            // one above in SERVER-41485.
            switch (schemaTree.parsedFrom) {
                case FleVersion::kFle1:
                    uassert(31146,
                            "Pipelines in updates are not allowed to modify encrypted fields or "
                            "add fields which are not present in the schema",
                            flePipe.getOutputSchema() == schemaTree);
                    break;
                case FleVersion::kFle2:
                    uassert(6329902,
                            "Pipelines in updates are not allowed to modify encrypted fields or "
                            "add encrypted fields which are not present in the schema",
                            flePipe.getOutputSchema().isFle2LeafEquivalent(schemaTree));
                    break;
            }

            BSONArrayBuilder arr;
            flePipe.serialize(&arr);

            return PlaceHolderResult{flePipe.hasEncryptedPlaceholders,
                                     schemaTree.mayContainEncryptedNode(),
                                     nullptr,
                                     arr.obj()};
        }
        case UpdateDriver::UpdateType::kDelta:
            // Users cannot explicitly specify $v: 2 delta-style updates.
            MONGO_UNREACHABLE;
        case UpdateDriver::UpdateType::kTransform:
            // Users cannot explicitly specify transform-style updates.
            MONGO_UNREACHABLE;
    }

    return PlaceHolderResult{hasEncryptionPlaceholder,
                             schemaTree.mayContainEncryptedNode(),
                             nullptr,
                             driver.serialize().getDocument().toBson()};
}

PlaceHolderResult addPlaceHoldersForFind(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                         const DatabaseName& dbName,
                                         const BSONObj& cmdObj,
                                         std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    // Parse to a FindCommandRequest to ensure the command syntax is valid. We can use a
    // temporary database name however the collection name will be used when serializing back to
    // BSON.

    auto findCommand = query_request_helper::makeFromFindCommand(
        cmdObj,
        auth::ValidatedTenancyScope::get(expCtx->getOperationContext()),
        dbName.tenantId(),
        expCtx->getSerializationContext());

    auto filterPlaceholder =
        replaceEncryptedFieldsInFilter(expCtx, *schemaTree, findCommand->getFilter());

    // This is an optional wrapper around a pipeline containing just the projection from the
    // find command. With it we can reuse our pipeline analysis to determine if the projection
    // is invalid or in need of markers.
    const auto projectionPipeWrapper = [&] {
        if (auto proj = findCommand->getProjection(); !proj.isEmpty()) {
            return boost::optional<FLEPipeline>{FLEPipeline{
                Pipeline::create(makeFlattenedList<boost::intrusive_ptr<DocumentSource>>(
                                     DocumentSourceProject::create(
                                         projection_ast::parseAndAnalyze(
                                             expCtx,
                                             proj,
                                             filterPlaceholder.matchExpr.get(),
                                             filterPlaceholder.result,
                                             ProjectionPolicies::findProjectionPolicies()),
                                         expCtx,
                                         DocumentSourceProject::kStageName)),
                                 expCtx),
                *schemaTree.get()}};
        } else {
            return boost::optional<FLEPipeline>{};
        }
    }();

    BSONObjBuilder bob;
    for (auto&& elem : cmdObj) {
        if (elem.fieldNameStringData() == "filter") {
            BSONObjBuilder filterBob = bob.subobjStart("filter");
            filterBob.appendElements(filterPlaceholder.result);
        } else if (elem.fieldNameStringData() == "projection" && projectionPipeWrapper) {
            BSONObjBuilder projectionBob = bob.subobjStart("projection");
            projectionPipeWrapper->serializeLoneProject(&bob);
        } else {
            bob.append(elem);
        }
    }
    return {projectionPipeWrapper.map([](auto&& pipe) { return pipe.hasEncryptedPlaceholders; })
                    .value_or(false) ||
                filterPlaceholder.hasEncryptionPlaceholders,
            projectionPipeWrapper ? schemaTree->mayContainEncryptedNode()
                                  : filterPlaceholder.schemaRequiresEncryption,
            nullptr,
            bob.obj()};
}

template <typename T>
bool schemaMayContainEncryptedNode(const T& schema) {
    static_assert(std::is_same_v<T, EncryptionSchemaMap> ||
                  std::is_same_v<T, std::unique_ptr<EncryptionSchemaTreeNode>>);
    if constexpr (std::is_same_v<T, EncryptionSchemaMap>) {
        bool hasEncryptedSchema{false};
        // Return true if any of the encryption schemas may contain an encrypted node.
        for (const auto& nsAndSchema : schema) {
            invariant(nsAndSchema.second);
            hasEncryptedSchema |= nsAndSchema.second->mayContainEncryptedNode();
        }
        return hasEncryptedSchema;
    } else {
        invariant(schema);
        return schema->mayContainEncryptedNode();
    }
}

template <typename T>
FLEPipeline createFLEPipeline(std::unique_ptr<Pipeline, PipelineDeleter> pipeline, T&& schema) {
    static_assert(std::is_same_v<T, EncryptionSchemaMap> ||
                  std::is_same_v<T, std::unique_ptr<EncryptionSchemaTreeNode>>);
    if constexpr (std::is_same_v<T, EncryptionSchemaMap>) {
        return FLEPipeline{std::move(pipeline), std::move(schema)};
    } else {
        invariant(schema);
        return FLEPipeline{std::move(pipeline), *schema};
    }
}

template <typename T>
PlaceHolderResult addPlaceHoldersForAggregate(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                              const DatabaseName& dbName,
                                              const BSONObj& cmdObj,
                                              T schema) {

    // Parse the command to an AggregateCommandRequest to verify that there no unknown fields.
    auto request = aggregation_request_helper::parseFromBSON(
        cmdObj,
        auth::ValidatedTenancyScope::get(expCtx->getOperationContext()),
        boost::none,
        expCtx->getSerializationContext());

    const LiteParsedPipeline liteParsedPipeline(request);
    const auto& pipelineInvolvedNamespaces = liteParsedPipeline.getInvolvedNamespaces();

    // Add the populated list of involved namespaces to the expression context, needed at parse
    // time by stages such as $lookup and $out.
    expCtx->setNamespaceString(request.getNamespace());
    expCtx->setResolvedNamespaces([&]() {
        StringMap<ResolvedNamespace> resolvedNamespaces;
        for (auto&& involvedNs : pipelineInvolvedNamespaces) {
            resolvedNamespaces[involvedNs.coll()] = {involvedNs, std::vector<BSONObj>{}};
        }
        return resolvedNamespaces;
    }());

    // Ensure we have a schema for every namespace used in the pipeline.
    if constexpr (std::is_same_v<T, EncryptionSchemaMap>) {
        for (auto&& involvedNs : pipelineInvolvedNamespaces) {
            uassert(9710000,
                    "Missing encryption schema for namespace: " + involvedNs.toStringForErrorMsg(),
                    schema.count(involvedNs));
        }
    }

    const auto schemaMayBeEncrypted = schemaMayContainEncryptedNode(schema);
    // Build a FLEPipeline which will replace encrypted fields with intent-to-encrypt markings,
    // then update the AggregateCommandRequest with the new pipeline if there were any replaced
    // fields.
    auto flePipe =
        createFLEPipeline(Pipeline::parse(request.getPipeline(), expCtx), std::move(schema));

    // Serialize the translated command by manually appending each field that was present in the
    // original command, replacing the pipeline with the translated version containing
    // intent-to-encrypt markings.
    BSONObjBuilder bob;
    for (auto&& elem : cmdObj) {
        if (elem.fieldNameStringData() == "pipeline"_sd) {
            BSONArrayBuilder arr(bob.subarrayStart("pipeline"));
            flePipe.serialize(&arr);
        } else {
            bob.append(elem);
        }
    }

    return {flePipe.hasEncryptedPlaceholders, schemaMayBeEncrypted, nullptr, bob.obj()};
}

PlaceHolderResult addPlaceHoldersForCount(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                          const DatabaseName& dbName,
                                          const BSONObj& cmdObj,
                                          std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    BSONObjBuilder resultBuilder;

    auto countCmd = CountCommandRequest::parse(
        IDLParserContext("count",
                         auth::ValidatedTenancyScope::get(expCtx->getOperationContext()),
                         dbName.tenantId(),
                         SerializationContext::stateCommandRequest()),
        cmdObj);
    auto query = countCmd.getQuery();

    auto newQueryPlaceholder = replaceEncryptedFieldsInFilter(expCtx, *schemaTree, query);
    countCmd.setQuery(newQueryPlaceholder.result);

    return PlaceHolderResult{newQueryPlaceholder.hasEncryptionPlaceholders,
                             newQueryPlaceholder.schemaRequiresEncryption ||
                                 schemaTree->mayContainEncryptedNode(),
                             nullptr,
                             countCmd.toBSON()};
}

PlaceHolderResult addPlaceHoldersForDistinct(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                             const DatabaseName& dbName,
                                             const BSONObj& cmdObj,
                                             std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto parsedDistinct = DistinctCommandRequest::parse(
        IDLParserContext("distinct",
                         auth::ValidatedTenancyScope::get(expCtx->getOperationContext()),
                         dbName.tenantId(),
                         SerializationContext::stateCommandRequest()),
        cmdObj);

    if (auto keyMetadata =
            schemaTree->getEncryptionMetadataForPath(FieldRef(parsedDistinct.getKey()))) {
        uassert(51131,
                "The distinct key is not allowed to be marked for encryption with a non-UUID keyId",
                keyMetadata->keyId.type() != EncryptSchemaKeyId::Type::kJSONPointer);
        uassert(31026,
                "Distinct key is not allowed to be marked for encryption with the randomized "
                "encryption algorithm",
                keyMetadata->algorithmIs(FleAlgorithmEnum::kDeterministic));

        // Raise an error if the non-simple collation has been specified, but only do it if the
        // schema indicates that the field type is a string.
        if (expCtx->getCollator()) {
            invariant(keyMetadata->bsonTypeSet);
            invariant(keyMetadata->bsonTypeSet->isSingleType());
            // We've already checked while constructing 'keyMetadata' that a deterministically
            // encrypted field must have exactly one specified type, so we'll check just for the
            // field type here.
            uassert(31058,
                    "Distinct key cannot be an encrypted string field if the collation is "
                    "non-simple",
                    !keyMetadata->bsonTypeSet->hasType(BSONType::String));
        }
    } else {
        uassert(31027,
                "Distinct key is not allowed to be a prefix of an encrypted field",
                !schemaTree->mayContainEncryptedNodeBelowPrefix(FieldRef(parsedDistinct.getKey())));
    }

    PlaceHolderResult placeholder;
    if (auto query = parsedDistinct.getQuery()) {
        // Replace any encrypted fields in the query, and overwrite the original query from the
        // parsed command.
        placeholder =
            replaceEncryptedFieldsInFilter(expCtx, *schemaTree, parsedDistinct.getQuery().value());
        parsedDistinct.setQuery(placeholder.result);
    }

    return PlaceHolderResult{placeholder.hasEncryptionPlaceholders,
                             schemaTree->mayContainEncryptedNode(),
                             nullptr,
                             parsedDistinct.serialize().body};
}

/**
 * Asserts that if _id is encrypted, it is provided explicitly. In addition, asserts
 * that no top level field in 'doc' has the value Timestamp(0,0). In both of those cases mongod
 * would usually generate a value so they cannot be encrypted here.
 */
void verifyNoGeneratedEncryptedFields(BSONObj doc, const EncryptionSchemaTreeNode& schemaTree) {
    uassert(51130,
            "_id must be explicitly provided when configured as encrypted",
            !schemaTree.getEncryptionMetadataForPath(FieldRef("_id")) || doc["_id"]);
    // Top level Timestamp(0, 0) is not allowed to be inserted because the server replaces it
    // with a different generated value. A nested Timestamp(0,0) does not have this issue.
    for (auto&& element : doc) {
        if (schemaTree.getEncryptionMetadataForPath(FieldRef(element.fieldNameStringData()))) {
            uassert(51129,
                    "A command that inserts cannot supply Timestamp(0, 0) for an encrypted "
                    "top-level field at path " +
                        element.fieldNameStringData(),
                    element.type() != BSONType::bsonTimestamp ||
                        element.timestamp() != Timestamp(0, 0));
        }
    }
}

PlaceHolderResult addPlaceHoldersForFindAndModify(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const DatabaseName& dbName,
    const BSONObj& cmdObj,
    std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {

    auto request(write_ops::FindAndModifyCommandRequest::parse(
        IDLParserContext("findAndModify",
                         auth::ValidatedTenancyScope::get(expCtx->getOperationContext()),
                         dbName.tenantId(),
                         SerializationContext::stateCommandRequest()),
        cmdObj));

    bool anythingEncrypted = false;
    if (auto updateMod = request.getUpdate()) {
        uassert(31151,
                "Pipelines in findAndModify are not allowed with an encrypted '_id' and "
                "'upsert: true'",
                !(updateMod->type() == write_ops::UpdateModification::Type::kPipeline &&
                  schemaTree->getEncryptionMetadataForPath(FieldRef("_id")) &&
                  request.getUpsert().value_or(false)));

        if (request.getUpsert().value_or(false) &&
            (updateMod->type() == write_ops::UpdateModification::Type::kReplacement ||
             updateMod->type() == write_ops::UpdateModification::Type::kModifier)) {
            auto updateObj = updateMod->type() == write_ops::UpdateModification::Type::kReplacement
                ? updateMod->getUpdateReplacement()
                : updateMod->getUpdateModifier();
            verifyNoGeneratedEncryptedFields(updateObj, *schemaTree.get());
        }

        auto newUpdate = replaceEncryptedFieldsInUpdate(
            expCtx,
            *schemaTree.get(),
            updateMod.value(),
            request.getArrayFilters().value_or(std::vector<BSONObj>()));
        request.setUpdate(
            write_ops::UpdateModification::parseFromClassicUpdate(newUpdate.result.getOwned()));
        anythingEncrypted = newUpdate.hasEncryptionPlaceholders;
    }

    auto newQuery = replaceEncryptedFieldsInFilter(expCtx, *schemaTree.get(), request.getQuery());
    if (newQuery.hasEncryptionPlaceholders) {
        request.setQuery(newQuery.result);
        anythingEncrypted = true;
    }

    return PlaceHolderResult{
        anythingEncrypted, schemaTree->mayContainEncryptedNode(), nullptr, request.toBSON()};
}

/**
 * Shared logic for addPlaceHoldersForUpdate and addPlaceHoldersForBulkWrite replacement of fields
 * in the filter and update.
 */
std::pair<PlaceHolderResult, PlaceHolderResult> addPlaceHoldersForUpdateHelper(
    OperationContext* opCtx,
    NamespaceString ns,
    bool multi,
    bool upsert,
    const mongo::BSONObj& query,
    const mongo::write_ops::UpdateModification& updateMod,
    const boost::optional<mongo::BSONObj>& collation,
    const std::vector<BSONObj>& arrayFilters,
    EncryptionSchemaTreeNode* schemaTree) {
    uassert(6329900,
            "Multi-document updates are not allowed with Queryable Encryption",
            !(multi && schemaTree->parsedFrom == FleVersion::kFle2));

    auto collator = parseCollator(opCtx, collation);
    auto expCtx =
        ExpressionContextBuilder{}.opCtx(opCtx).collator(std::move(collator)).ns(ns).build();

    uassert(31150,
            "Pipelines in update are not allowed with an encrypted '_id' and 'upsert: true'",
            !(updateMod.type() == write_ops::UpdateModification::Type::kPipeline &&
              schemaTree->getEncryptionMetadataForPath(FieldRef("_id")) && upsert));

    if (upsert &&
        (updateMod.type() == write_ops::UpdateModification::Type::kReplacement ||
         updateMod.type() == write_ops::UpdateModification::Type::kModifier)) {
        auto updateObj = updateMod.type() == write_ops::UpdateModification::Type::kReplacement
            ? updateMod.getUpdateReplacement()
            : updateMod.getUpdateModifier();
        verifyNoGeneratedEncryptedFields(updateObj, *schemaTree);
    }

    auto newFilter = replaceEncryptedFieldsInFilter(expCtx, *schemaTree, query);
    auto newUpdate = replaceEncryptedFieldsInUpdate(expCtx, *schemaTree, updateMod, arrayFilters);

    return std::pair<PlaceHolderResult, PlaceHolderResult>{std::move(newFilter),
                                                           std::move(newUpdate)};
}

PlaceHolderResult addPlaceHoldersForBulkWrite(
    OperationContext* opCtx,
    const OpMsgRequest& request,
    std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    BulkWriteCommandRequest bulk =
        BulkWriteCommandRequest::parse(IDLParserContext("bulkWrite"), request);

    // getOps returns a const& so setOps is used below to overwrite bulk.ops with this local copy.
    auto ops = bulk.getOps();
    PlaceHolderResult retPlaceholder;
    for (auto& opVariant : ops) {
        BulkWriteCRUDOp op(opVariant);
        auto opType = op.getType();

        if (opType == BulkWriteCRUDOp::kInsert) {
            // This makes a copy, it is assigned back to opVariant below.
            BulkWriteInsertOp insertOp = *op.getInsert();

            const BSONObj& doc = insertOp.getDocument();
            verifyNoGeneratedEncryptedFields(doc, *schemaTree.get());
            // The insert command cannot currently perform any collation-aware comparisons, and
            // therefore does not accept a collation argument.
            const CollatorInterface* collator = nullptr;
            auto placeholderPair = replaceEncryptedFields(doc,
                                                          schemaTree.get(),
                                                          EncryptionPlaceholderContext::kWrite,
                                                          FieldRef(),
                                                          doc,
                                                          collator);
            retPlaceholder.hasEncryptionPlaceholders |= placeholderPair.hasEncryptionPlaceholders;
            insertOp.setDocument(placeholderPair.result);
            opVariant = insertOp;
        } else {
            uassert(ErrorCodes::BadValue,
                    "Only insert is supported in BulkWrite with multiple operations and Queryable "
                    "Encryption.",
                    ops.size() == 1);

            if (opType == BulkWriteCRUDOp::kUpdate) {
                // This makes a copy, it is assigned back to opVariant below.
                BulkWriteUpdateOp updateOp = *op.getUpdate();
                uassert(ErrorCodes::BadValue,
                        "BulkWrite with Queryable Encryption supports only a single namespace",
                        updateOp.getUpdate() == 0 && bulk.getNsInfo().size() == 1);
                auto [newFilter, newUpdate] =
                    addPlaceHoldersForUpdateHelper(opCtx,
                                                   bulk.getNsInfo()[updateOp.getUpdate()].getNs(),
                                                   updateOp.getMulti(),
                                                   updateOp.getUpsert(),
                                                   updateOp.getFilter(),
                                                   updateOp.getUpdateMods(),
                                                   updateOp.getCollation(),
                                                   write_ops::arrayFiltersOf(updateOp),
                                                   schemaTree.get());
                updateOp.setFilter(newFilter.result);
                updateOp.setUpdateMods(
                    write_ops::UpdateModification::parseFromClassicUpdate(newUpdate.result));
                retPlaceholder.hasEncryptionPlaceholders |= newFilter.hasEncryptionPlaceholders;
                opVariant = updateOp;
            } else {
                // This makes a copy, it is assigned back to opVariant below.
                BulkWriteDeleteOp deleteOp = *op.getDelete();
                uassert(ErrorCodes::BadValue,
                        "BulkWrite with Queryable Encryption supports only a single namespace",
                        deleteOp.getDeleteCommand() == 0 && bulk.getNsInfo().size() == 1);

                auto collator = parseCollator(opCtx, deleteOp.getCollation());
                auto expCtx =
                    ExpressionContextBuilder{}
                        .opCtx(opCtx)
                        .collator(std::move(collator))
                        .ns(NamespaceString(bulk.getNsInfo()[deleteOp.getDeleteCommand()].getNs()))
                        .build();
                auto newFilter =
                    replaceEncryptedFieldsInFilter(expCtx, *schemaTree, deleteOp.getFilter());
                deleteOp.setFilter(newFilter.result);
                retPlaceholder.hasEncryptionPlaceholders |= newFilter.hasEncryptionPlaceholders;
                opVariant = deleteOp;
            }
        }
    }
    bulk.setOps(ops);

    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    retPlaceholder.result = removeExtraFields(fieldNames, bulk.toBSON());
    retPlaceholder.schemaRequiresEncryption = schemaTree->mayContainEncryptedNode();
    return retPlaceholder;
}

PlaceHolderResult addPlaceHoldersForInsert(OperationContext* opCtx,
                                           const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto batch = InsertOp::parse(request);
    auto docs = batch.getDocuments();
    PlaceHolderResult retPlaceholder;
    std::vector<BSONObj> docVector;
    for (const BSONObj& doc : docs) {
        verifyNoGeneratedEncryptedFields(doc, *schemaTree.get());
        // The insert command cannot currently perform any collation-aware comparisons, and
        // therefore does not accept a collation argument.
        const CollatorInterface* collator = nullptr;
        auto placeholderPair = replaceEncryptedFields(
            doc, schemaTree.get(), EncryptionPlaceholderContext::kWrite, FieldRef(), doc, collator);
        retPlaceholder.hasEncryptionPlaceholders =
            retPlaceholder.hasEncryptionPlaceholders || placeholderPair.hasEncryptionPlaceholders;
        docVector.push_back(placeholderPair.result);
    }
    batch.setDocuments(docVector);
    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    fieldNames.insert("documents"_sd);
    retPlaceholder.result = removeExtraFields(fieldNames, batch.toBSON());
    retPlaceholder.schemaRequiresEncryption = schemaTree->mayContainEncryptedNode();
    return retPlaceholder;
}

PlaceHolderResult addPlaceHoldersForUpdate(OperationContext* opCtx,
                                           const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto updateOp = UpdateOp::parse(request);
    auto updates = updateOp.getUpdates();
    std::vector<write_ops::UpdateOpEntry> updateVector;
    PlaceHolderResult phr;

    for (auto&& update : updateOp.getUpdates()) {
        auto [newFilter, newUpdate] =
            addPlaceHoldersForUpdateHelper(opCtx,
                                           NamespaceString(request.parseDbName()),
                                           update.getMulti(),
                                           update.getUpsert(),
                                           update.getQ(),
                                           update.getU(),
                                           update.getCollation(),
                                           write_ops::arrayFiltersOf(update),
                                           schemaTree.get());

        // Create a non-const copy.
        auto newEntry = update;
        newEntry.setQ(newFilter.result);
        newEntry.setU(write_ops::UpdateModification::parseFromClassicUpdate(newUpdate.result));
        updateVector.push_back(newEntry);
        phr.hasEncryptionPlaceholders = phr.hasEncryptionPlaceholders ||
            newUpdate.hasEncryptionPlaceholders || newFilter.hasEncryptionPlaceholders;
    }

    updateOp.setUpdates(updateVector);
    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    fieldNames.insert("updates"_sd);
    phr.result = removeExtraFields(fieldNames, updateOp.toBSON());
    phr.schemaRequiresEncryption = schemaTree->mayContainEncryptedNode();
    return phr;
}

PlaceHolderResult addPlaceHoldersForDelete(OperationContext* opCtx,
                                           const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    invariant(schemaTree);
    PlaceHolderResult placeHolderResult{};

    auto deleteRequest =
        write_ops::DeleteCommandRequest::parse(IDLParserContext("delete"), request);
    std::vector<write_ops::DeleteOpEntry> markedDeletes;
    for (auto&& op : deleteRequest.getDeletes()) {
        markedDeletes.push_back(op);
        auto& opToMark = markedDeletes.back();
        auto collator = parseCollator(opCtx, op.getCollation());
        auto expCtx = ExpressionContextBuilder{}
                          .opCtx(opCtx)
                          .collator(std::move(collator))
                          .ns(NamespaceString(request.parseDbName()))
                          .build();
        auto resultForOp = replaceEncryptedFieldsInFilter(expCtx, *schemaTree, opToMark.getQ());
        placeHolderResult.hasEncryptionPlaceholders =
            placeHolderResult.hasEncryptionPlaceholders || resultForOp.hasEncryptionPlaceholders;
        opToMark.setQ(resultForOp.result);
    }

    deleteRequest.setDeletes(std::move(markedDeletes));
    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    fieldNames.insert("deletes"_sd);
    placeHolderResult.result = removeExtraFields(fieldNames, deleteRequest.toBSON());
    placeHolderResult.schemaRequiresEncryption = schemaTree->mayContainEncryptedNode();
    return placeHolderResult;
}

OpMsgRequest makeHybrid(const OpMsgRequest& request, BSONObj body) {
    OpMsgRequest newRequest;
    newRequest.body = body;
    newRequest.sequences = request.sequences;
    newRequest.validatedTenancyScope = request.validatedTenancyScope;
    return newRequest;
}

using WriteOpProcessFunction =
    PlaceHolderResult(OperationContext* opCtx,
                      const OpMsgRequest& request,
                      std::unique_ptr<EncryptionSchemaTreeNode> schemaTree);

void processWriteOpCommand(OperationContext* opCtx,
                           const OpMsgRequest& request,
                           BSONObjBuilder* builder,
                           WriteOpProcessFunction func,
                           const NamespaceString ns) {
    auto cryptdParams = extractCryptdParameters(request.body, ns, false /* parseMultiSchema*/);
    auto newRequest = makeHybrid(request, cryptdParams.strippedObj);

    // Parse the JSON Schema to an encryption schema tree.
    auto schemaTree =
        EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(cryptdParams);

    PlaceHolderResult placeholder = func(opCtx, newRequest, std::move(schemaTree));

    serializePlaceholderResult(placeholder, builder);
}

template <typename T>
using QueryProcessFunction =
    PlaceHolderResult(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                      const DatabaseName& dbName,
                      const BSONObj& cmdObj,
                      T schemaTree);

template <typename T>
void processQueryCommand(OperationContext* opCtx,
                         const DatabaseName& dbName,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* builder,
                         QueryProcessFunction<T> func,
                         const NamespaceString ns) {
    auto cryptdParams = extractCryptdParameters(
        cmdObj, ns, std::is_same_v<T, EncryptionSchemaMap> /*parseMultiSchema*/);
    // Parse the JSON Schema to an encryption schema tree.
    auto schema = EncryptionSchemaTreeNode::parse<T>(cryptdParams);

    auto collator = extractCollator(opCtx, cmdObj);
    auto expCtx = ExpressionContextBuilder{}
                      .opCtx(opCtx)
                      .collator(std::move(collator))
                      .ns(NamespaceString(dbName))
                      .build();
    const auto vts = auth::ValidatedTenancyScope::get(opCtx);
    // assume that the tenantId on dbName wasn't from a prefix
    expCtx->setSerializationContext(
        vts != boost::none
            ? SerializationContext::stateCommandRequest(vts->hasTenantId(), vts->isFromAtlasProxy())
            : SerializationContext::stateCommandRequest());

    PlaceHolderResult placeholder =
        func(expCtx, dbName, cryptdParams.strippedObj, std::move(schema));
    auto fieldNames = cmdObj.getFieldNames<std::set<StringData>>();

    // A new camel-case name of the FindAndModify command needs to be used
    // in place of the legacy one.
    if (fieldNames.count("findandmodify")) {
        fieldNames.insert(write_ops::FindAndModifyCommandRequest::kCommandName);
    }
    placeholder.result = removeExtraFields(fieldNames, placeholder.result);

    serializePlaceholderResult(placeholder, builder);
}

/**
 * This function takes in a single element to be encrypted. It is useful for creating insert
 * placeholders for all encryption types, and find/comparison placeholders for any type that
 * requires a single BSONElement to build a payload. Notably, this does not include encrypted
 * placeholders for range queries.
 */
BSONObj buildFle2EncryptPlaceholder(EncryptionPlaceholderContext ctx,
                                    const ResolvedEncryptionInfo& metadata,
                                    BSONElement elem) {
    auto placeholderType = ctx == EncryptionPlaceholderContext::kComparison
        ? Fle2PlaceholderType::kFind
        : Fle2PlaceholderType::kInsert;
    auto algorithm = get<Fle2AlgorithmInt>(metadata.algorithm);
    auto ki = metadata.keyId.uuids()[0];
    auto cm = algorithm == Fle2AlgorithmInt::kUnindexed
        ? 0
        : metadata.fle2SupportedQueries.value()[0]
              .getContention();  // TODO: SERVER-67421 support multiple encrypted query types on a
                                 // single field.
    BSONObj backingBSON;
    auto marking = [&]() {
        switch (algorithm) {
            case Fle2AlgorithmInt::kRange: {
                auto q = metadata.fle2SupportedQueries.value()[0];
                // At this point, the encrypted index spec must have the range query type, and must
                // have min, max, and sparsity defined.
                invariant(q.getQueryType() == QueryTypeEnum::Range ||
                          q.getQueryType() == QueryTypeEnum::RangePreviewDeprecated);

                auto lb = q.getMin();
                auto ub = q.getMax();
                // IDL needs to take in BSONElements, not Values, so add the bounds to an array to
                // be pulled out as BSONElements.
                auto bounds = BSON_ARRAY(lb.value_or(Value(0)) << ub.value_or(Value(0)));
                auto sparsity = q.getSparsity().value();

                auto spec = FLE2RangeInsertSpec(IDLAnyType(elem));
                if (lb) {
                    spec.setMinBound(boost::optional<IDLAnyType>(bounds[0]));
                }
                if (ub) {
                    spec.setMaxBound(boost::optional<IDLAnyType>(bounds[1]));
                }
                auto precision = q.getPrecision();
                spec.setPrecision(precision);

                auto tf = q.getTrimFactor();
                spec.setTrimFactor(tf);

                // Ensure that the serialized spec lives until the end of the enclosing scope.
                backingBSON = BSON("" << spec.toBSON());
                auto placeholder = FLE2EncryptionPlaceholder(placeholderType,
                                                             algorithm,
                                                             ki /*indexKeyId*/,
                                                             ki /*userKeyId*/,
                                                             IDLAnyType(backingBSON.firstElement()),
                                                             cm);
                placeholder.setSparsity(sparsity);
                return placeholder;
            }
            case Fle2AlgorithmInt::kTextSearch: {
                // At this point, there may be multiple query type configs, but all of them
                // must be text search query types.
                for (auto& qtc : metadata.fle2SupportedQueries.value()) {
                    auto qtype = qtc.getQueryType();
                    invariant(qtype == QueryTypeEnum::SubstringPreview ||
                              qtype == QueryTypeEnum::SuffixPreview ||
                              qtype == QueryTypeEnum::PrefixPreview);
                }
                // TODO: SERVER-97835 implement FLE2TextSearchInsertSpec
                auto placeholder = FLE2EncryptionPlaceholder(placeholderType,
                                                             algorithm,
                                                             ki /*indexKeyId*/,
                                                             ki /*userKeyId*/,
                                                             IDLAnyType(elem),
                                                             cm);
                return placeholder;
            }
            case Fle2AlgorithmInt::kEquality:
            case Fle2AlgorithmInt::kUnindexed:
                return FLE2EncryptionPlaceholder(placeholderType,
                                                 algorithm,
                                                 ki /*indexKeyId*/,
                                                 ki /*userKeyId*/,
                                                 IDLAnyType(elem),
                                                 cm);
        }
        MONGO_UNREACHABLE;
    }();

    // Serialize the placeholder to BSON.
    BSONObjBuilder bob;
    marking.serialize(&bob);
    auto markingObj = bob.done();

    // Encode the placeholder BSON as BinData (sub-type 6 for encryption). Prepend the
    // sub-subtype byte representing the FLE2 intent-to-encrypt marking before the BSON payload.
    BufBuilder binDataBuffer;
    binDataBuffer.appendChar(static_cast<uint8_t>(EncryptedBinDataType::kFLE2Placeholder));
    binDataBuffer.appendBuf(markingObj.objdata(), markingObj.objsize());

    BSONObjBuilder binDataBob;
    binDataBob.appendBinData(
        elem.fieldNameStringData(), binDataBuffer.len(), BinDataType::Encrypt, binDataBuffer.buf());
    return binDataBob.obj();
}

PlaceHolderResult addPlaceholdersForCommandWithValidator(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const DatabaseName& dbName,
    const BSONObj& cmdObj,
    std::unique_ptr<EncryptionSchemaTreeNode> schemaTree,
    boost::optional<BSONObj> validator) {
    if (!validator) {
        return PlaceHolderResult{false, schemaTree->mayContainEncryptedNode(), nullptr, cmdObj};
    }

    // None of the MatchExpression operators that are desugared from $jsonSchema are supported in
    // query analysis. However, for FLE1 we need to explicitly support the use case where the
    // collection validator is used to store the schema rather than a client-side schemaMap. So, if
    // we're parsing a validator in a FLE1 collection and $jsonSchema is the only operator, we allow
    // the operation if the schema inside the operator exactly matches the schema passed to query
    // analysis for this collection. This is the case when the validator has one top-level field
    // with the key `$jsonSchema`. If $jsonSchema is AND-ed (explicitly or implicity) or OR-ed with
    // other operators, this branch will not be taken and a generic error will be thrown that an
    // internal schema operator is unsupported in query analysis.
    // TODO: SERVER-66657 Support some validators that have $jsonSchema AND-ed or OR-ed with other
    // operators.
    if (schemaTree->parsedFrom == FleVersion::kFle1 && validator->nFields() == 1 &&
        validator->firstElementFieldNameStringData() == "$jsonSchema"_sd) {
        auto cmdWithValidatorSchema =
            cmdObj.addField(BSON("jsonSchema" << validator->firstElement()).firstElement())
                .addField(BSON("isRemoteSchema" << false).firstElement());
        auto cryptdParams = extractCryptdParameters(
            cmdWithValidatorSchema,
            NamespaceString{CommandHelpers::parseNsFromCommand(dbName, cmdObj)},
            false /*parseMultiSchema*/);
        auto schemaTreeFromValidator =
            EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(
                cryptdParams);

        uassert(6491101,
                "validator with $jsonSchema must be identical to FLE 1 jsonSchema parameter.",
                *schemaTree == *schemaTreeFromValidator);

        return PlaceHolderResult{false, schemaTree->mayContainEncryptedNode(), nullptr, cmdObj};
    }

    auto newQueryPlaceholder =
        replaceEncryptedFieldsInFilter(expCtx, *schemaTree, validator.value());

    // TODO: SERVER-66094 Support encrypted fields in collection validator.
    uassert(6491100,
            "Comparison to encrypted fields not supported in collection validator.",
            !newQueryPlaceholder.hasEncryptionPlaceholders);
    return PlaceHolderResult{false,
                             schemaTree->mayContainEncryptedNode(),
                             std::move(newQueryPlaceholder.matchExpr),
                             cmdObj};
}

PlaceHolderResult addPlaceHoldersForCreate(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                           const DatabaseName& dbName,
                                           const BSONObj& cmdObj,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    // TODO: SERVER-66094 Add encryptionInformation to command IDL and stop stripping it out when
    // supporting encrypted fields in validator.
    auto strippedCmd = cmdObj.removeField(kEncryptionInformation);

    auto cmd = CreateCommand::parse(
        IDLParserContext("create",
                         auth::ValidatedTenancyScope::get(expCtx->getOperationContext()),
                         dbName.tenantId(),
                         SerializationContext::stateCommandRequest()),
        strippedCmd);
    uassert(7501300,
            "Creating a view is not supported with automatic encryption",
            !cmd.getPipeline().has_value() && !cmd.getViewOn().has_value());
    return addPlaceholdersForCommandWithValidator(
        expCtx, dbName, strippedCmd, std::move(schemaTree), cmd.getValidator());
}

PlaceHolderResult addPlaceHoldersForCollMod(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                            const DatabaseName& dbName,
                                            const BSONObj& cmdObj,
                                            std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    // TODO: SERVER-66094 Add encryptionInformation to command IDL and stop stripping it out when
    // supporting encrypted fields in validator.
    auto strippedCmd = cmdObj.removeField(kEncryptionInformation);
    auto cmd = CollMod::parse(
        IDLParserContext("collMod",
                         auth::ValidatedTenancyScope::get(expCtx->getOperationContext()),
                         dbName.tenantId(),
                         SerializationContext::stateCommandRequest()),
        strippedCmd);
    return addPlaceholdersForCommandWithValidator(
        expCtx, dbName, strippedCmd, std::move(schemaTree), cmd.getValidator());
}

PlaceHolderResult addPlaceHoldersForCreateIndexes(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const DatabaseName& dbName,
    const BSONObj& cmdObj,
    std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    // TODO: SERVER-66092 Add encryptionInformation to command IDL and stop stripping it out when
    // supporting encrypted fields in partial filter expression.
    auto strippedCmd = cmdObj.removeField(kEncryptionInformation);

    auto cmd = CreateIndexesCommand::parse(
        IDLParserContext("createIndexes",
                         auth::ValidatedTenancyScope::get(expCtx->getOperationContext()),
                         dbName.tenantId(),
                         SerializationContext::stateCommandRequest()),
        strippedCmd);

    for (const auto& index : cmd.getIndexes()) {
        if (index.hasField(NewIndexSpec::kPartialFilterExpressionFieldName)) {
            auto partialFilter =
                index.getObjectField(NewIndexSpec::kPartialFilterExpressionFieldName);
            auto newQueryPlaceholder =
                replaceEncryptedFieldsInFilter(expCtx, *schemaTree, partialFilter);
            // TODO: SERVER-66092 Support encrypted fields in partial filter expression.
            uassert(6491102,
                    "Comparison to encrypted fields not supported in a partialFilterExpression.",
                    !newQueryPlaceholder.hasEncryptionPlaceholders);
        }
    }
    return PlaceHolderResult{false, schemaTree->mayContainEncryptedNode(), nullptr, strippedCmd};
}

}  // namespace

std::vector<std::pair<FieldPath, Value>> reportFullPathsAndValues(Value val, FieldPath basePath) {
    std::vector<std::pair<FieldPath, Value>> retVec;
    if (val.isObject()) {
        auto doc = val.getDocument();
        FieldIterator iter(doc);
        while (iter.more()) {
            auto [fieldName, nextVal] = iter.next();
            auto nestedPaths = reportFullPathsAndValues(nextVal, basePath.concat(fieldName));
            retVec.insert(retVec.end(), nestedPaths.begin(), nestedPaths.end());
        }
    } else if (val.isArray()) {
        uasserted(6994300, "Nested arrays not supported for encrypted $in");
    } else {
        retVec.push_back({basePath, val});
    }
    return retVec;
}

PlaceHolderResult parsePlaceholderResult(BSONObj obj) {
    PlaceHolderResult placeholder;

    for (const auto& el : obj) {
        if (el.fieldNameStringData() == kHasEncryptionPlaceholders) {
            placeholder.hasEncryptionPlaceholders = el.Bool();
        } else if (el.fieldNameStringData() == kSchemaRequiresEncryption) {
            placeholder.schemaRequiresEncryption = el.Bool();
        } else if (el.fieldNameStringData() == kResult) {
            placeholder.result = el.Obj();
        }
    }

    return placeholder;
}

void serializePlaceholderResult(const PlaceHolderResult& placeholder, BSONObjBuilder* builder) {
    builder->append(kHasEncryptionPlaceholders, placeholder.hasEncryptionPlaceholders);
    builder->append(kSchemaRequiresEncryption, placeholder.schemaRequiresEncryption);
    builder->append(kResult, placeholder.result);
}

PlaceHolderResult replaceEncryptedFields(BSONObj doc,
                                         const EncryptionSchemaTreeNode* schema,
                                         EncryptionPlaceholderContext placeholderContext,
                                         FieldRef leadingPath,
                                         const boost::optional<BSONObj>& origDoc,
                                         const CollatorInterface* collator) {
    PlaceHolderResult res;
    res.result = replaceEncryptedFieldsRecursive(schema,
                                                 doc,
                                                 placeholderContext,
                                                 origDoc,
                                                 collator,
                                                 &leadingPath,
                                                 &res.hasEncryptionPlaceholders);
    return res;
}

void processFindCommand(OperationContext* opCtx,
                        const DatabaseName& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* builder,
                        const NamespaceString ns) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForFind, ns);
}

void processAggregateCommand(OperationContext* opCtx,
                             const DatabaseName& dbName,
                             const BSONObj& cmdObj,
                             BSONObjBuilder* builder,
                             const NamespaceString ns) {
    // (Ignore FCV check): Query Analysis does not run in the server, so it can't be FCV gated.
    if (feature_flags::gFeatureFlagLookupEncryptionSchemasFLE.isEnabledAndIgnoreFCVUnsafe()) {
        processQueryCommand(
            opCtx, dbName, cmdObj, builder, addPlaceHoldersForAggregate<EncryptionSchemaMap>, ns);
    } else {
        processQueryCommand(opCtx,
                            dbName,
                            cmdObj,
                            builder,
                            addPlaceHoldersForAggregate<std::unique_ptr<EncryptionSchemaTreeNode>>,
                            ns);
    }
}

void processDistinctCommand(OperationContext* opCtx,
                            const DatabaseName& dbName,
                            const BSONObj& cmdObj,
                            BSONObjBuilder* builder,
                            const NamespaceString ns) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForDistinct, ns);
}

void processCountCommand(OperationContext* opCtx,
                         const DatabaseName& dbName,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* builder,
                         const NamespaceString ns) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForCount, ns);
}

void processFindAndModifyCommand(OperationContext* opCtx,
                                 const DatabaseName& dbName,
                                 const BSONObj& cmdObj,
                                 BSONObjBuilder* builder,
                                 const NamespaceString ns) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForFindAndModify, ns);
}

void processCreateCommand(OperationContext* opCtx,
                          const DatabaseName& dbName,
                          const BSONObj& cmdObj,
                          BSONObjBuilder* builder,
                          const NamespaceString ns) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForCreate, ns);
}

void processCollModCommand(OperationContext* opCtx,
                           const DatabaseName& dbName,
                           const BSONObj& cmdObj,
                           BSONObjBuilder* builder,
                           const NamespaceString ns) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForCollMod, ns);
}

void processCreateIndexesCommand(OperationContext* opCtx,
                                 const DatabaseName& dbName,
                                 const BSONObj& cmdObj,
                                 BSONObjBuilder* builder,
                                 const NamespaceString ns) {
    processQueryCommand(opCtx, dbName, cmdObj, builder, addPlaceHoldersForCreateIndexes, ns);
}

void processBulkWriteCommand(OperationContext* opCtx,
                             const OpMsgRequest& request,
                             BSONObjBuilder* builder,
                             const NamespaceString ns) {
    processWriteOpCommand(opCtx, request, builder, addPlaceHoldersForBulkWrite, ns);
}

void processInsertCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder,
                          const NamespaceString ns) {
    processWriteOpCommand(opCtx, request, builder, addPlaceHoldersForInsert, ns);
}

void processUpdateCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder,
                          const NamespaceString ns) {
    processWriteOpCommand(opCtx, request, builder, addPlaceHoldersForUpdate, ns);
}

void processDeleteCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder,
                          const NamespaceString ns) {
    processWriteOpCommand(opCtx, request, builder, addPlaceHoldersForDelete, ns);
}

BSONObj buildEncryptPlaceholder(BSONElement elem,
                                const ResolvedEncryptionInfo& metadata,
                                EncryptionPlaceholderContext placeholderContext,
                                const CollatorInterface* collator,
                                const boost::optional<BSONObj>& origDoc,
                                const boost::optional<const EncryptionSchemaTreeNode&> schema) {
    // Some types are unconditionally banned. Make sure we're not trying to mark any such type for
    // encryption.
    uassert(31041,
            str::stream() << "Cannot encrypt element of type: "
                          << (elem.type() == BSONType::BinData ? "encrypted binary data"
                                                               : typeName(elem.type())),
            metadata.isElemLegalForEncryption(elem));
    // There are more stringent requirements for which encryption placeholders are legal in the
    // context of a query which makes a comparison to an encrypted field. For instance, comparisons
    // are only possible against deterministically encrypted fields, whereas either the random or
    // deterministic encryption algorithms are legal in the context of a placeholder for a write.
    if (placeholderContext == EncryptionPlaceholderContext::kComparison) {
        if (metadata.isFle2Encrypted()) {
            uassert(63165,
                    "Can only execute encrypted equality queries with an encrypted equality index",
                    metadata.algorithmIs(Fle2AlgorithmInt::kEquality));
        } else {
            uassert(51158,
                    "Cannot query on fields encrypted with the randomized encryption algorithm",
                    metadata.algorithmIs(FleAlgorithmEnum::kDeterministic));
        }

        switch (elem.type()) {
            case BSONType::String:
            case BSONType::Symbol: {
                uassert(31054,
                        str::stream()
                            << "cannot apply non-simple collation when comparing to element "
                            << elem << " with client-side encryption",
                        !collator);
                break;
            }
            default:
                break;
        }
    }

    if (metadata.bsonTypeSet) {
        uassert(31118,
                str::stream() << "Cannot encrypt element of type " << typeName(elem.type())
                              << " because schema requires that type is one of: "
                              << typeSetToString(*metadata.bsonTypeSet),
                metadata.bsonTypeSet->hasType(elem.type()));
    }


    invariant(metadata.isTypeLegal(elem.type()));
    if (metadata.isFle2Encrypted()) {
        return buildFle2EncryptPlaceholder(placeholderContext, metadata, elem);
    }

    // At this point we know the following:
    //  - The metadata is configured for FLE 1
    //  - The caller has provided a valid ResolvedEncryptionInfo, which enforces that there is
    //  exactly one type specified in combination with the deterministic encryption algorithm.
    //  - The caller also has verified that this type is valid in combination with the deterministic
    //  encryption algorithm. For instance, we ban deterministic encryption of objects and arrays.
    //  - If the schema specifies 'bsonType', then the type which we're marking for encryption
    //  is of a matching type.
    //
    // Together, these conditions imply that if the encryption algorithm is deterministic, then the
    // type of 'elem' is known to be valid for deterministic encryption. Check that assumption here.
    invariant(metadata.algorithmIs(FleAlgorithmEnum::kRandom) || metadata.isTypeLegal(elem.type()));

    FleAlgorithmInt integerAlgorithm = metadata.algorithmIs(FleAlgorithmEnum::kDeterministic)
        ? FleAlgorithmInt::kDeterministic
        : FleAlgorithmInt::kRandom;

    EncryptionPlaceholder marking(integerAlgorithm, IDLAnyType(elem));

    const auto& keyId = metadata.keyId;
    if (keyId.type() == EncryptSchemaKeyId::Type::kUUIDs) {
        marking.setKeyId(keyId.uuids()[0]);
    } else {
        uassert(51093, "A non-static (JSONPointer) keyId is not supported.", origDoc);
        auto pointer = keyId.jsonPointer();
        auto resolvedKey = pointer.evaluate(origDoc.value());
        uassert(51114,
                "keyId pointer '" + pointer.toString() + "' must point to a field that exists",
                resolvedKey);
        uassert(30017,
                "keyId pointer '" + pointer.toString() + "' cannot point to an encrypted field",
                !(schema->getEncryptionMetadataForPath(pointer.toFieldRef())));
        uassert(51115,
                "keyId pointer '" + pointer.toString() + "' must point to a string",
                resolvedKey.type() == BSONType::String);
        marking.setKeyAltName(resolvedKey.valueStringData());
    }

    // Serialize the placeholder to BSON.
    BSONObjBuilder bob;
    marking.serialize(&bob);
    auto markingObj = bob.done();

    // Encode the placeholder BSON as BinData (sub-type 6 for encryption). Prepend the sub-subtype
    // byte represent the intent-to-encrypt marking before the BSON payload.
    BufBuilder binDataBuffer;
    binDataBuffer.appendChar(static_cast<uint8_t>(EncryptedBinDataType::kPlaceholder));
    binDataBuffer.appendBuf(markingObj.objdata(), markingObj.objsize());

    BSONObjBuilder binDataBob;
    binDataBob.appendBinData(
        elem.fieldNameStringData(), binDataBuffer.len(), BinDataType::Encrypt, binDataBuffer.buf());
    return binDataBob.obj();
}

Value buildEncryptPlaceholder(Value input,
                              const ResolvedEncryptionInfo& metadata,
                              EncryptionPlaceholderContext placeholderContext,
                              const CollatorInterface* collator) {
    StringData wrappingKey;
    // We cannot convert a Value directly into a BSONElement. So we wrap the 'input' into a Document
    // with key as 'wrappingKey' and then unwrap before returning.
    return Value(buildEncryptPlaceholder(Document{{wrappingKey, input}}.toBson().firstElement(),
                                         metadata,
                                         placeholderContext,
                                         collator,
                                         boost::none,
                                         boost::none)[wrappingKey]);
}

BSONObj serializeFle2Placeholder(StringData fieldname,
                                 const FLE2EncryptionPlaceholder& placeholder) {

    // Encode the placeholder BSON as BinData (sub-type 6 for encryption). Prepend the
    // sub-subtype byte representing the FLE2 intent-to-encrypt marking before the BSON payload.
    BufBuilder binDataBuffer;
    binDataBuffer.appendChar(static_cast<uint8_t>(EncryptedBinDataType::kFLE2Placeholder));
    auto markingObj = placeholder.toBSON();
    binDataBuffer.appendBuf(markingObj.objdata(), markingObj.objsize());

    BSONObjBuilder binDataBob;
    binDataBob.appendBinData(
        fieldname, binDataBuffer.len(), BinDataType::Encrypt, binDataBuffer.buf());
    return binDataBob.obj();
}

namespace {
void assertNotNaN(BSONElement elt) {
    switch (elt.type()) {
        case BSONType::NumberDouble:
            uassert(6991000, "Query bound cannot be NaN.", !std::isnan(elt.Double()));
            break;
        case BSONType::NumberDecimal:
            uassert(6991001, "Query bound cannot be NaN.", !elt.Decimal().isNaN());
            break;
        default:
            break;
    }
}

}  // namespace

BSONObj makeAndSerializeRangeStub(StringData fieldname,
                                  UUID ki,
                                  QueryTypeConfig indexConfig,
                                  int32_t payloadId,
                                  Fle2RangeOperator firstOp,
                                  Fle2RangeOperator secondOp) {
    auto cm = indexConfig.getContention();
    auto sparsity = indexConfig.getSparsity();

    FLE2RangeFindSpec findSpec;

    findSpec.setPayloadId(payloadId);
    findSpec.setFirstOperator(firstOp);
    findSpec.setSecondOperator(secondOp);
    findSpec.setEdgesInfo(boost::none);

    auto rangeBSON = BSON("" << findSpec.toBSON());
    auto idlPlaceholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                    Fle2AlgorithmInt::kRange,
                                                    ki,
                                                    ki,
                                                    IDLAnyType(rangeBSON.firstElement()),
                                                    cm);
    idlPlaceholder.setSparsity(sparsity);
    return serializeFle2Placeholder(fieldname, idlPlaceholder);
}

BSONObj makeAndSerializeRangePlaceholder(StringData fieldname,
                                         UUID ki,
                                         QueryTypeConfig indexConfig,
                                         std::pair<BSONElement, bool> lowerSpec,
                                         std::pair<BSONElement, bool> upperSpec,
                                         int32_t payloadId,
                                         Fle2RangeOperator firstOp,
                                         boost::optional<Fle2RangeOperator> secondOp) {
    auto [lowerBound, lowerIncluded] = lowerSpec;
    auto [upperBound, upperIncluded] = upperSpec;
    assertNotNaN(lowerBound);
    assertNotNaN(upperBound);
    tassert(7018204, "Encrypted query must define a minimum.", indexConfig.getMin().has_value());
    tassert(7018205, "Encrypted query must define a maximum.", indexConfig.getMax().has_value());
    auto indexBounds = BSON_ARRAY(indexConfig.getMin().value() << indexConfig.getMax().value());
    auto cm = indexConfig.getContention();

    FLE2RangeFindSpec findSpec;

    FLE2RangeFindSpecEdgesInfo edgesInfo{};
    edgesInfo.setLowerBound(lowerBound);
    edgesInfo.setLbIncluded(lowerIncluded);
    edgesInfo.setUpperBound(upperBound);
    edgesInfo.setUbIncluded(upperIncluded);
    edgesInfo.setIndexMin(indexBounds["0"]);
    edgesInfo.setIndexMax(indexBounds["1"]);
    edgesInfo.setPrecision(indexConfig.getPrecision());
    edgesInfo.setTrimFactor(indexConfig.getTrimFactor());
    findSpec.setEdgesInfo(edgesInfo);

    findSpec.setPayloadId(payloadId);
    findSpec.setFirstOperator(firstOp);
    findSpec.setSecondOperator(secondOp);

    auto rangeBSON = BSON("" << findSpec.toBSON());
    auto idlPlaceholder = FLE2EncryptionPlaceholder(Fle2PlaceholderType::kFind,
                                                    Fle2AlgorithmInt::kRange,
                                                    ki,
                                                    ki,
                                                    IDLAnyType(rangeBSON.firstElement()),
                                                    cm);
    idlPlaceholder.setSparsity(indexConfig.getSparsity().value_or(kFLERangeSparsityDefault));
    return serializeFle2Placeholder(fieldname, idlPlaceholder);
}


BSONObj buildOneSidedEncryptedRangePlaceholder(StringData fieldname,
                                               const ResolvedEncryptionInfo& metadata,
                                               BSONElement value,
                                               MatchExpression::MatchType op,
                                               int32_t payloadId) {
    auto ki = metadata.keyId.uuids()[0];
    // TODO: SERVER-67421 support multiple queries for a field.
    auto indexConfig = metadata.fle2SupportedQueries.get()[0];
    auto kMinDouble = BSON("" << -std::numeric_limits<double>::infinity());
    auto kMaxDouble = BSON("" << std::numeric_limits<double>::infinity());
    uassert(6747900,
            str::stream() << "Range query predicate must be within the bounds of encrypted index.",
            literalWithinRangeBounds(indexConfig, value));
    switch (op) {
        case MatchExpression::GT:
            return makeAndSerializeRangePlaceholder(fieldname,
                                                    ki,
                                                    indexConfig,
                                                    {value, false},
                                                    {kMaxDouble.firstElement(), true},
                                                    payloadId,
                                                    Fle2RangeOperator::kGt);
        case MatchExpression::GTE:
            return makeAndSerializeRangePlaceholder(fieldname,
                                                    ki,
                                                    indexConfig,
                                                    {value, true},
                                                    {kMaxDouble.firstElement(), true},
                                                    payloadId,
                                                    Fle2RangeOperator::kGte);
        case MatchExpression::LT:
            return makeAndSerializeRangePlaceholder(fieldname,
                                                    ki,
                                                    indexConfig,
                                                    {kMinDouble.firstElement(), true},
                                                    {value, false},
                                                    payloadId,
                                                    Fle2RangeOperator::kLt);
        case MatchExpression::LTE:
            return makeAndSerializeRangePlaceholder(fieldname,
                                                    ki,
                                                    indexConfig,
                                                    {kMinDouble.firstElement(), true},
                                                    {value, true},
                                                    payloadId,
                                                    Fle2RangeOperator::kLte);
        default:
            MONGO_UNREACHABLE_TASSERT(7030200);
    }
    MONGO_COMPILER_UNREACHABLE;
}

std::unique_ptr<AndMatchExpression> buildTwoSidedEncryptedRangeWithPlaceholder(
    StringData fieldname,
    const ResolvedEncryptionInfo& metadata,
    std::pair<BSONElement, bool> lowerSpec,
    std::pair<BSONElement, bool> upperSpec,
    int32_t payloadId,
    std::vector<BSONObj>& bsonBuffer) {
    auto ki = metadata.keyId.uuids()[0];
    // TODO: SERVER-67421 support multiple queries for a field.
    auto indexConfig = metadata.fle2SupportedQueries.get()[0];

    return buildTwoSidedEncryptedRangeWithPlaceholder(
        fieldname, ki, indexConfig, lowerSpec, upperSpec, payloadId, bsonBuffer);
}

std::unique_ptr<AndMatchExpression> buildTwoSidedEncryptedRangeWithPlaceholder(
    StringData fieldname,
    UUID ki,
    QueryTypeConfig indexConfig,
    std::pair<BSONElement, bool> lowerSpec,
    std::pair<BSONElement, bool> upperSpec,
    int32_t payloadId,
    std::vector<BSONObj>& bsonBuffer) {
    auto placeholder = bsonBuffer.emplace_back(makeAndSerializeRangePlaceholder(
        fieldname,
        ki,
        indexConfig,
        lowerSpec,
        upperSpec,
        payloadId,
        lowerSpec.second ? Fle2RangeOperator::kGte : Fle2RangeOperator::kGt,
        upperSpec.second ? Fle2RangeOperator::kLte : Fle2RangeOperator::kLt));

    auto stub = bsonBuffer.emplace_back(makeAndSerializeRangeStub(
        fieldname,
        ki,
        indexConfig,
        payloadId,
        lowerSpec.second ? Fle2RangeOperator::kGte : Fle2RangeOperator::kGt,
        upperSpec.second ? Fle2RangeOperator::kLte : Fle2RangeOperator::kLt));

    auto lowerBoundExpr = [&]() -> std::unique_ptr<MatchExpression> {
        if (lowerSpec.second) {
            return std::make_unique<GTEMatchExpression>(fieldname, placeholder.firstElement());
        } else {
            return std::make_unique<GTMatchExpression>(fieldname, placeholder.firstElement());
        }
    }();

    auto upperBoundExpr = [&]() -> std::unique_ptr<MatchExpression> {
        if (upperSpec.second) {
            return std::make_unique<LTEMatchExpression>(fieldname, stub.firstElement());
        } else {
            return std::make_unique<LTMatchExpression>(fieldname, stub.firstElement());
        }
    }();

    std::vector<std::unique_ptr<MatchExpression>> children;
    children.push_back(std::move(lowerBoundExpr));
    children.push_back(std::move(upperBoundExpr));

    return std::make_unique<AndMatchExpression>(std::move(children));
}

boost::intrusive_ptr<Expression> buildTwoSidedEncryptedRangeWithPlaceholder(
    ExpressionContext* expCtx,
    StringData fieldname,
    const ResolvedEncryptionInfo& metadata,
    std::pair<BSONElement, bool> lowerSpec,
    std::pair<BSONElement, bool> upperSpec,
    int32_t payloadId,
    std::vector<BSONObj>& bsonBuffer) {
    auto ki = metadata.keyId.uuids()[0];
    // TODO: SERVER-67421 support multiple queries for a field.
    auto indexConfig = metadata.fle2SupportedQueries.get()[0];

    return buildEncryptedRangeWithPlaceholder(
        expCtx, fieldname, ki, indexConfig, lowerSpec, upperSpec, payloadId);
}

boost::intrusive_ptr<Expression> buildEncryptedRangeWithPlaceholder(
    ExpressionContext* expCtx,
    StringData fieldname,
    UUID ki,
    QueryTypeConfig indexConfig,
    std::pair<BSONElement, bool> lowerSpec,
    std::pair<BSONElement, bool> upperSpec,
    int32_t payloadId) {
    auto placeholder = makeAndSerializeRangePlaceholder(
        fieldname,
        ki,
        indexConfig,
        lowerSpec,
        upperSpec,
        payloadId,
        lowerSpec.second ? Fle2RangeOperator::kGte : Fle2RangeOperator::kGt,
        upperSpec.second ? Fle2RangeOperator::kLte : Fle2RangeOperator::kLt);

    auto stub = makeAndSerializeRangeStub(
        fieldname,
        ki,
        indexConfig,
        payloadId,
        lowerSpec.second ? Fle2RangeOperator::kGte : Fle2RangeOperator::kGt,
        upperSpec.second ? Fle2RangeOperator::kLte : Fle2RangeOperator::kGt);

    std::vector<boost::intrusive_ptr<Expression>> gtArgs{
        ExpressionFieldPath::createPathFromString(
            expCtx, std::string(fieldname), expCtx->variablesParseState),
        ExpressionConstant::create(expCtx, Value(placeholder.firstElement()))};
    std::vector<boost::intrusive_ptr<Expression>> ltArgs{
        ExpressionFieldPath::createPathFromString(
            expCtx, std::string(fieldname), expCtx->variablesParseState),
        ExpressionConstant::create(expCtx, Value(placeholder.firstElement()))};
    // Create operators based on whether the specs are inclusive or exclusive.
    auto gtExpression = make_intrusive<ExpressionCompare>(
        expCtx,
        lowerSpec.second ? ExpressionCompare::CmpOp::GTE : ExpressionCompare::CmpOp::GT,
        std::move(gtArgs));
    auto ltExpression = make_intrusive<ExpressionCompare>(
        expCtx,
        upperSpec.second ? ExpressionCompare::CmpOp::LTE : ExpressionCompare::CmpOp::LT,
        std::move(ltArgs));
    std::vector<boost::intrusive_ptr<Expression>> andArgs{std::move(gtExpression),
                                                          std::move(ltExpression)};
    return make_intrusive<ExpressionAnd>(expCtx, std::move(andArgs));
}

bool literalWithinRangeBounds(const QueryTypeConfig& config, BSONElement elt) {
    tassert(7030201,
            "Expected range index",
            config.getQueryType() == QueryTypeEnum::Range ||
                config.getQueryType() == QueryTypeEnum::RangePreviewDeprecated);
    tassert(7030202,
            "Min and max must be set on the encrypted index",
            config.getMin().has_value() && config.getMax().has_value());
    auto min = config.getMin().value();
    auto max = config.getMax().value();
    auto literal = Value(elt);

    invariant(min.getType() == max.getType());
    Value coercedLiteral = coerceValueToRangeIndexTypes(literal, min.getType());

    auto minBoundCheck = Value::compare(min, coercedLiteral, nullptr);
    auto maxBoundCheck = Value::compare(coercedLiteral, max, nullptr);
    return (minBoundCheck <= 0) && (maxBoundCheck <= 0);
}

bool literalWithinRangeBounds(const ResolvedEncryptionInfo& metadata, BSONElement elt) {
    tassert(6720808, "Expected range index", metadata.algorithmIs(Fle2AlgorithmInt::kRange));
    // TODO: SERVER-67421 support multiple encrypted index types on a single field.
    return literalWithinRangeBounds(metadata.fle2SupportedQueries.get()[0], elt);
}

}  // namespace mongo::query_analysis
