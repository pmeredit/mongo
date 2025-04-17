#include "mongo/db/pipeline/document_source_extension.h"

#include <cstdint>
#include <mutex>
#include <shared_mutex>

#include "absl/base/nullability.h"
#include "mongo/base/init.h"  // IWYU pragma: keep
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/db/pipeline/plugin/plugin.h"
#include "mongo/db/query/search/mongot_options.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/serialization_context.h"
#include "mongo/util/string_map.h"

namespace mongo {

namespace {

StringData byteViewAsStringData(const MongoExtensionByteView view) {
    return StringData(reinterpret_cast<const char*>(view.data), view.len);
}

StringData byteBufAsStringData(const MongoExtensionByteBuf& buf) {
    return byteViewAsStringData(buf.vtable->get(&buf));
}

BSONObj bsonObjFromByteView(const MongoExtensionByteView view) {
    auto is_valid_obj = [](const MongoExtensionByteView view) -> bool {
        if (view.len < BSONObj::kMinBSONLength) {
            return false;
        }

        // Decode the value in little-endian order.
        int32_t document_len = (int32_t)view.data[0] | ((int32_t)view.data[1] << 8) |
            ((int32_t)view.data[2] << 16) | ((int32_t)view.data[3] << 24);
        return document_len >= 0 && (size_t)document_len <= view.len;
    };
    tassert(1234567, "extension returned invalid bson obj", is_valid_obj(view));
    return BSONObj(reinterpret_cast<const char*>(view.data));
}

MongoExtensionByteView objAsByteView(const BSONObj& obj) {
    return MongoExtensionByteView{reinterpret_cast<const unsigned char*>(obj.objdata()),
                                  static_cast<size_t>(obj.objsize())};
}

BSONObj createContext(const ExpressionContext& ctx) {
    BSONObjBuilder b;
    b.append("$db",
             DatabaseNameUtil::serialize(ctx.getNamespaceString().dbName(),
                                         SerializationContext::stateCommandRequest()));
    if (!ctx.getNamespaceString().coll().empty()) {
        b.append("collection", ctx.getNamespaceString().coll());
    }
    if (ctx.getUUID()) {
        (*ctx.getUUID()).appendToBuilder(&b, "collectionUUID");
    }
    b.append("inRouter", ctx.getInRouter());
    b.append("mongotHost", globalMongotParams.host);
    return b.obj();
}

}  // anonymous namespace

MONGO_INITIALIZER_GENERAL(addToDocSourceParserMap_plugin,
                          ("BeginDocumentSourceRegistration"),
                          ("EndDocumentSourceRegistration"))
(InitializerContext*) {
    MongoExtensionPortal portal;
    portal.version = MONGODB_PLUGIN_VERSION_0;
    portal.registerStageDescriptor = &DocumentSourceExtension::registerStageDescriptor;
    mongodb_initialize_plugin(&portal);
}

// static
void DocumentSourceExtension::registerStageDescriptor(
    MongoExtensionByteView name, const MongoExtensionAggregationStageDescriptor* descriptor) {
    fassert(9999999, descriptor != nullptr);

    auto name_sd = byteViewAsStringData(name);
    auto id = DocumentSource::allocateId(name_sd);
    LiteParsedDocumentSource::registerParser(name_sd.toString(),
                                             LiteParsedDocumentSourceDefault::parse,
                                             AllowedWithApiStrict::kAlways,
                                             AllowedWithClientType::kAny);

    switch (descriptor->vtable->type(descriptor)) {
        case MongoExtensionAggregationStageType::kSource:
        case MongoExtensionAggregationStageType::kTransform:
            registerConcreteStage(name_sd.toString(), id, descriptor);
            break;
        case MongoExtensionAggregationStageType::kDesugar:
            registerDesugarStage(name_sd.toString(), id, descriptor);
            break;
        default:
            fassert(999999, false);  // unknown stage type. should just log instead?
            break;
    };
}

// static
void DocumentSourceExtension::registerConcreteStage(
    std::string name,
    DocumentSource::Id id,
    absl::Nonnull<const MongoExtensionAggregationStageDescriptor*> descriptor) {
    DocumentSource::registerParser(
        name,
        [id, descriptor](BSONElement specElem,
                         const boost::intrusive_ptr<ExpressionContext>& expCtx)
            -> boost::intrusive_ptr<DocumentSource> {
            BSONObj stageDef = specElem.wrap();
            BSONObj context = createContext(*expCtx);

            MongoExtensionBoundAggregationStageDescriptor* boundDescriptorPtr = nullptr;
            MongoExtensionByteBuf* errorPtr = nullptr;
            int code = descriptor->vtable->bind(descriptor,
                                                objAsByteView(stageDef),
                                                objAsByteView(context),
                                                &boundDescriptorPtr,
                                                &errorPtr);
            auto boundDescriptor = BoundDescriptorPtr(boundDescriptorPtr);
            auto error = ByteBufPtr(errorPtr);
            uassert(code, str::stream() << byteBufAsStringData(*error), code == 0);

            return boost::intrusive_ptr(new DocumentSourceExtension(specElem.fieldNameStringData(),
                                                                    expCtx,
                                                                    id,
                                                                    std::move(stageDef),
                                                                    descriptor,
                                                                    std::move(boundDescriptor)));
        },
        kDoesNotRequireFeatureFlag);
}

// static
void DocumentSourceExtension::registerDesugarStage(
    std::string name,
    DocumentSource::Id id,
    absl::Nonnull<const MongoExtensionAggregationStageDescriptor*> descriptor) {
    DocumentSource::registerParser(
        name,
        [id, descriptor](BSONElement specElem,
                         const boost::intrusive_ptr<ExpressionContext>& expCtx)
            -> std::list<boost::intrusive_ptr<DocumentSource>> {
            BSONObj stageDef = specElem.wrap();
            BSONObj context = createContext(*expCtx);
            MongoExtensionByteBuf* resultPtr = nullptr;
            const int code = descriptor->vtable->desugar(
                descriptor, objAsByteView(stageDef), objAsByteView(context), &resultPtr);

            std::unique_ptr<MongoExtensionByteBuf, ExtensionObjectDeleter> result(resultPtr);
            uassert(code, str::stream() << byteBufAsStringData(*result), code == 0);

            // TODO: verify result.len matches the length prefix of result.data.
            BSONObj desugared(byteBufAsStringData(*result).data());
            auto elem = desugared.firstElement();
            std::list<boost::intrusive_ptr<DocumentSource>> desugared_sources;
            if (elem.type() == BSONType::Array) {
                for (auto stageElem : elem.embeddedObject()) {
                    for (auto stage : DocumentSource::parse(expCtx, stageElem.embeddedObject())) {
                        desugared_sources.push_back(stage);
                    }
                }
            } else {
                desugared_sources = DocumentSource::parse(expCtx, elem.embeddedObject());
            }
            return desugared_sources;
        },
        kDoesNotRequireFeatureFlag);
}

DocumentSource::GetNextResult DocumentSourceExtension::doGetNext() {
    if (_executor == nullptr) {
        // Create an executor from _boundDescriptor. DocumentSource doesn't provide any signal that
        // execution is about to begin, unlike SBE.
        MongoExtensionAggregationStage* sourcePtr = nullptr;
        if (_descriptor->vtable->type(_descriptor) ==
            MongoExtensionAggregationStageType::kTransform) {
            uassert(1, "Transform stage does not have a source pointer", pSource != nullptr);
            _source = std::make_unique<SourceAggregationStageExecutor>(pSource);
            sourcePtr = _source.get();
        }
        MongoExtensionAggregationStage* executorPtr = nullptr;
        MongoExtensionByteBuf* errorPtr = nullptr;
        const int code = _boundDescriptor->vtable->createExecutor(
            _boundDescriptor.get(), sourcePtr, &executorPtr, &errorPtr);
        _executor = ExecutorPtr(executorPtr);
        auto error = ByteBufPtr(errorPtr);
        uassert(code, str::stream() << byteBufAsStringData(*error), code == 0);
    }

    MongoExtensionGetNextResultCode code;
    MongoExtensionByteView doc{nullptr, 0};
    auto error = ErrorPtr(_executor->vtable->getNext(_executor.get(), &code, &doc));
    if (error != nullptr) {
        // If this call returns non-nullptr, then the underlying error is a thrown exception that
        // was caught and passed through the extension.
        auto exception = ServerExceptionError::extractException(*error);
        if (exception) {
            std::rethrow_exception(exception);
        } else {
            // This error was generated by the extension so assert to throw a new exception.
            // TODO: we may want to 'type' errors so that they can perform another assert variant.
            uasserted(error->vtable->code(error.get()),
                      str::stream() << byteViewAsStringData(error->vtable->reason(error.get())));
        }
    }
    switch (code) {
        case MongoExtensionGetNextResultCode::kAdvanced:
            return GetNextResult(Document::fromBsonWithMetaData(bsonObjFromByteView(doc)));
        case MongoExtensionGetNextResultCode::kEOF:
            return GetNextResult::makeEOF();
        case MongoExtensionGetNextResultCode::kPauseExecution:
            return GetNextResult::makePauseExecution();
        default:
            uassert(code, "unknown GetNextResult code", false);
            return GetNextResult::makeEOF();
    }
}

boost::optional<DocumentSource::DistributedPlanLogic>
DocumentSourceExtension::distributedPlanLogic() {
    // TODO Can the plugin modify "shardsStages"?
    // TODO Handle stages that must execute on the merging node ($voyageRerank)
    // TODO More potential optimization through "needsSplit"/"canMovePast" fields for $search.

    MongoExtensionByteBuf* result_ptr = nullptr;
    int code = _boundDescriptor->vtable->getMergingStages(_boundDescriptor.get(), &result_ptr);
    std::unique_ptr<MongoExtensionByteBuf, ExtensionObjectDeleter> result(result_ptr);
    uassert(code, str::stream() << byteBufAsStringData(*result), code == 0);
    BSONObj mergeStagesBson(byteBufAsStringData(*result).data());

    DistributedPlanLogic logic;
    auto elem = mergeStagesBson.firstElement();
    std::list<boost::intrusive_ptr<DocumentSource>> merge_sources;
    uassert(9999999,
            "Merging stages provided by extension stage must be array.",
            elem.type() == BSONType::Array);

    bool firstStage = true;
    for (auto stageElem : elem.embeddedObject()) {
        for (auto stage : DocumentSource::parse(pExpCtx, stageElem.embeddedObject())) {
            if (firstStage) {
                firstStage = false;
                // If this is a $sort stage, set it as mergeSortPattern
                if (stage->getId() == DocumentSourceSort::id) {
                    auto sortStage = static_cast<DocumentSourceSort*>(stage.get());
                    logic.mergeSortPattern =
                        sortStage->getSortKeyPattern()
                            .serialize(SortPattern::SortKeySerialization::kForPipelineSerialization)
                            .toBson();
                }
            } else {
                merge_sources.push_back(stage);
            }
        }
    }

    logic.mergingStages = std::move(merge_sources);
    logic.shardsStage = this;

    return logic;
}

namespace {
// TODO: consider how will all work as we add types. Extension might need to be aware of the server
// version and use different constraints for older versions.
StageConstraints::StreamType propertiesStreamType(const BSONObj& properties) {
    static const auto* kTypes = new StringDataMap<StageConstraints::StreamType>{
        {"streaming", StageConstraints::StreamType::kStreaming},
        {"blocking", StageConstraints::StreamType::kBlocking}};
    auto it = kTypes->find(properties.getStringField("streamType"));
    tassert(1234567, "unknown streamType", it != kTypes->end());
    return it->second;
}

StageConstraints::PositionRequirement propertiesPosition(const BSONObj& properties) {
    static const auto* kTypes = new StringDataMap<StageConstraints::PositionRequirement>{
        {"none", StageConstraints::PositionRequirement::kNone},
        {"first", StageConstraints::PositionRequirement::kFirst},
        {"last", StageConstraints::PositionRequirement::kLast}};
    auto it = kTypes->find(properties.getStringField("position"));
    tassert(1234567, "unknown position", it != kTypes->end());
    return it->second;
}

StageConstraints::HostTypeRequirement propertiesHostType(const BSONObj& properties) {
    static const auto* kTypes = new StringDataMap<StageConstraints::HostTypeRequirement>{
        {"none", StageConstraints::HostTypeRequirement::kNone},
        {"localOnly", StageConstraints::HostTypeRequirement::kLocalOnly},
        {"runOnceAnyNode", StageConstraints::HostTypeRequirement::kRunOnceAnyNode},
        {"anyShard", StageConstraints::HostTypeRequirement::kAnyShard},
        {"router", StageConstraints::HostTypeRequirement::kRouter},
        {"allShards", StageConstraints::HostTypeRequirement::kAllShardHosts},
    };
    auto it = kTypes->find(properties.getStringField("hostType"));
    tassert(1234567, "unknown host type", it != kTypes->end());
    return it->second;
}
}  // anonymous namespace

StageConstraints DocumentSourceExtension::constraints(Pipeline::SplitState pipeState) const {
    auto properties = bsonObjFromByteView(_descriptor->vtable->properties(_descriptor));
    auto constraints = StageConstraints(propertiesStreamType(properties),
                                        propertiesPosition(properties),
                                        propertiesHostType(properties),
                                        DiskUseRequirement::kNoDiskUse,
                                        FacetRequirement::kNotAllowed,
                                        TransactionRequirement::kNotAllowed,
                                        LookupRequirement::kNotAllowed,
                                        UnionRequirement::kNotAllowed,
                                        ChangeStreamRequirement::kDenylist);
    if (_descriptor->vtable->type(_descriptor) == MongoExtensionAggregationStageType::kSource) {
        constraints.requiresInputDocSource = false;
    }
    return constraints;
}

DocumentSourceExtension::SourceAggregationStageExecutor::SourceAggregationStageExecutor(
    DocumentSource* source)
    : _source(source) {
    this->vtable = &VTABLE;
}

MongoExtensionError* DocumentSourceExtension::SourceAggregationStageExecutor::getNext(
    MongoExtensionGetNextResultCode* code, MongoExtensionByteView* doc) noexcept {
    *doc = MongoExtensionByteView{nullptr, 0};
    try {
        auto result = _source->getNext();
        switch (result.getStatus()) {
            case DocumentSource::GetNextResult::ReturnStatus::kAdvanced:
                _source_doc = result.releaseDocument().toBson();
                *code = MongoExtensionGetNextResultCode::kAdvanced;
                doc->data = reinterpret_cast<const unsigned char*>(_source_doc.objdata());
                doc->len = _source_doc.objsize();
                break;
            case DocumentSource::GetNextResult::ReturnStatus::kEOF:
                *code = MongoExtensionGetNextResultCode::kEOF;
                break;
            case DocumentSource::GetNextResult::ReturnStatus::kPauseExecution:
                *code = MongoExtensionGetNextResultCode::kPauseExecution;
                break;
        }
        return nullptr;
    } catch (const DBException& e) {
        return new ServerExceptionError(std::current_exception(), e.code());
    } catch (...) {
        return new ServerExceptionError(std::current_exception(), -1);
    }
}

extern "C" MongoExtensionError* _mongoSourceAggregationStageExecutorGetNext(
    MongoExtensionAggregationStage* executor,
    MongoExtensionGetNextResultCode* code,
    MongoExtensionByteView* result) {
    return static_cast<DocumentSourceExtension::SourceAggregationStageExecutor*>(executor)->getNext(
        code, result);
}

const MongoExtensionAggregationStageVTable
    DocumentSourceExtension::SourceAggregationStageExecutor::VTABLE = {
        // NB: this method does nothing as a DocumentSource does not own pSource
        [](MongoExtensionAggregationStage*) {},
        &_mongoSourceAggregationStageExecutorGetNext,
};

DocumentSourceExtension::ServerExceptionError::ServerExceptionError(std::exception_ptr&& e,
                                                                    int code)
    : _e(std::move(e)), _code(code) {
    this->vtable = &VTABLE;
}

// static
std::exception_ptr DocumentSourceExtension::ServerExceptionError::extractException(
    MongoExtensionError& err) {
    if (err.vtable == &VTABLE) {
        return std::move(static_cast<ServerExceptionError*>(&err)->_e);
    } else {
        return nullptr;
    }
}

extern "C" void _mongoServerExceptionExtensionErrorDrop(MongoExtensionError* error) {
    delete static_cast<DocumentSourceExtension::ServerExceptionError*>(error);
}

extern "C" int _mongoServerExceptionExtensionErrorCode(const MongoExtensionError* error) {
    return static_cast<const DocumentSourceExtension::ServerExceptionError*>(error)->code();
}

extern "C" MongoExtensionByteView _mongoServerExceptionExtensionErrorReason(
    const MongoExtensionError* error) {
    // TODO: determine if we need to make additional exception context beyond code available to
    // extensions.
    return MongoExtensionByteView{nullptr, 0};
}

const MongoExtensionErrorVTable DocumentSourceExtension::ServerExceptionError::VTABLE = {
    &_mongoServerExceptionExtensionErrorDrop,
    &_mongoServerExceptionExtensionErrorCode,
    &_mongoServerExceptionExtensionErrorReason,
};

}  // namespace mongo
