#include "mongo/db/pipeline/document_source_extension.h"

#include <cstdint>
#include <mutex>
#include <shared_mutex>

#include "absl/base/nullability.h"
#include "mongo/base/init.h"  // IWYU pragma: keep
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/pipeline/document_source_plugin.h"
#include "mongo/db/pipeline/plugin/plugin.h"
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
    return b.obj();
}

}  // anonymous namespace

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

            // TODO: defer creation of agg stage until the first getNext() call.
            MongoExtensionAggregationStage* executorPtr = nullptr;
            code = boundDescriptor->vtable->createExecutor(
                boundDescriptor.get(), &executorPtr, &errorPtr);
            auto executor = ExecutorPtr(executorPtr);
            error = ByteBufPtr(errorPtr);
            uassert(code, str::stream() << byteBufAsStringData(*error), code == 0);

            return boost::intrusive_ptr(new DocumentSourceExtension(specElem.fieldNameStringData(),
                                                                    expCtx,
                                                                    id,
                                                                    std::move(stageDef),
                                                                    descriptor,
                                                                    std::move(boundDescriptor),
                                                                    std::move(executor)));
        },
        boost::none);
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
        boost::none);
}

// static
int DocumentSourceExtension::externalSourceGetNext(void* source_ptr,
                                                   const unsigned char** result,
                                                   size_t* len) {
    return reinterpret_cast<DocumentSourceExtension*>(source_ptr)->sourceGetNext(result, len);
}

void DocumentSourceExtension::setSource(DocumentSource* source) {
    pSource = source;
    _executor->vtable->set_source(
        _executor.get(), this, &DocumentSourceExtension::externalSourceGetNext);
}

int DocumentSourceExtension::sourceGetNext(const unsigned char** result, size_t* len) {
    // TODO: if this call throws we need to turn it into an error. As-is an exception would unwind
    // through extension stack frames. This is fine for the rust SDK because they use C-unwind ABI
    // but arbitrary extensions might have a problem.
    GetNextResult get_next_result = pSource->getNext();
    switch (get_next_result.getStatus()) {
        case GetNextResult::ReturnStatus::kAdvanced:
            _source_doc = get_next_result.releaseDocument().toBson();
            *result = reinterpret_cast<const unsigned char*>(_source_doc.objdata());
            *len = _source_doc.objsize();
            return GET_NEXT_ADVANCED;
        case GetNextResult::ReturnStatus::kEOF:
            *result = nullptr;
            *len = 0;
            return GET_NEXT_EOF;
        case GetNextResult::ReturnStatus::kPauseExecution:
            *result = nullptr;
            *len = 0;
            return GET_NEXT_PAUSE_EXECUTION;
    }
}

DocumentSource::GetNextResult DocumentSourceExtension::doGetNext() {
    MongoExtensionByteView result{nullptr, 0};
    int code = _executor->vtable->get_next(_executor.get(), &result);
    switch (code) {
        case GET_NEXT_ADVANCED:
            return GetNextResult(Document::fromBsonWithMetaData(bsonObjFromByteView(result)));
        case GET_NEXT_EOF:
            return GetNextResult::makeEOF();
        case GET_NEXT_PAUSE_EXECUTION:
            return GetNextResult::makePauseExecution();
        default:
            uassert(code, str::stream() << byteViewAsStringData(result), false);
            return GetNextResult::makeEOF();
    }
}

boost::optional<DocumentSource::DistributedPlanLogic>
DocumentSourceExtension::distributedPlanLogic() {
    // TODO Can the plugin modify "shardsStages"?
    // TODO Handle stages that must execute on the merging node ($voyageRerank)
    // TODO If first stage is a $sort, we should extract the sort pattern for the
    // "mergeSortPattern" field.
    // TODO More potential optimization through "needsSplit"/"canMovePast" fields for $search.

    MongoExtensionByteBuf* result_ptr = nullptr;
    _executor->vtable->get_merging_stages(_executor.get(), &result_ptr);
    std::unique_ptr<MongoExtensionByteBuf, ExtensionObjectDeleter> result(result_ptr);
    BSONObj mergeStagesBson(byteBufAsStringData(*result).data());

    auto elem = mergeStagesBson.firstElement();
    std::list<boost::intrusive_ptr<DocumentSource>> merge_sources;
    uassert(9999999,
            "Merging stages provided by extension stage must be array.",
            elem.type() == BSONType::Array);
    for (auto stageElem : elem.embeddedObject()) {
        for (auto stage : DocumentSource::parse(pExpCtx, stageElem.embeddedObject())) {
            merge_sources.push_back(stage);
        }
    }

    DistributedPlanLogic logic;
    logic.shardsStage = this;
    logic.mergingStages = std::move(merge_sources);

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
    return StageConstraints(propertiesStreamType(properties),
                            propertiesPosition(properties),
                            propertiesHostType(properties),
                            DiskUseRequirement::kNoDiskUse,
                            FacetRequirement::kNotAllowed,
                            TransactionRequirement::kNotAllowed,
                            LookupRequirement::kNotAllowed,
                            UnionRequirement::kNotAllowed,
                            ChangeStreamRequirement::kDenylist);
}

}  // namespace mongo
