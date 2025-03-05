#include "mongo/db/pipeline/document_source_plugin.h"

#include <cstdint>

#include "mongo/base/init.h"  // IWYU pragma: keep
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/pipeline/plugin/plugin.h"

namespace mongo {

namespace {

StringData byteViewAsStringData(const MongoExtensionByteView view) {
    return StringData(reinterpret_cast<const char*>(view.data), view.len);
}

StringData byteBufAsStringData(const MongoExtensionByteBuf& buf) {
    return byteViewAsStringData(buf.vtable->get(&buf));
}

void addDesugarStage(MongoExtensionByteView name, MongoExtensionParseDesugarStage parser) {
    auto name_sd = byteViewAsStringData(name);
    auto id = DocumentSource::allocateId(name_sd);
    LiteParsedDocumentSource::registerParser(name_sd.toString(),
                                             LiteParsedDocumentSourceDefault::parse,
                                             AllowedWithApiStrict::kAlways,
                                             AllowedWithClientType::kAny);
    DocumentSource::registerParser(
        name_sd.toString(),
        [id, parser](BSONElement specElem, const boost::intrusive_ptr<ExpressionContext>& expCtx)
            -> std::list<boost::intrusive_ptr<DocumentSource>> {
            BSONObj stage_def = specElem.wrap();
            struct ResultDeleter {
                void operator()(MongoExtensionByteBuf* buf) {
                    buf->vtable->drop(buf);
                }
            };
            MongoExtensionByteBuf* result_ptr = nullptr;
            int code = parser(
                MongoExtensionByteView{reinterpret_cast<const unsigned char*>(stage_def.objdata()),
                                       static_cast<size_t>(stage_def.objsize())},
                &result_ptr);
            std::unique_ptr<MongoExtensionByteBuf, ResultDeleter> result(result_ptr);
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

void addAggregationStage(MongoExtensionByteView name, MongoExtensionParseAggregationStage parser) {
    auto name_sd = byteViewAsStringData(name);
    auto id = DocumentSource::allocateId(name_sd);
    LiteParsedDocumentSource::registerParser(name_sd.toString(),
                                             LiteParsedDocumentSourceDefault::parse,
                                             AllowedWithApiStrict::kAlways,
                                             AllowedWithClientType::kAny);
    DocumentSource::registerParser(
        name_sd.toString(),
        [id, parser](BSONElement specElem, const boost::intrusive_ptr<ExpressionContext>& expCtx)
            -> boost::intrusive_ptr<DocumentSource> {
            BSONObj stage_def = specElem.wrap();
            mongodb_aggregation_stage* stage = nullptr;
            MongoExtensionByteBuf* error = nullptr;
            int code = parser(
                MongoExtensionByteView{reinterpret_cast<const unsigned char*>(stage_def.objdata()),
                                       static_cast<size_t>(stage_def.objsize())},
                &stage,
                &error);
            uassert(code, str::stream() << byteBufAsStringData(*error), code == 0);
            return boost::intrusive_ptr(
                new DocumentSourcePlugin(specElem.fieldNameStringData(), expCtx, id, stage));
        },
        boost::none);
}

bool is_valid_bson_document(const unsigned char* bson_value, size_t bson_value_len) {
    if (bson_value_len < BSONObj::kMinBSONLength) {
        return false;
    }

    // Decode the value in little-endian order.
    int32_t document_len = (int32_t)bson_value[0] | ((int32_t)bson_value[1] << 8) |
        ((int32_t)bson_value[2] << 16) | ((int32_t)bson_value[3] << 24);
    return document_len >= 0 && (size_t)document_len == bson_value_len;
}

}  // anonymous namespace

MONGO_INITIALIZER_GENERAL(addToDocSourceParserMap_plugin,
                          ("BeginDocumentSourceRegistration"),
                          ("EndDocumentSourceRegistration"))
(InitializerContext*) {
    MongoExtensionPortal portal;
    portal.version = MONGODB_PLUGIN_VERSION_0;
    portal.add_desugar_stage = &addDesugarStage;
    portal.add_aggregation_stage = &addAggregationStage;
    mongodb_initialize_plugin(&portal);
}

int source_get_next(void* source_ptr, const unsigned char** result, size_t* len) {
    return reinterpret_cast<DocumentSourcePlugin*>(source_ptr)->sourceGetNext(result, len);
}

void DocumentSourcePlugin::setSource(DocumentSource* source) {
    pSource = source;
    _plugin_stage->vtable->set_source(_plugin_stage.get(), this, &source_get_next);
}

int DocumentSourcePlugin::sourceGetNext(const unsigned char** result, size_t* len) {
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

DocumentSource::GetNextResult DocumentSourcePlugin::doGetNext() {
    MongoExtensionByteView result{nullptr, 0};
    int code = _plugin_stage->vtable->get_next(_plugin_stage.get(), &result);
    switch (code) {
        case GET_NEXT_ADVANCED:
            // Ensure that the result buffer contains a document bound to result_len.
            tassert(123456,
                    str::stream() << "plugin returned an invalid BSONObj.",
                    is_valid_bson_document(result.data, result.len));
            return GetNextResult(Document(BSONObj(reinterpret_cast<const char*>(result.data))));
        case GET_NEXT_EOF:
            return GetNextResult::makeEOF();
        case GET_NEXT_PAUSE_EXECUTION:
            return GetNextResult::makePauseExecution();
        default:
            uassert(code, str::stream() << byteViewAsStringData(result), false);
            return GetNextResult::makeEOF();
    }
}

}  // namespace mongo