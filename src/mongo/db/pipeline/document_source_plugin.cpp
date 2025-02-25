#include "mongo/db/pipeline/document_source_plugin.h"

#include <cstdint>

#include "mongo/base/init.h"  // IWYU pragma: keep
#include "mongo/bson/bsonobj.h"
#include "mongo/db/pipeline/plugin/plugin.h"

namespace mongo {

static void add_aggregation_stage(MongoExtensionByteView name,
                                  MongoExtensionParseAggregationStage parser) {
    auto name_sd = StringData(reinterpret_cast<const char*>(name.data), name.len);
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
            const unsigned char* error = nullptr;
            size_t error_len = 0;
            int code = parser(
                MongoExtensionByteView{reinterpret_cast<const unsigned char*>(stage_def.objdata()),
                                       static_cast<size_t>(stage_def.objsize())},
                &stage,
                &error,
                &error_len);
            uassert(code,
                    str::stream() << StringData(reinterpret_cast<const char*>(error), error_len),
                    code == 0);
            return boost::intrusive_ptr(
                new DocumentSourcePlugin(specElem.fieldNameStringData(), expCtx, id, stage));
        },
        boost::none);
}

static bool is_valid_bson_document(const unsigned char* bson_value, size_t bson_value_len) {
    if (bson_value_len < BSONObj::kMinBSONLength) {
        return false;
    }

    // Decode the value in little-endian order.
    int32_t document_len = (int32_t)bson_value[0] | ((int32_t)bson_value[1] << 8) |
        ((int32_t)bson_value[2] << 16) | ((int32_t)bson_value[3] << 24);
    return document_len >= 0 && (size_t)document_len == bson_value_len;
}

MONGO_INITIALIZER_GENERAL(addToDocSourceParserMap_plugin,
                          ("BeginDocumentSourceRegistration"),
                          ("EndDocumentSourceRegistration"))
(InitializerContext*) {
    MongoExtensionPortal portal;
    portal.version = MONGODB_PLUGIN_VERSION_0;
    portal.add_aggregation_stage = &add_aggregation_stage;
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
    const unsigned char* result = nullptr;
    size_t result_len = 0;
    int code = _plugin_stage->vtable->get_next(_plugin_stage.get(), &result, &result_len);
    switch (code) {
        case GET_NEXT_ADVANCED:
            // Ensure that the result buffer contains a document bound to result_len.
            tassert(123456,
                    str::stream() << "plugin returned an invalid BSONObj.",
                    is_valid_bson_document(result, result_len));
            return GetNextResult(Document(BSONObj(reinterpret_cast<const char*>(result))));
        case GET_NEXT_EOF:
            return GetNextResult::makeEOF();
        case GET_NEXT_PAUSE_EXECUTION:
            return GetNextResult::makePauseExecution();
        default:
            uassert(code,
                    str::stream() << StringData(reinterpret_cast<const char*>(result), result_len),
                    false);
            return GetNextResult::makeEOF();
    }
}

}  // namespace mongo