#include "src/mongo/db/pipeline/plugin/document_source_plugin.h"

#include "mongo/base/init.h"  // IWYU pragma: keep

namespace mongo {

static void add_aggregation_stage(const char* name,
                                  size_t name_len,
                                  mongodb_parse_aggregation_stage parser) {
    auto name_sd = StringData(name, name_len);
    auto id = DocumentSource::allocateId(name_sd);
    LiteParsedDocumentSource::registerParser(name_sd.toString(),
                                             LiteParsedDocumentSourceDefault::parse,
                                             AllowedWithApiStrict::kAlways,
                                             AllowedWithClientType::kAny);
    DocumentSource::registerParser(
        name_sd.toString(),
        [id, parser](BSONElement specElem, const boost::intrusive_ptr<ExpressionContext>& expCtx)
            -> boost::intrusive_ptr<DocumentSource> {
            mongodb_aggregation_stage* stage = nullptr;
            const char* error = nullptr;
            size_t error_len = 0;
            int code = parser((char)specElem.type(),
                              specElem.value(),
                              specElem.valuesize(),
                              &stage,
                              &error,
                              &error_len);
            uassert(code, str::stream() << StringData(error, error_len), code == 0);
            return boost::intrusive_ptr(
                new DocumentSourcePlugin(specElem.fieldNameStringData(), expCtx, id, stage));
        },
        boost::none);
}

MONGO_INITIALIZER_GENERAL(addToDoSourceParserMap_plugin,
                          ("BeginDocumentSourceRegistration"),
                          ("EndDocumentSourceRegistration"))
(InitializerContext*) {
    mongodb_plugin_portal portal;
    portal.version = MONGODB_PLUGIN_VERSION_0;
    portal.add_aggregation_stage = &add_aggregation_stage;
    mongodb_initialize_plugin(&portal);
}

DocumentSource::GetNextResult DocumentSourcePlugin::doGetNext() {
    const char* result = nullptr;
    size_t result_len = 0;
    int code = _plugin_stage->get_next(_plugin_stage.get(), &result, &result_len);
    switch (code) {
        case GET_NEXT_ADVANCED:
            // XXX BSONObj() accepts a raw pointer without a length which gives me the ick.
            // Confirm that the 32-bit value at the start of result matches result_len before
            // currying this into a Document.
            return GetNextResult(Document(BSONObj(result)));
        case GET_NEXT_EOF:
            return GetNextResult::makeEOF();
        case GET_NEXT_PAUSE_EXECUTION:
            return GetNextResult::makePauseExecution();
        default:
            uassert(code, str::stream() << StringData(result, result_len), false);
            return GetNextResult::makeEOF();
    }
}

}  // namespace mongo