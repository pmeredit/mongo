/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo_crypt.h"

#include <algorithm>
#include <string>

#include "fle/query_analysis/query_analysis.h"
#include "mongo/base/initializer.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/api_parameters.h"
#include "mongo/db/commands/query_cmd/explain_gen.h"
#include "mongo/db/exec/projection_executor.h"
#include "mongo/db/exec/projection_executor_builder.h"
#include "mongo/db/matcher/matcher.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/query/projection_parser.h"
#include "mongo/db/query/write_ops/parsed_update_array_filters.h"
#include "mongo/db/service_context.h"
#include "mongo/db/update/update_driver.h"
#include "mongo/embedded/api_common.h"
#include "mongo/logv2/log_domain_global.h"
#include "mongo/logv2/log_manager.h"
#include "mongo/rpc/op_msg_rpc_impls.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/version.h"
#include "mongo/util/version_constants.h"

#if defined(_WIN32)
#define MONGO_API_CALL __cdecl
#else
#define MONGO_API_CALL
#endif

namespace mongo {

using MongoCryptSupportStatusImpl = StatusForAPI<mongo_crypt_v1_error>;

const NamespaceString kDummyNamespaceStr = NamespaceString::kEmpty;
constexpr auto kExplainField = "explain"_sd;
constexpr auto kResultField = "result"_sd;
constexpr auto kVerbosityField = "verbosity"_sd;

/**
 * C interfaces that use enterCXX() must provide a translateException() function that converts any
 * possible exception into a StatusForAPI<> object.
 */
static MongoCryptSupportStatusImpl translateException(
    stdx::type_identity<MongoCryptSupportStatusImpl>) try {
    throw;
} catch (const ExceptionFor<ErrorCodes::ReentrancyNotAllowed>& ex) {
    return {MONGO_CRYPT_V1_ERROR_REENTRANCY_NOT_ALLOWED, ex.code(), ex.what()};
} catch (const DBException& ex) {
    return {MONGO_CRYPT_V1_ERROR_EXCEPTION, ex.code(), ex.what()};
} catch (const ExceptionForAPI<mongo_crypt_v1_error>& ex) {
    return {ex.statusCode(), mongo::ErrorCodes::InternalError, ex.what()};
} catch (const std::bad_alloc& ex) {
    return {MONGO_CRYPT_V1_ERROR_ENOMEM, mongo::ErrorCodes::InternalError, ex.what()};
} catch (const std::exception& ex) {
    return {MONGO_CRYPT_V1_ERROR_UNKNOWN, mongo::ErrorCodes::InternalError, ex.what()};
} catch (...) {
    return {MONGO_CRYPT_V1_ERROR_UNKNOWN,
            mongo::ErrorCodes::InternalError,
            "Unknown error encountered in performing requested mongo_crypt_v1 operation"};
}

/**
 * C interfaces that use enterCXX() must provide a tranlsateExceptionFallback() function that
 * populates a StatusForAPI<> object to indicate a double-fault error during error reporting. The
 * translateExceptionFallback() function gets called when translateException() throws, and it should
 * not include any code that may itself throw.
 *
 * We use an out param instead of returning the StatusForAPI<> object so as to avoid a std::string
 * copy that may allocate memory.
 */
static void translateExceptionFallback(MongoCryptSupportStatusImpl& status) noexcept {
    status.error = MONGO_CRYPT_V1_ERROR_IN_REPORTING_ERROR;
    status.exception_code = -1;
    setErrorMessageNoAlloc(status.what);
}

/**
 * For explain we need to re-wrap the inner command with placeholders inside an explain
 * command.
 */
static void buildExplainReturnMessage(OperationContext* opCtx,
                                      BSONObjBuilder* responseBuilder,
                                      const BSONObj& innerObj,
                                      const ExplainOptions::Verbosity& verbosity) {
    // All successful commands have a result field.
    dassert(innerObj.hasField(kResultField) &&
            innerObj.getField(kResultField).type() == BSONType::Object);

    for (const auto& elem : innerObj) {
        if (elem.fieldNameStringData() == kResultField) {
            // Hoist "result" up into result.explain.
            BSONObjBuilder result(responseBuilder->subobjStart(kResultField));

            // The nested explain object needs to be stripped of the API Version fields,
            // and those fields need to be appended to the result object.
            {
                BSONObj explainObj = elem.Obj();

                std::vector<BSONElement> apiParams;
                apiParams.reserve(3);

                BSONObjBuilder explainBuilder(result.subobjStart(kExplainField));

                for (const auto& elem : explainObj) {
                    if (elem.fieldName() == APIParameters::kAPIVersionFieldName ||
                        elem.fieldName() == APIParameters::kAPIStrictFieldName ||
                        elem.fieldName() == APIParameters::kAPIDeprecationErrorsFieldName) {
                        apiParams.push_back(elem);
                    } else {
                        explainBuilder << elem;
                    }
                }

                explainBuilder.doneFast();

                for (const auto& elem : apiParams) {
                    result << elem;
                }
            }

            // TODO: SERVER-40354 Only send back verbosity if it was sent in the original message.
            result.append(kVerbosityField, ExplainOptions::verbosityString(verbosity));

            result.doneFast();
        } else {
            responseBuilder->append(elem);
        }
    }
}

namespace {
BSONObj analyzeNonExplainQuery(const BSONObj document,
                               OperationContext* opCtx,
                               const NamespaceString ns) {
    mongo::OpMsgRequest opmsg;
    opmsg.body = document;
    if (auto tenant = ns.dbName().tenantId()) {
        opmsg.validatedTenancyScope = auth::ValidatedTenancyScopeFactory::create(
            tenant.value(), auth::ValidatedTenancyScopeFactory::TrustedForInnerOpMsgRequestTag{});
    }
    const StringData commandName = document.firstElementFieldName();

    BSONObjBuilder schemaInfoBuilder;
    if (commandName == "find"_sd) {
        auto db = ns.dbName();
        query_analysis::processFindCommand(
            opCtx, std::move(db), document, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "aggregate"_sd) {
        auto db = ns.dbName();
        query_analysis::processAggregateCommand(
            opCtx, std::move(db), document, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "findandmodify"_sd || commandName == "findAndModify"_sd) {
        auto db = ns.dbName();
        query_analysis::processFindAndModifyCommand(
            opCtx, std::move(db), document, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "count"_sd) {
        auto db = ns.dbName();
        query_analysis::processCountCommand(
            opCtx, std::move(db), document, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "distinct"_sd) {
        auto db = ns.dbName();
        query_analysis::processDistinctCommand(
            opCtx, std::move(db), document, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "create"_sd) {
        auto db = ns.dbName();
        query_analysis::processCreateCommand(
            opCtx, std::move(db), document, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "collMod"_sd) {
        auto db = ns.dbName();
        query_analysis::processCollModCommand(
            opCtx, std::move(db), document, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "createIndexes"_sd) {
        auto db = ns.dbName();
        query_analysis::processCreateIndexesCommand(
            opCtx, std::move(db), document, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "update"_sd) {
        query_analysis::processUpdateCommand(opCtx, opmsg, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "insert"_sd) {
        query_analysis::processInsertCommand(opCtx, opmsg, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "delete"_sd) {
        query_analysis::processDeleteCommand(opCtx, opmsg, &schemaInfoBuilder, std::move(ns));
    } else if (commandName == "bulkWrite"_sd) {
        query_analysis::processBulkWriteCommand(opCtx, opmsg, &schemaInfoBuilder, std::move(ns));
    } else {
        uasserted(mongo::ErrorCodes::CommandNotFound,
                  str::stream() << "Query contains an unknown command: " << commandName);
    }

    return schemaInfoBuilder.obj();
}

BSONObj analyzeExplainQuery(const BSONObj document,
                            OperationContext* opCtx,
                            const NamespaceString ns) {
    auto cleanedCmdObj =
        document.removeFields(StringDataSet{query_analysis::kJsonSchema,
                                            query_analysis::kIsRemoteSchema,
                                            query_analysis::kEncryptionInformation});
    auto explainCmd = idl::parseCommandDocument<ExplainCommandRequest>(
        IDLParserContext(ExplainCommandRequest::kCommandName), cleanedCmdObj);

    auto explainedObj = explainCmd.getCommandParameter();
    uassert(6206601,
            "In an explain command the jsonSchema field must be top-level and not inside the "
            "command being explained.",
            !explainedObj.hasField(query_analysis::kJsonSchema));
    if (auto cmdSchema = document[query_analysis::kJsonSchema]) {
        explainedObj = explainedObj.addField(cmdSchema);
    }

    if (auto apiVersion = document["apiVersion"]) {
        explainedObj = explainedObj.addField(apiVersion);
    }

    if (auto apiDeprecationErrors = document["apiDeprecationErrors"]) {
        explainedObj = explainedObj.addField(apiDeprecationErrors);
    }

    if (auto apiStrict = document["apiStrict"]) {
        explainedObj = explainedObj.addField(apiStrict);
    }

    uassert(6650801,
            "In an explain command the encryptionInformation field cannot both be in the explain "
            "command and inside the command being explained.",
            !document.hasField(query_analysis::kEncryptionInformation) ||
                !explainedObj.hasField(query_analysis::kEncryptionInformation));
    if (auto cmdEncryptInfo = document[query_analysis::kEncryptionInformation]) {
        explainedObj = explainedObj.addField(cmdEncryptInfo);
    }

    uassert(6206602,
            "In an explain command the isRemoteSchema field must be top-level and not inside the "
            "command being explained.",
            !explainedObj.hasField(query_analysis::kIsRemoteSchema));
    if (auto isRemoteSchema = document[query_analysis::kIsRemoteSchema]) {
        explainedObj = explainedObj.addField(isRemoteSchema);
    }
    if (auto innerDb = explainedObj["$db"]) {
        const auto dbName = explainCmd.getDbName();
        uassert(ErrorCodes::InvalidNamespace,
                str::stream() << "Mismatched $db in explain command. Expected "
                              << dbName.toStringForErrorMsg() << " but got "
                              << innerDb.checkAndGetStringData(),
                DatabaseNameUtil::deserialize(dbName.tenantId(),
                                              innerDb.checkAndGetStringData(),
                                              explainCmd.getSerializationContext()) == dbName);
    } else {
        explainedObj = explainedObj.addField(document["$db"]);
    }

    // Analyze the inner query
    StringData innerCmdName = explainedObj.firstElementFieldName();
    if (innerCmdName == "explain"_sd) {
        uasserted(mongo::ErrorCodes::IllegalOperation, "Explain cannot explain itself");
    }

    BSONObj innerResult;

    try {
        innerResult = analyzeNonExplainQuery(explainedObj, opCtx, ns);
    } catch (...) {
        auto error = exceptionToStatus();
        uassert(mongo::ErrorCodes::CommandNotFound,
                str::stream() << "Explain failed due to unknown inner command: " << innerCmdName,
                error.code() != mongo::ErrorCodes::CommandNotFound);
        throw;
    }

    // Build the final response by wrapping the inner result & return
    BSONObjBuilder explainBuilder;
    buildExplainReturnMessage(opCtx, &explainBuilder, innerResult, explainCmd.getVerbosity());
    return explainBuilder.obj();
}
}  // namespace

// This is used in tests but not exposed in a header
BSONObj analyzeQuery(BSONObj document, OperationContext* opCtx, NamespaceString ns);
BSONObj analyzeQuery(const BSONObj document, OperationContext* opCtx, const NamespaceString ns) {
    const StringData commandName = document.firstElementFieldName();
    if (commandName == "explain"_sd) {
        return analyzeExplainQuery(document, opCtx, ns);
    }
    return analyzeNonExplainQuery(document, opCtx, ns);
}

namespace {
uint64_t getMongoCryptVersion() {
    return (static_cast<uint64_t>(version::kMajorVersion) << 48) |
        (static_cast<uint64_t>(version::kMinorVersion) << 32) |
        (static_cast<uint64_t>(version::kPatchVersion) << 16);
}

#ifndef MONGO_DISTMOD
#define MONGO_DISTMOD "dev"
#endif

const char* getMongoCryptVersionStr() {
    static const auto version = "mongo_crypt_v1-" MONGO_DISTMOD "-" + version::kVersion;
    return version.c_str();
}
}  // namespace

}  // namespace mongo

struct mongo_crypt_v1_status {
    mongo::MongoCryptSupportStatusImpl statusImpl;
};

namespace mongo {

namespace {

MongoCryptSupportStatusImpl* getStatusImpl(mongo_crypt_v1_status* status) {
    return status ? &status->statusImpl : nullptr;
}

using MongoCryptSupportException = ExceptionForAPI<mongo_crypt_v1_error>;

ServiceContext* initialize() {
    srand(static_cast<unsigned>(curTimeMicros64()));  // NOLINT

    // Initialize the global logger and disable logging to console.
    auto& logManager = logv2::LogManager::global();
    logv2::LogDomainGlobal::ConfigurationOptions logConfig;
    logConfig.makeDisabled();
    uassertStatusOK(logManager.getGlobalDomainInternal().configure(logConfig));

    // The global initializers can take arguments, which would normally be supplied on the command
    // line, but we assume that clients of this library will never want anything other than the
    // defaults for all configuration that would be controlled by these parameters.
    Status status = mongo::runGlobalInitializers(std::vector<std::string>{});
    uassertStatusOKWithContext(status, "Global initialization failed");
    setGlobalServiceContext(ServiceContext::make());
    auto serviceContext = getGlobalServiceContext();

    // (Generic FCV reference): feature flag support
    serverGlobalParams.mutableFCV.setVersion(multiversion::GenericFCV::kLatest);

    return serviceContext;
}

}  // namespace
}  // namespace mongo

struct mongo_crypt_v1_lib {
    mongo_crypt_v1_lib() : serviceContext(mongo::initialize()) {}

    mongo_crypt_v1_lib(const mongo_crypt_v1_lib&) = delete;
    void operator=(const mongo_crypt_v1_lib&) = delete;

    /**
     * This gets called when the Mongo Crypt Shared Library gets torn down, by a call to
     * mongo_crypt_v1_lib_destroy()
     */
    void free() noexcept {
        if (nullptr != serviceContext) {
            serviceContext = nullptr;

            auto status = mongo::runGlobalDeinitializers();
            uassertStatusOKWithContext(status, "Global deinitilization failed");

            mongo::setGlobalServiceContext(nullptr);
        }
    }

    mongo::ServiceContext* serviceContext;
};

struct mongo_crypt_v1_query_analyzer {
    mongo_crypt_v1_query_analyzer(mongo::ServiceContext::UniqueClient client)
        : client(std::move(client)), opCtx(this->client->makeOperationContext()) {}


    mongo::ServiceContext::UniqueClient client;
    mongo::ServiceContext::UniqueOperationContext opCtx;
};


namespace mongo {
namespace {

std::unique_ptr<mongo_crypt_v1_lib> library;

mongo_crypt_v1_lib* mongo_crypt_lib_init() {
    if (library) {
        throw MongoCryptSupportException{MONGO_CRYPT_V1_ERROR_LIBRARY_ALREADY_INITIALIZED,
                                         "Cannot initialize the Mongo Crypt Shared Support Library "
                                         "when it is already initialized."};
    }

    library = std::make_unique<mongo_crypt_v1_lib>();

    return library.get();
}

void mongo_crypt_lib_fini(mongo_crypt_v1_lib* const lib) {
    if (!lib) {
        throw MongoCryptSupportException{
            MONGO_CRYPT_V1_ERROR_INVALID_LIB_HANDLE,
            "Cannot close a `NULL` pointer referencing a Mongo Crypt Shared Library Instance"};
    }

    if (!library) {
        throw MongoCryptSupportException{
            MONGO_CRYPT_V1_ERROR_LIBRARY_NOT_INITIALIZED,
            "Cannot close the Mongo Crypt Shared Library when it is not initialized"};
    }

    if (library.get() != lib) {
        throw MongoCryptSupportException{MONGO_CRYPT_V1_ERROR_INVALID_LIB_HANDLE,
                                         "Invalid Mongo Crypt Shared Library handle."};
    }

    library->free();
    library.reset();
}

mongo_crypt_v1_query_analyzer* query_analyzer_create(mongo_crypt_v1_lib* const lib) {
    if (!library) {
        throw MongoCryptSupportException{
            MONGO_CRYPT_V1_ERROR_LIBRARY_NOT_INITIALIZED,
            "Cannot create a new collator when the Mongo Crypt Shared Library "
            "is not yet initialized."};
    }

    if (library.get() != lib) {
        throw MongoCryptSupportException{
            MONGO_CRYPT_V1_ERROR_INVALID_LIB_HANDLE,
            "Cannot create a new collator when the Mongo Crypt Shared Library "
            "is not yet initialized."};
    }

    // We leave this client as killable, however since this is in mongo_crypt we never expect it to
    // actually be interrupted during operation. If interrupted, mongo_crypt will likely die.
    auto client = lib->serviceContext->getService()->makeClient("crypt_support");

    return new mongo_crypt_v1_query_analyzer(std::move(client));
}

int capi_status_get_error(const mongo_crypt_v1_status* const status) noexcept {
    invariant(status);
    return status->statusImpl.error;
}

const char* capi_status_get_what(const mongo_crypt_v1_status* const status) noexcept {
    invariant(status);
    return status->statusImpl.what.c_str();
}

int capi_status_get_code(const mongo_crypt_v1_status* const status) noexcept {
    invariant(status);
    return status->statusImpl.exception_code;
}

/**
 * toInterfaceType changes the compiler's interpretation from our internal BSON type 'char*' to
 * 'uint8_t*' which is the interface type of the Mongo Crypt Shared library.
 */
auto toInterfaceType(char* bson) noexcept {
    return static_cast<uint8_t*>(static_cast<void*>(bson));
}

/**
 * fromInterfaceType changes the compiler's interpretation from 'uint8_t*' which is the BSON
 * interface type of the Mongo Crypt Shared library to our internal type 'char*'.
 */
auto fromInterfaceType(const uint8_t* bson) noexcept {
    return static_cast<const char*>(static_cast<const void*>(bson));
}

}  // namespace
}  // namespace mongo

extern "C" {

uint64_t MONGO_API_CALL mongo_crypt_v1_get_version(void) {
    return mongo::getMongoCryptVersion();
}

const char* MONGO_API_CALL mongo_crypt_v1_get_version_str(void) {
    return mongo::getMongoCryptVersionStr();
}

mongo_crypt_v1_lib* MONGO_API_CALL mongo_crypt_v1_lib_create(mongo_crypt_v1_status* status) {
    return enterCXX(mongo::getStatusImpl(status), [&]() { return mongo::mongo_crypt_lib_init(); });
}

int MONGO_API_CALL mongo_crypt_v1_lib_destroy(mongo_crypt_v1_lib* const lib,
                                              mongo_crypt_v1_status* const status) {
    return enterCXX(mongo::getStatusImpl(status),
                    [&]() { return mongo::mongo_crypt_lib_fini(lib); });
}

int MONGO_API_CALL mongo_crypt_v1_status_get_error(const mongo_crypt_v1_status* const status) {
    return mongo::capi_status_get_error(status);
}

const char* MONGO_API_CALL
mongo_crypt_v1_status_get_explanation(const mongo_crypt_v1_status* const status) {
    return mongo::capi_status_get_what(status);
}

int MONGO_API_CALL mongo_crypt_v1_status_get_code(const mongo_crypt_v1_status* const status) {
    return mongo::capi_status_get_code(status);
}

mongo_crypt_v1_status* MONGO_API_CALL mongo_crypt_v1_status_create(void) {
    return new (std::nothrow) mongo_crypt_v1_status;
}

void MONGO_API_CALL mongo_crypt_v1_status_destroy(mongo_crypt_v1_status* const status) {
    delete status;
}

mongo_crypt_v1_query_analyzer* MONGO_API_CALL
mongo_crypt_v1_query_analyzer_create(mongo_crypt_v1_lib* lib, mongo_crypt_v1_status* const status) {
    return enterCXX(mongo::getStatusImpl(status),
                    [&]() { return mongo::query_analyzer_create(lib); });
}

void MONGO_API_CALL
mongo_crypt_v1_query_analyzer_destroy(mongo_crypt_v1_query_analyzer* const collator) {
    mongo::MongoCryptSupportStatusImpl* nullStatus = nullptr;
    static_cast<void>(enterCXX(nullStatus, [=]() { delete collator; }));
}

uint8_t* MONGO_API_CALL mongo_crypt_v1_analyze_query(mongo_crypt_v1_query_analyzer* matcher,
                                                     const uint8_t* documentBSON,
                                                     const char* ns_str,
                                                     uint32_t ns_len,
                                                     uint32_t* bson_len,
                                                     mongo_crypt_v1_status* status) {
    invariant(matcher);
    invariant(documentBSON);
    invariant(bson_len);

    return enterCXX(mongo::getStatusImpl(status), [&]() {
        mongo::BSONObj document(mongo::fromInterfaceType(documentBSON));

        mongo::StringData ns_sd(ns_str, ns_len);
        // we use the `forTest` function here since this is client side code which is not tenant
        // aware nor has access to server parameters.
        mongo::NamespaceString ns = mongo::NamespaceString::createNamespaceString_forTest(ns_sd);
        mongo::OperationContext* opCtx = matcher->opCtx.get();

        auto result = mongo::analyzeQuery(document, opCtx, ns);

        // TODO -cleanup
        auto outputSize = static_cast<size_t>(result.objsize());
        auto output = new (std::nothrow) char[outputSize];

        uassert(mongo::ErrorCodes::ExceededMemoryLimit,
                "Failed to allocate memory for projection",
                output);

        static_cast<void>(std::copy_n(result.objdata(), outputSize, output));
        *bson_len = result.objsize();
        return mongo::toInterfaceType(output);
    });
}


void MONGO_API_CALL mongo_crypt_v1_bson_free(uint8_t* bson) {
    mongo::MongoCryptSupportStatusImpl* nullStatus = nullptr;
    static_cast<void>(enterCXX(nullStatus, [=]() { delete[](bson); }));
}

}  // extern "C"
