/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/mongocxx_utils.h"

#include <boost/algorithm/string/replace.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/uri.hpp>

#include "mongo/db/service_context.h"
#include "streams/exec/context.h"
#include "streams/util/exception.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

namespace {

static const auto _decoration =
    ServiceContext::declareDecoration<std::unique_ptr<mongocxx::instance>>();
}

std::string sanitizeMongocxxErrorMsg(const std::string& msg, const mongocxx::uri& uri) {
    // mongocxx will include the URI in some errors messages. In Atlas Stream Processing
    // that URI will include internal mesh details, which we don't want to return to customers.
    // So we remove all the URI from the message.
    auto sanitized = msg;
    for (const auto& host : uri.hosts()) {
        boost::replace_all(sanitized, host.name, "");
        boost::replace_all(sanitized, std::to_string(host.port), "");
    }
    return sanitized;
}

mongocxx::instance* getMongocxxInstance(ServiceContext* svcCtx) {
    auto& mongocxxInstance = _decoration(svcCtx);
    static std::once_flag initOnce;
    std::call_once(initOnce, [&]() {
        dassert(!mongocxxInstance);
        mongocxxInstance = std::make_unique<mongocxx::instance>();
    });
    return mongocxxInstance.get();
}

MongoCxxClientOptions::MongoCxxClientOptions(const mongo::AtlasConnectionOptions& atlasOptions) {
    uri = atlasOptions.getUri().toString();

    if (auto pemFileOption = atlasOptions.getPemFile()) {
        pemFile = pemFileOption->toString();
    }

    if (auto caFileOption = atlasOptions.getCaFile()) {
        caFile = caFileOption->toString();
    }

    if (!caFile.empty()) {
        uassert(ErrorCodes::InvalidOptions,
                "Must specify 'pemFile' when 'caFile' is specified",
                !pemFile.empty());
    }
}

mongocxx::options::client MongoCxxClientOptions::toMongoCxxClientOptions() const {
    mongocxx::options::client clientOptions;
    if (!caFile.empty()) {
        tassert(7847300, "'pemFile' must be specified when 'caFile' is provided", !pemFile.empty());
    }

    if (!pemFile.empty()) {
        mongocxx::options::tls tlsOptions;
        tlsOptions.pem_file(pemFile);
        if (!caFile.empty()) {
            tlsOptions.ca_file(caFile);
        }

        clientOptions.tls_opts(tlsOptions);
    }
    return clientOptions;
}

bsoncxx::document::value callHello(mongocxx::database& db) {
    return db.run_command(make_document(kvp("hello", "1")));
}

SPStatus runMongocxxNoThrow(std::function<void()> func,
                            Context* context,
                            mongo::ErrorCodes::Error genericErrorCode,
                            const std::string& genericErrorMsg,
                            const mongocxx::uri& uri) {
    try {
        func();
        return {Status::OK()};
    } catch (const DBException& e) {
        // Our code throws DBExceptions, so we treat these errors as safe to return to customers.
        auto status = e.toStatus();
        LOGV2_INFO(genericErrorCode,
                   "mongocxx request failed with DBException",
                   "genericErrorMsg"_attr = genericErrorMsg,
                   "genericErrorCode"_attr = int(genericErrorCode),
                   "context"_attr = context->toBSON(),
                   "code"_attr = status.code(),
                   "reason"_attr = status.reason(),
                   "exception"_attr = e.what());
        return {e.toStatus()};
    } catch (const mongocxx::exception& e) {
        // Some mongocxx::exceptions need to be sanitized to return to customers.
        auto code = e.code().value();
        auto what = e.what();
        LOGV2_INFO(genericErrorCode,
                   "mongocxx request failed with mongocxx::exception",
                   "genericErrorMsg"_attr = genericErrorMsg,
                   "genericErrorCode"_attr = int(genericErrorCode),
                   "context"_attr = context->toBSON(),
                   "code"_attr = code,
                   "exception"_attr = what);

        auto safeError =
            fmt::format("{}: {}", genericErrorMsg, sanitizeMongocxxErrorMsg(what, uri));
        return {{ErrorCodes::Error{code}, safeError}, what};
    } catch (const std::exception& e) {
        // std::exceptions are not expected and might indicate an InternalError.
        LOGV2_INFO(genericErrorCode,
                   "mongocxx request failed with std::exception",
                   "genericErrorMsg"_attr = genericErrorMsg,
                   "genericErrorCode"_attr = int(genericErrorCode),
                   "context"_attr = context->toBSON(),
                   "exception"_attr = e.what());
        return {Status{ErrorCodes::InternalError, genericErrorMsg}, e.what()};
    }
}

std::unique_ptr<mongocxx::uri> makeMongocxxUri(const std::string& uri) {
    try {
        return std::make_unique<mongocxx::uri>(uri);
    } catch (const std::exception& e) {
        LOGV2_WARNING(8733400,
                      "Exception thrown parsing atlas uri for mongocxx",
                      "exception"_attr = e.what());
        // We get URIs from the streams Agent, not directly from the customer, so this is an
        // InternalError.
        uasserted(ErrorCodes::InternalError, "Invalid atlas uri.");
    }
}

boost::optional<write_ops::WriteError> getWriteErrorFromRawServerError(
    const mongocxx::operation_exception& ex) {
    using namespace mongo::write_ops;
    using namespace fmt::literals;
    const auto& rawServerError = ex.raw_server_error();
    if (!rawServerError || rawServerError->find(kWriteErrorsFieldName) == rawServerError->end()) {
        return boost::none;
    }

    // Convenience lambda to log the original exception and uassert if something in
    // getWriteErrorIndexFromRawServerError fails.
    auto logAndUassert =
        [&ex, &rawServerError](ErrorCodes::Error code, const std::string& msg, bool expr) {
            if (!expr) {
                LOGV2_WARNING(8807400,
                              "Error in getWriteErrorIndexFromRawServerError",
                              "originalWhat"_attr = ex.what(),
                              "originalCode"_attr = ex.code().value(),
                              "rawServerError"_attr = bsoncxx::to_json(*rawServerError),
                              "errorInGetWriteError"_attr = msg,
                              "codeInGetWriteError"_attr = int(code));
                uasserted(code, msg);
            }
        };

    // Here is the expected schema of 'rawServerError':
    // https://github.com/mongodb/specifications/blob/master/source/driver-bulk-update.rst#merging-write-errors
    auto rawServerErrorObj = fromBsoncxxDocument(*rawServerError);

    // Extract write error indexes.
    auto writeErrorsVec = rawServerErrorObj[kWriteErrorsFieldName].Array();
    if (writeErrorsVec.empty()) {
        // An empty writeErrors can correspond to auth failures or other issues.
        return boost::none;
    }
    constexpr auto writeErrorLess = [](const mongo::write_ops::WriteError& lhs,
                                       const mongo::write_ops::WriteError& rhs) {
        return lhs.getIndex() < rhs.getIndex();
    };
    std::set<WriteError, decltype(writeErrorLess)> writeErrors;
    for (auto& writeErrorElem : writeErrorsVec) {
        writeErrors.insert(WriteError::parse(writeErrorElem.embeddedObject()));
    }
    logAndUassert(ErrorCodes::InternalError,
                  "bulk_write_exception::raw_server_error() contains duplicate entries in the "
                  "'{}' field"_format(kWriteErrorsFieldName),
                  writeErrors.size() == writeErrorsVec.size());

    // Since we apply the writes in ordered manner there should only be 1 failed write and all the
    // writes before it should have succeeded.
    logAndUassert(ErrorCodes::InternalError,
                  str::stream() << "bulk_write_exception::raw_server_error() contains unexpected ("
                                << writeErrors.size() << ") number of write error",
                  writeErrors.size() == 1);

    // Extract upserted indexes.
    auto upserted = rawServerErrorObj[UpdateCommandReply::kUpsertedFieldName];
    std::set<size_t> upsertedIndexes;
    if (!upserted.eoo()) {
        auto upsertedVec = upserted.Array();
        for (auto& upsertedItem : upsertedVec) {
            upsertedIndexes.insert(upsertedItem[Upserted::kIndexFieldName].Int());
        }
        logAndUassert(ErrorCodes::InternalError,
                      "bulk_write_exception::raw_server_error() contains duplicate entries in the "
                      "'{}' field"_format(UpdateCommandReply::kUpsertedFieldName),
                      upsertedIndexes.size() == upsertedVec.size());
        logAndUassert(ErrorCodes::InternalError,
                      str::stream()
                          << "unexpected number of upserted indexes (" << upsertedIndexes.size()
                          << " vs " << writeErrors.size() << ")",
                      upsertedIndexes.size() == size_t(writeErrors.begin()->getIndex()));
        size_t i = 0;
        for (auto idx : upsertedIndexes) {
            logAndUassert(ErrorCodes::InternalError,
                          str::stream()
                              << "unexpected upserted index value (" << idx << " vs " << i << ")",
                          idx == i);
            ++i;
        }
    }
    return *writeErrors.begin();
}


}  // namespace streams
