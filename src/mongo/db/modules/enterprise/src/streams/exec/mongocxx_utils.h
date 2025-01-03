/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/apm.hpp>

#include "mongo/bson/bsonobj.h"
#include "streams/exec/stages_gen.h"
#include "streams/util/exception.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

struct Context;

// Error code returned by the atlas proxy, which checks for authentication issues and whether
// or not certain stages are supported (e.g. change stream). Atlas proxy typically produces
// user friendly error messages.
//
// This error code is based on whats registered in the atlasproxy codebase here:
// https://github.com/10gen/atlasproxy/blob/12c7507315dc5d20d4d03aedc1275697d1d65c7e/session_proxy.go#L36-L37
static constexpr int kAtlasErrorCode = 8000;

static const std::string kWriteErrorsFieldName =
    mongo::write_ops::WriteCommandReplyBase::kWriteErrorsFieldName.toString();

/**
 * Struct containing options commonly used to configure a client using the mongocxx driver.
 */
struct MongoCxxClientOptions {
    MongoCxxClientOptions() = default;
    MongoCxxClientOptions(const mongo::AtlasConnectionOptions& atlasOptions,
                          const Context* context);

    // Utility which produces options for the cxx driver.
    mongocxx::options::client toMongoCxxClientOptions() const;

    mongo::ServiceContext* svcCtx{nullptr};
    std::string uri;
    boost::optional<std::string> database;
    boost::optional<std::string> collection;
    std::string pemFile;
    std::string caFile;
    std::vector<std::string> collectionList;

    boost::optional<mongocxx::options::apm> apmOptions;
};

// There should only be 1 mongocxx::instance object per process.
mongocxx::instance* getMongocxxInstance(mongo::ServiceContext* svcCtx);

/**
 * Converts a BSONObj to a bsoncxx::document::view which does not own the underlying BSON buffer. If
 * a 'bsoncxx::document::value' which owns the BSON buffer is needed, use toBsoncxxValue() instead.
 */
inline bsoncxx::document::view toBsoncxxView(const mongo::BSONObj& obj) {
    return bsoncxx::document::view(reinterpret_cast<const uint8_t*>(obj.objdata()), obj.objsize());
}

/**
 * Converts a BSONObj to a bsoncxx::document::value which owns the underlying BSON buffer. If a
 * 'bsoncxx::document::view' which does not own the BSON buffer is needed, use toBsoncxxView()
 * instead.
 */
inline bsoncxx::document::value toBsoncxxValue(const mongo::BSONObj& obj) {
    return bsoncxx::document::value(toBsoncxxView(obj));
}

template <class T>
requires std::is_convertible_v<T, bsoncxx::document::view> mongo::BSONObj fromBsoncxxDocument(
    T value) {
    bsoncxx::document::view view = value;
    return mongo::BSONObj(reinterpret_cast<const char*>(view.data())).getOwned();
}

/**
 * Makes a runCommand({hello: 1}) call to the target server.
 * The runCommand will be retried if there are operation_exceptions thrown. For the last attempt,
 * operation_exceptions are not caught and should be handled by the caller.
 */
bsoncxx::document::value callHello(mongocxx::database& db);

/**
 * Wraps a func that might return mongocxx exceptions. Sanitizes the error message so that the
 * returned Status is always safe to return to customers.
 *
 * The genericErrorMessage argument is used for the Status::reason, plus the sanitized message from
 * mongocxx. The genericErrorCode argument is used as the Status::code and code for LOGV2
 * statements.
 */
SPStatus runMongocxxNoThrow(std::function<void()> func,
                            Context* context,
                            mongo::ErrorCodes::Error genericErrorCode,
                            const std::string& genericErrorMsg,
                            const mongocxx::uri& uri);

/*
 * Translates a mongocxx exception into an SPStatus. An SPStatus contains a code,
 * user error message, and internal error message.
 */
SPStatus mongocxxExceptionToStatus(const mongocxx::exception& ex,
                                   const mongocxx::uri& uri,
                                   const std::string& errorPrefix);

/*
 * Creates a mongocxx uri instance. Might throw a DBException if the URI is malformed.
 */
std::unique_ptr<mongocxx::uri> makeMongocxxUri(const std::string& uri);

/**
 * Returns a WriteError from a server error, if it is a WriteError. Will return none if the
 * exception is not a WriteError.
 */
boost::optional<mongo::write_ops::WriteError> getWriteErrorFromRawServerError(
    const mongocxx::operation_exception& ex);

}  // namespace streams
