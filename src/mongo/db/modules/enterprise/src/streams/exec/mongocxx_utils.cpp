/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/mongocxx_utils.h"

#include "mongo/db/service_context.h"

#include <mongocxx/exception/exception.hpp>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

namespace {

static const auto _decoration =
    ServiceContext::declareDecoration<std::unique_ptr<mongocxx::instance>>();
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

}  // namespace streams
