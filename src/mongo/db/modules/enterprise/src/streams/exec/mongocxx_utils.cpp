/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/mongocxx_utils.h"
#include "mongo/db/json.h"
#include "mongo/db/service_context.h"
#include "mongo/platform/basic.h"

#include <bsoncxx/json.hpp>

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

// TODO: Current implementation is quite inefficient as we convert to json first.
bsoncxx::document::value toBsoncxxDocument(const BSONObj& obj) {
    return bsoncxx::from_json(tojson(obj));
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

}  // namespace streams
