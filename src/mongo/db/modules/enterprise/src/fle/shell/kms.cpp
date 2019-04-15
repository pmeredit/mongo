/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "kms.h"

#include "fle/shell/kms_gen.h"
#include "mongo/platform/random.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/text.h"


namespace mongo {

HostAndPort parseUrl(StringData url) {
    // Treat the URL as a host and port
    // URL: https://(host):(port)
    //
    constexpr StringData urlPrefix = "https://"_sd;
    uassert(51140, "AWS KMS URL must start with https://", url.startsWith(urlPrefix));

    StringData hostAndPort = url.substr(urlPrefix.size());

    return HostAndPort(hostAndPort);
}

stdx::unordered_map<KMSProviderEnum, std::unique_ptr<KMSServiceFactory>>
    KMSServiceController::_factories;

void KMSServiceController::registerFactory(KMSProviderEnum provider,
                                           std::unique_ptr<KMSServiceFactory> factory) {
    auto ret = _factories.insert({provider, std::move(factory)});
    invariant(ret.second);
}

std::unique_ptr<KMSService> KMSServiceController::createFromClient(const BSONObj& config) {
    for (auto && [ _, factory ] : _factories) {
        std::unique_ptr<KMSService> kmsService = factory->create(config);
        if (kmsService != nullptr) {
            return kmsService;
        }
    }
    uasserted(31039, "Unsupported kms type.");
}

std::unique_ptr<KMSService> KMSServiceController::createFromDisk(const BSONObj& config,
                                                                 const BSONObj& masterKey) {
    auto providerObj = masterKey.getStringField("provider"_sd);
    auto provider = KMSProvider_parse(IDLParserErrorContext("root"), providerObj);
    return _factories.at(provider)->create(config);
}

}  // namespace mongo
