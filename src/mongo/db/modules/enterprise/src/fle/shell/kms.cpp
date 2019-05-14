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

std::unique_ptr<KMSService> KMSServiceController::createFromClient(StringData kmsProvider,
                                                                   const BSONObj& config) {
    KMSProviderEnum provider =
        KMSProvider_parse(IDLParserErrorContext("client fle options"), kmsProvider);

    auto service = _factories.at(provider)->create(config);
    uassert(51192, str::stream() << "Cannot find client kms provider " << kmsProvider, service);
    return service;
}

std::unique_ptr<KMSService> KMSServiceController::createFromDisk(const BSONObj& config,
                                                                 const BSONObj& masterKey) {
    auto providerObj = masterKey.getStringField("provider"_sd);
    auto provider = KMSProvider_parse(IDLParserErrorContext("root"), providerObj);
    auto service = _factories.at(provider)->create(config);
    uassert(51193, str::stream() << "Cannot find disk kms provider " << providerObj, service);
    return service;
}

}  // namespace mongo
