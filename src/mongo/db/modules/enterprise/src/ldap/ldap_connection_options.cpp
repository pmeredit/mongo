/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "ldap_connection_options.h"

#include <algorithm>
#include <string>

#include "ldap_host.h"
#include "mongo/base/string_data.h"
#include "mongo/util/str.h"
#include "mongo/util/text.h"

namespace mongo {

namespace {
const StringData kSimple("simple");
const StringData kSasl("sasl");

constexpr auto srvPrefix = "srv:"_sd;
constexpr auto srvRawPrefix = "srv_raw:"_sd;
}  // namespace

StatusWith<LDAPBindType> getLDAPBindType(StringData type) {
    if (type.equalCaseInsensitive(kSimple)) {
        return LDAPBindType::kSimple;
    }
    if (type.equalCaseInsensitive(kSasl)) {
        return LDAPBindType::kSasl;
    }
    return Status(ErrorCodes::FailedToParse,
                  str::stream() << "Unrecognized LDAP bind method: " << type);
}

StringData authenticationChoiceToString(LDAPBindType type) {
    if (type == LDAPBindType::kSimple) {
        return kSimple;
    }
    if (type == LDAPBindType::kSasl) {
        return kSasl;
    }
    // We should never get here, because we should never make a bad authentication type
    MONGO_UNREACHABLE
}

std::string LDAPBindOptions::toCleanString() const {
    StringBuilder builder;
    builder << "{BindDN: " << bindDN
            << ", authenticationType: " << authenticationChoiceToString(authenticationChoice);
    if (authenticationChoice == LDAPBindType::kSasl) {
        builder << ", saslMechanisms: " << saslMechanisms;
    }
    builder << "}";

    return builder.str();
}

StatusWith<std::vector<LDAPHost>> LDAPConnectionOptions::parseHostURIs(const std::string& hosts,
                                                                       bool isSSL) {
    if (hosts.find(' ') != std::string::npos) {
        return Status(ErrorCodes::FailedToParse, "Hostnames must be comma separated");
    }

    std::vector<LDAPHost> result;
    StringSplitter splitter(hosts.c_str(), ",");
    while (splitter.more()) {
        std::string token = splitter.next();

        if (token.find("ldap://") == 0 || token.find("ldaps://") == 0) {
            return Status(ErrorCodes::FailedToParse,
                          "LDAP server hosts must not contain protocol 'ldap://' or 'ldaps://'");
        }

        auto type = LDAPHost::Type::kDefault;
        auto token_sd = StringData(token);
        if (token_sd.startsWith(srvPrefix)) {
            type = LDAPHost::Type::kSRV;
            token_sd = token_sd.substr(srvPrefix.size());
        } else if (token_sd.startsWith(srvRawPrefix)) {
            type = LDAPHost::Type::kSRVRaw;
            token_sd = token_sd.substr(srvRawPrefix.size());
        }

        result.emplace_back(LDAPHost(type, token_sd, isSSL));
    }

    return result;
}

StatusWith<std::string> LDAPConnectionOptions::constructHostURIs() const {
    StringBuilder hostURIBuilder;
    bool firstLoop = true;
    for (const auto& host : hosts) {
        if (!firstLoop) {
            hostURIBuilder << ",";
        } else {
            firstLoop = false;
        }
        hostURIBuilder << host.serializeURI();
    }

    return hostURIBuilder.str();
}

}  //  namespace mongo
