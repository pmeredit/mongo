/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_connection_options.h"

#include <algorithm>

#include "mongo/base/string_data.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/text.h"

namespace mongo {

namespace {
const StringData kSimple("simple");
const StringData kSasl("sasl");
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

const StringData authenticationChoiceToString(LDAPBindType type) {
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

StatusWith<std::vector<std::string>> LDAPConnectionOptions::parseHostURIs(
    const std::string& hosts) {
    if (hosts.find(" ") != std::string::npos) {
        return Status(ErrorCodes::FailedToParse, "Hostnames must be comma separated");
    }

    std::vector<std::string> result;
    StringSplitter splitter(hosts.c_str(), ",");
    while (splitter.more()) {
        std::string token = splitter.next();
        if (token.find("ldap://") == 0 || token.find("ldaps://") == 0) {
            return Status(ErrorCodes::FailedToParse,
                          "LDAP server hosts must not contain protocol 'ldap://' or 'ldaps://'");
        }
        result.emplace_back(std::move(token));
    }

    return result;
}

StatusWith<std::string> LDAPConnectionOptions::constructHostURIs() {
    StringBuilder hostURIBuilder;
    bool firstLoop = true;
    for (const auto& host : hosts) {
        if (!firstLoop) {
            hostURIBuilder << ",";
        } else {
            firstLoop = false;
        }

        if (transportSecurity == LDAPTransportSecurityType::kNone) {
            hostURIBuilder << "ldap://";
        } else if (transportSecurity == LDAPTransportSecurityType::kTLS) {
            hostURIBuilder << "ldaps://";
        } else {
            return Status(ErrorCodes::FailedToParse, "Unrecognized protocol security mechanism");
        }
        hostURIBuilder << host;
    }
    return hostURIBuilder.str();
}

}  //  namespace mongo
