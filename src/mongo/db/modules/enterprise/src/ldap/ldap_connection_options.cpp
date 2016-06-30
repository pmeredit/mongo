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

StatusWith<std::string> LDAPConnectionOptions::parseHostURIs(std::string uris) {
    // Replace ',' with ' ' to support Windows native LDAP and OpenLDAP syntax.
    std::replace(uris.begin(), uris.end(), ',', ' ');

    StringSplitter splitter(uris.c_str(), " ");
    while (splitter.more()) {
        std::string token = splitter.next();
        if (token.find("ldap://") != 0 && token.find("ldaps://") != 0) {
            return Status(ErrorCodes::FailedToParse,
                          "LDAP server URI must begin with either 'ldap://' or 'ldaps://'");
        }
    }

    return uris;
}

}  //  namespace mongo
