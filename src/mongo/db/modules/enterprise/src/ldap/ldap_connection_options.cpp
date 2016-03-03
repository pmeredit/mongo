/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_connection_options.h"

#include "mongo/base/string_data.h"
#include "mongo/util/mongoutils/str.h"

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

}  //  namespace mongo
