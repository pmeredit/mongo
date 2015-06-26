/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_connection_options.h"

#include "mongo/base/string_data.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

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
