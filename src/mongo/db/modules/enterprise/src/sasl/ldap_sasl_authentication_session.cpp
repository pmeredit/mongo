/*
 *    Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "ldap_sasl_authentication_session.h"

#include <boost/range/size.hpp>

#include "mongo/base/secure_allocator.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "../ldap/ldap_manager.h"
#include "../ldap/name_mapping/internal_to_ldap_user_name_mapper.h"

namespace mongo {

LDAPSaslAuthenticationSession::LDAPSaslAuthenticationSession(AuthorizationSession* authzSession)
    : SaslAuthenticationSession(authzSession) {}

Status LDAPSaslAuthenticationSession::start(StringData authenticationDatabase,
                                            StringData mechanism,
                                            StringData serviceName,
                                            StringData serviceHostname,
                                            int64_t conversationId,
                                            bool autoAuthorize) {
    fassert(40049, conversationId > 0);

    if (_conversationId != 0) {
        return Status(ErrorCodes::AlreadyInitialized,
                      "Cannot call start() twice on same LDAPSaslAuthenticationSession.");
    }

    _authenticationDatabase = authenticationDatabase.toString();
    _serviceName = serviceName.toString();
    _serviceHostname = serviceHostname.toString();
    _conversationId = conversationId;
    _autoAuthorize = autoAuthorize;

    if (mechanism != mechanismPLAIN) {
        return Status(ErrorCodes::BadValue,
                      mongoutils::str::stream() << "SASL mechanism " << mechanism
                                                << " is not supported for LDAP authentication");
    }

    return Status::OK();
}

Status LDAPSaslAuthenticationSession::step(StringData inputData, std::string* outputData) {
    // Expecting user input on the form: [authz-id]\0authn-id\0pwd
    std::string input = inputData.toString();

    // TODO: make PLAIN use the same code
    SecureString pwd = "";
    try {
        size_t firstNull = inputData.find('\0');
        if (firstNull == std::string::npos) {
            return Status(
                ErrorCodes::AuthenticationFailed,
                str::stream()
                    << "Incorrectly formatted PLAIN client message, missing first NULL delimiter");
        }
        size_t secondNull = inputData.find('\0', firstNull + 1);
        if (secondNull == std::string::npos) {
            return Status(
                ErrorCodes::AuthenticationFailed,
                str::stream()
                    << "Incorrectly formatted PLAIN client message, missing second NULL delimiter");
        }

        std::string authorizationIdentity = input.substr(0, firstNull);
        _user = input.substr(firstNull + 1, (secondNull - firstNull) - 1);
        if (_user.empty()) {
            return Status(ErrorCodes::AuthenticationFailed,
                          str::stream()
                              << "Incorrectly formatted PLAIN client message, empty username");
        } else if (!authorizationIdentity.empty() && authorizationIdentity != _user) {
            return Status(ErrorCodes::AuthenticationFailed,
                          str::stream()
                              << "SASL authorization identity must match authentication identity");
        }

        pwd = SecureString(input.substr(secondNull + 1).c_str());
        if (pwd->empty()) {
            return Status(ErrorCodes::AuthenticationFailed,
                          str::stream()
                              << "Incorrectly formatted PLAIN client message, empty password");
        }
    } catch (std::out_of_range& exception) {
        return Status(ErrorCodes::AuthenticationFailed,
                      mongoutils::str::stream() << "Incorrectly formatted PLAIN client message");
    }

    // A SASL PLAIN conversation only has one step
    _done = true;

    return LDAPManager::get(_opCtx->getServiceContext())->verifyLDAPCredentials(_user, pwd);
}

std::string LDAPSaslAuthenticationSession::getPrincipalId() const {
    return _user;
}

const char* LDAPSaslAuthenticationSession::getMechanism() const {
    return mechanismPLAIN;
}

}  // namespace mongo
